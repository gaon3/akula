use crate::{models::*, stagedsync::StagedSyncStatus};
use async_trait::async_trait;
use ethereum_jsonrpc::{types, EthApiServer, EthSubscriptionKind, PubsubApiServer, SyncStatus};
use jsonrpsee::{core::error::SubscriptionClosed, types::SubscriptionResult, SubscriptionSink};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::BroadcastStream;

pub struct PubsubServerImpl {
    pub sync_sub_tx: broadcast::Sender<SyncStatus>,
    pub block_sub_tx: broadcast::Sender<Option<types::Block>>,
}
impl PubsubServerImpl {
    pub fn run<API: EthApiServer>(
        &self,
        mut staged_sync_rx: mpsc::Receiver<StagedSyncStatus>,
        eth_api: API,
    ) {
        let sync_sub_tx2 = self.sync_sub_tx.clone();
        let block_sub_tx2 = self.block_sub_tx.clone();
        tokio::spawn(async move {
            while let Some(status) = staged_sync_rx.recv().await {
                let current_block = status.minimum_progress.unwrap_or(BlockNumber(0));
                let highest_block = status.maximum_progress.unwrap_or(BlockNumber(0));
                // If node is synced, we also publish new blocks
                if current_block > 0 && current_block >= highest_block {
                    sync_sub_tx2.send(SyncStatus::NotSyncing).unwrap();
                    let header = eth_api
                        .get_block_by_number(current_block.0.into(), false)
                        .await
                        .unwrap();
                    block_sub_tx2.send(header).unwrap();
                } else {
                    sync_sub_tx2
                        .send(SyncStatus::Syncing {
                            highest_block: highest_block.0.into(),
                            current_block: current_block.0.into(),
                        })
                        .unwrap();
                };
            }
        });
    }
}

#[async_trait]
impl PubsubApiServer for PubsubServerImpl {
    fn sub(&self, mut sink: SubscriptionSink, kind: EthSubscriptionKind) -> SubscriptionResult {
        match kind {
            EthSubscriptionKind::Sync => {
                let rx = BroadcastStream::new(self.sync_sub_tx.clone().subscribe());
                tokio::spawn(async move {
                    match sink.pipe_from_try_stream(rx).await {
                        SubscriptionClosed::Success => {
                            sink.close(SubscriptionClosed::Success);
                        }
                        SubscriptionClosed::RemotePeerAborted => (),
                        SubscriptionClosed::Failed(err) => {
                            sink.close(err);
                        }
                    }
                });
            }
            EthSubscriptionKind::Block => {
                let rx = BroadcastStream::new(self.block_sub_tx.clone().subscribe());
                tokio::spawn(async move {
                    match sink.pipe_from_try_stream(rx).await {
                        SubscriptionClosed::Success => {
                            sink.close(SubscriptionClosed::Success);
                        }
                        SubscriptionClosed::RemotePeerAborted => (),
                        SubscriptionClosed::Failed(err) => {
                            sink.close(err);
                        }
                    }
                });
            }
        }

        Ok(())
    }
}
