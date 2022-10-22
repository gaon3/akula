use crate::{models::*, stagedsync::StagedSyncStatus};
use async_trait::async_trait;
use ethereum_jsonrpc::{
    types, EthApiServer, EthSubscriptionKind, EthSubscriptionResult, PubsubApiServer, SyncStatus,
};
use jsonrpsee::{core::error::SubscriptionClosed, types::SubscriptionResult, SubscriptionSink};
use tokio::sync::{broadcast, watch};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

pub struct PubsubServerImpl {
    pub sync_sub_tx: broadcast::Sender<SyncStatus>,
    pub block_sub_tx: broadcast::Sender<types::Block>,
}
impl PubsubServerImpl {
    pub fn run<API: EthApiServer>(
        &self,
        mut staged_sync_rx: watch::Receiver<StagedSyncStatus>,
        eth_api: API,
    ) {
        let sync_sub_tx2 = self.sync_sub_tx.clone();
        let block_sub_tx2 = self.block_sub_tx.clone();
        let block_sub_tx3 = block_sub_tx2.clone();
        tokio::spawn(async move {
            let mut stream = BroadcastStream::new(block_sub_tx3.subscribe());
            while let Some(block) = stream.next().await {
                println!("{:?}", block);
            }
        });
        tokio::spawn(async move {
            while staged_sync_rx.changed().await.is_ok() {
                let status = staged_sync_rx.borrow().clone();
                let current_block = status.minimum_progress.unwrap_or(BlockNumber(0));
                let highest_block = status.maximum_progress.unwrap_or(BlockNumber(0));
                // If node is synced, we also publish new blocks
                if current_block > 0 && current_block >= highest_block {
                    let _ = sync_sub_tx2.send(SyncStatus::NotSyncing);
                    let block = eth_api
                        .get_block_by_number(current_block.0.into(), false)
                        .await
                        .unwrap();
                    if let Some(head) = block {
                        let _ = block_sub_tx2.send(head);
                    }
                } else {
                    let _ = sync_sub_tx2.send(SyncStatus::Syncing {
                        highest_block: highest_block.0.into(),
                        current_block: current_block.0.into(),
                    });
                };
            }
        });
    }
}

#[async_trait]
impl PubsubApiServer for PubsubServerImpl {
    fn sub(&self, mut sink: SubscriptionSink, kind: EthSubscriptionKind) -> SubscriptionResult {
        match kind {
            EthSubscriptionKind::Syncing => {
                let stream = BroadcastStream::new(self.sync_sub_tx.clone().subscribe())
                    .map(|status| status.map(|status| EthSubscriptionResult::Syncing(status)));
                tokio::spawn(async move {
                    match sink.pipe_from_try_stream(stream).await {
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
            EthSubscriptionKind::NewHeads => {
                let stream = BroadcastStream::new(self.block_sub_tx.clone().subscribe())
                    .map(|block| block.map(|block| EthSubscriptionResult::NewHeads(block)));
                tokio::spawn(async move {
                    match sink.pipe_from_try_stream(stream).await {
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
