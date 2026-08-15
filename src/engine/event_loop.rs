use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use crate::aws::poller::Poller;
use crate::provider::Provider;

use super::Engine;

pub struct EventLoop<P: Poller> {
    poller: P,
    provider: Arc<dyn Provider>,
    engine: Arc<Engine>,
}

impl<P: Poller> EventLoop<P> {
    pub fn new(poller: P, provider: Arc<dyn Provider>, engine: Arc<Engine>) -> Self {
        Self {
            poller,
            provider,
            engine,
        }
    }

    pub async fn run(&self, cancel: CancellationToken) {
        let Some(parser) = self.provider.as_event_parser() else {
            error!("provider does not support event-driven sync; event loop exiting");
            return;
        };

        loop {
            let messages = tokio::select! {
                _ = cancel.cancelled() => return,
                result = self.poller.poll() => match result {
                    Ok(m) => m,
                    Err(err) => {
                        error!(error = %err, "sqs poll failed; backing off");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                },
            };

            for msg in messages {
                match parser.parse_event(msg.body.as_bytes()) {
                    Ok(events) if events.is_empty() => {
                        // Not relevant to us (wrong source/detail-type/prefix) -- safe to
                        // acknowledge so it doesn't sit around for redelivery.
                        if let Err(err) = self.poller.delete(&msg.receipt_handle).await {
                            error!(error = %err, "failed to delete irrelevant sqs message");
                        }
                    }
                    Ok(events) => match self.engine.apply_change_events(events).await {
                        Ok(()) => {
                            if let Err(err) = self.poller.delete(&msg.receipt_handle).await {
                                error!(error = %err, "failed to delete processed sqs message");
                            }
                        }
                        Err(err) => {
                            warn!(error = %err, "failed to apply change event; leaving message for redelivery/DLQ");
                        }
                    },
                    Err(err) => {
                        warn!(error = %err, "failed to parse sqs message; leaving for redelivery/DLQ");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws::poller::Message;
    use crate::engine::Notifier;
    use crate::engine::filewriter::FileWriter;
    use crate::engine::state::StateStore;
    use crate::provider::{ChangeEvent, Operation, Parameter};
    use async_trait::async_trait;
    use std::sync::Mutex;

    struct FakePoller {
        // each poll() call pops one batch off the front; once empty, blocks until cancelled
        batches: Mutex<Vec<Vec<Message>>>,
        deleted: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl Poller for FakePoller {
        async fn poll(&self) -> anyhow::Result<Vec<Message>> {
            let next = {
                let mut batches = self.batches.lock().unwrap();
                if batches.is_empty() {
                    None
                } else {
                    Some(batches.remove(0))
                }
            };
            match next {
                Some(batch) => Ok(batch),
                None => {
                    std::future::pending::<()>().await;
                    unreachable!()
                }
            }
        }
        async fn delete(&self, receipt_handle: &str) -> anyhow::Result<()> {
            self.deleted
                .lock()
                .unwrap()
                .push(receipt_handle.to_string());
            Ok(())
        }
    }

    struct FakeProvider;
    #[async_trait]
    impl Provider for FakeProvider {
        async fn list_by_prefix(&self, _prefix: &str) -> anyhow::Result<Vec<Parameter>> {
            Ok(vec![])
        }
        async fn get_parameters(&self, _names: &[String]) -> anyhow::Result<Vec<Parameter>> {
            Ok(vec![Parameter {
                name: "/app/a".to_string(),
                value: "1".to_string(),
                version: 1,
                type_: "String".to_string(),
            }])
        }
        fn as_event_parser(&self) -> Option<&dyn crate::provider::EventParser> {
            Some(self)
        }
    }
    impl crate::provider::EventParser for FakeProvider {
        fn parse_event(&self, body: &[u8]) -> anyhow::Result<Vec<ChangeEvent>> {
            let text = String::from_utf8_lossy(body);
            if text == "bad" {
                anyhow::bail!("malformed");
            }
            Ok(vec![ChangeEvent {
                name: "/app/a".to_string(),
                operation: Operation::Updated,
            }])
        }
    }

    struct NoopNotifier;
    impl Notifier for NoopNotifier {
        fn notify(&self, _names: Vec<String>) {}
    }

    #[tokio::test]
    async fn deletes_message_only_after_successful_apply() {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(FakeProvider);
        let state = StateStore::load(&dir.path().join("state.json")).unwrap();
        let writer = FileWriter::new(dir.path().join("params"), 0o700, 0o600);
        let engine = Arc::new(Engine::new(
            provider.clone(),
            state,
            writer,
            Arc::new(NoopNotifier),
            "/app".to_string(),
        ));

        let deleted = Arc::new(Mutex::new(Vec::new()));
        let poller = FakePoller {
            batches: Mutex::new(vec![vec![
                Message {
                    body: "ok".to_string(),
                    receipt_handle: "r-ok".to_string(),
                },
                Message {
                    body: "bad".to_string(),
                    receipt_handle: "r-bad".to_string(),
                },
            ]]),
            deleted: deleted.clone(),
        };

        let event_loop = EventLoop::new(poller, provider, engine);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let run = tokio::spawn(async move { event_loop.run(cancel_clone).await });
        // give the loop a moment to process the one available batch, then stop it
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        run.await.unwrap();

        // only the successfully-applied message should have been deleted; the
        // malformed one is left alone for SQS redelivery/DLQ.
        assert_eq!(*deleted.lock().unwrap(), vec!["r-ok".to_string()]);
    }
}
