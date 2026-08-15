use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use super::Engine;

pub struct FullSyncLoop {
    engine: Arc<Engine>,
    interval: Duration,
}

impl FullSyncLoop {
    pub fn new(engine: Arc<Engine>, interval: Duration) -> Self {
        Self { engine, interval }
    }

    /// Ticks every `interval` and runs a full sync. Assumes the caller already performed
    /// the initial sync at startup, so the first (immediate) tick from `interval()` is
    /// consumed without acting on it.
    pub async fn run(&self, cancel: CancellationToken) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("full sync loop shutting down");
                    return;
                }
                _ = ticker.tick() => {
                    match self.engine.run_full_sync().await {
                        Ok(changed) if !changed.is_empty() => {
                            info!(count = changed.len(), "full sync applied changes");
                        }
                        Ok(_) => {}
                        Err(err) => error!(error = %err, "full sync failed"),
                    }
                }
            }
        }
    }
}
