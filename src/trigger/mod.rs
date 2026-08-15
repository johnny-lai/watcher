use std::collections::BTreeSet;
use std::process::Stdio;
use std::time::Duration;

use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use crate::config::OnChangeConfig;
use crate::engine::Notifier;

enum TriggerMsg {
    Changed(Vec<String>),
    Flush(oneshot::Sender<()>),
}

/// Handle to the debounced command-trigger task. Cheap to clone -- clone one into the
/// `Engine` (as the `Notifier` it calls on every real change) and keep another to call
/// `flush` during shutdown so a last-second change isn't dropped silently.
#[derive(Clone)]
pub struct TriggerHandle {
    sender: Option<mpsc::UnboundedSender<TriggerMsg>>,
}

impl Notifier for TriggerHandle {
    fn notify(&self, names: Vec<String>) {
        if let Some(sender) = &self.sender
            && sender.send(TriggerMsg::Changed(names)).is_err()
        {
            warn!("trigger task is no longer running; dropping change notification");
        }
    }
}

impl TriggerHandle {
    /// Forces any pending/in-flight debounced burst to run now and waits (bounded by
    /// `grace`) for it to finish. A no-op if `on_change.command` is empty.
    pub async fn flush(&self, grace: Duration) {
        let Some(sender) = &self.sender else {
            return;
        };
        let (ack_tx, ack_rx) = oneshot::channel();
        if sender.send(TriggerMsg::Flush(ack_tx)).is_err() {
            return;
        }
        if tokio::time::timeout(grace, ack_rx).await.is_err() {
            warn!("on_change command did not finish within the shutdown grace period");
        }
    }
}

/// Spawns the background debounce task. If `cfg.command` is empty the trigger is
/// disabled: `notify`/`flush` become no-ops and no task is spawned.
pub fn spawn(cfg: OnChangeConfig) -> TriggerHandle {
    if cfg.command.trim().is_empty() {
        return TriggerHandle { sender: None };
    }
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(run_debounce_task(cfg, rx));
    TriggerHandle { sender: Some(tx) }
}

async fn run_debounce_task(cfg: OnChangeConfig, mut rx: mpsc::UnboundedReceiver<TriggerMsg>) {
    let mut pending: BTreeSet<String> = BTreeSet::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            TriggerMsg::Changed(names) => {
                pending.extend(names);
                // Keep absorbing further notifications until the channel is quiet for the
                // debounce window (coalescing a burst into one run), or a Flush request
                // forces an immediate run.
                loop {
                    match tokio::time::timeout(cfg.debounce, rx.recv()).await {
                        Ok(Some(TriggerMsg::Changed(names))) => pending.extend(names),
                        Ok(Some(TriggerMsg::Flush(ack))) => {
                            run_command(&cfg, &pending).await;
                            pending.clear();
                            let _ = ack.send(());
                            break;
                        }
                        Ok(None) => return,
                        Err(_elapsed) => {
                            run_command(&cfg, &pending).await;
                            pending.clear();
                            break;
                        }
                    }
                }
            }
            TriggerMsg::Flush(ack) => {
                let _ = ack.send(());
            }
        }
    }
}

async fn run_command(cfg: &OnChangeConfig, changed: &BTreeSet<String>) {
    if changed.is_empty() {
        return;
    }
    let names: Vec<String> = changed.iter().cloned().collect();
    info!(count = names.len(), "running on_change command");

    let mut command = Command::new("sh");
    command.arg("-c").arg(&cfg.command);
    if !cfg.workdir.trim().is_empty() {
        command.current_dir(&cfg.workdir);
    }
    command.env("WATCHER_CHANGED_PARAMS", names.join(","));
    command.env("WATCHER_CHANGED_COUNT", names.len().to_string());
    for (k, v) in &cfg.env {
        command.env(k, v);
    }
    command.stdin(Stdio::null());
    command.kill_on_drop(true);

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            error!(error = %err, "failed to spawn on_change command");
            return;
        }
    };

    match tokio::time::timeout(cfg.timeout, child.wait()).await {
        Ok(Ok(status)) if status.success() => info!("on_change command completed successfully"),
        Ok(Ok(status)) => error!(status = %status, "on_change command exited non-zero"),
        Ok(Err(err)) => error!(error = %err, "on_change command failed"),
        Err(_elapsed) => {
            error!(timeout = ?cfg.timeout, "on_change command timed out; killing process")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_cfg(command: String) -> OnChangeConfig {
        OnChangeConfig {
            command,
            workdir: String::new(),
            env: Default::default(),
            timeout: Duration::from_secs(5),
            debounce: Duration::from_millis(30),
        }
    }

    #[tokio::test]
    async fn coalesces_a_burst_into_one_run() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("out.txt");
        let cfg = base_cfg(format!(
            "printf '%s:%s' \"$WATCHER_CHANGED_PARAMS\" \"$WATCHER_CHANGED_COUNT\" >> {}",
            out_path.display()
        ));
        let handle = spawn(cfg);

        handle.notify(vec!["/a".to_string()]);
        handle.notify(vec!["/b".to_string()]);
        handle.notify(vec!["/c".to_string()]);

        handle.flush(Duration::from_secs(5)).await;

        let contents = std::fs::read_to_string(&out_path).unwrap();
        // BTreeSet ordering makes this deterministic; single run means the file was
        // written exactly once.
        assert_eq!(contents, "/a,/b,/c:3");
    }

    #[tokio::test]
    async fn empty_command_disables_trigger() {
        let handle = spawn(base_cfg(String::new()));
        handle.notify(vec!["/a".to_string()]);
        // must return promptly since there's no task to wait on
        handle.flush(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn flush_waits_for_in_flight_command() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("out.txt");
        let cfg = base_cfg(format!("sleep 0.1 && touch {}", out_path.display()));
        let handle = spawn(cfg);

        handle.notify(vec!["/a".to_string()]);
        handle.flush(Duration::from_secs(5)).await;

        assert!(out_path.exists());
    }
}
