use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::aws;
use crate::aws::poller::SqsPoller;
use crate::config;
use crate::engine::event_loop::EventLoop;
use crate::engine::filewriter::FileWriter;
use crate::engine::fullsync_loop::FullSyncLoop;
use crate::engine::state::StateStore;
use crate::engine::{Engine, Notifier};
use crate::provider;
use crate::trigger;

pub async fn run(
    config_path: PathBuf,
    log_level: Option<String>,
    log_format: Option<String>,
) -> anyhow::Result<()> {
    let cfg = config::load(&config_path)?;
    config::validate(&cfg)?;

    crate::logging::init(
        log_level.as_deref().unwrap_or(&cfg.log.level),
        log_format.as_deref().unwrap_or(&cfg.log.format),
    );

    warn_if_root_permissive(&cfg.destination.root);

    let prov = provider::new(&cfg.provider).await?;

    let dir_mode = u32::from_str_radix(&cfg.destination.dir_perm, 8).map_err(|_| {
        anyhow::anyhow!(
            "invalid destination.dir_perm {:?}",
            cfg.destination.dir_perm
        )
    })?;
    let file_mode = u32::from_str_radix(&cfg.destination.file_perm, 8).map_err(|_| {
        anyhow::anyhow!(
            "invalid destination.file_perm {:?}",
            cfg.destination.file_perm
        )
    })?;

    let state = StateStore::load(&cfg.state.path)?;
    let writer = FileWriter::new(cfg.destination.root.clone(), dir_mode, file_mode);
    let trigger_handle = trigger::spawn(cfg.on_change.clone());
    let notifier: Arc<dyn Notifier> = Arc::new(trigger_handle.clone());

    let prefix = cfg
        .provider
        .parameter_store
        .as_ref()
        .expect("validated: provider.parameter_store section present")
        .prefix
        .clone();

    let engine = Arc::new(Engine::new(prov.clone(), state, writer, notifier, prefix));

    info!("running initial full sync");
    let changed = engine.run_full_sync().await?;
    info!(count = changed.len(), "initial full sync complete");

    let cancel = CancellationToken::new();
    let shutdown_cancel = cancel.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        info!("shutdown signal received");
        shutdown_cancel.cancel();
    });

    let full_sync_loop = FullSyncLoop::new(engine.clone(), cfg.full_sync.interval);
    let full_sync_cancel = cancel.clone();
    let full_sync_task = tokio::spawn(async move { full_sync_loop.run(full_sync_cancel).await });

    let event_task = if cfg.event_sync.enabled {
        if prov.as_event_parser().is_none() {
            anyhow::bail!(
                "provider does not support event-driven sync but event_sync.enabled = true"
            );
        }

        let region = cfg
            .provider
            .parameter_store
            .as_ref()
            .and_then(|p| p.region.clone());
        let sdk_config = aws::load_sdk_config(region).await;
        let sqs_client = aws_sdk_sqs::Client::new(&sdk_config);
        let poller = SqsPoller::new(
            sqs_client,
            cfg.event_sync.queue_url.clone(),
            cfg.event_sync.wait_time_seconds,
            cfg.event_sync.max_messages,
            cfg.event_sync.visibility_timeout,
        );

        let event_loop = EventLoop::new(poller, prov.clone(), engine.clone());
        let event_cancel = cancel.clone();
        Some(tokio::spawn(
            async move { event_loop.run(event_cancel).await },
        ))
    } else {
        None
    };

    full_sync_task.await?;
    if let Some(task) = event_task {
        task.await?;
    }

    trigger_handle.flush(Duration::from_secs(10)).await;

    Ok(())
}

async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = sigterm.recv() => {},
    }
}

fn warn_if_root_permissive(root: &Path) {
    if let Ok(meta) = std::fs::metadata(root) {
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            warn!(
                root = %root.display(),
                mode = format!("{mode:o}"),
                "destination.root has group/other permission bits set; consider chmod 0700"
            );
        }
    }
}
