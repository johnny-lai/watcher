use std::collections::BTreeMap;
use std::sync::Arc;

use sha2::{Digest, Sha256};
use tokio::sync::Mutex as AsyncMutex;
use tracing::debug;

use crate::provider::{ChangeEvent, Operation, Parameter, Provider};

pub mod event_loop;
pub mod filewriter;
pub mod fsutil;
pub mod fullsync_loop;
pub mod state;

use filewriter::FileWriter;
use state::{StateEntry, StateStore};

/// Whatever should be told "these parameter names changed" -- implemented by
/// `trigger::TriggerHandle`. Kept as a small trait so the engine doesn't depend on the
/// concrete trigger/debounce machinery.
pub trait Notifier: Send + Sync {
    fn notify(&self, names: Vec<String>);
}

/// Owns the diff/write/state machinery shared by both sync paths: `run_full_sync` (the
/// periodic reconciliation) and `apply_change_events` (event-driven partial updates) both
/// funnel through the same local diff+write+state step, guarded by `write_lock`, so the
/// two sync loops never race on disk state and both drive the on-change notifier
/// identically. AWS network calls happen *before* the lock is taken; only the fast local
/// part is serialized.
pub struct Engine {
    provider: Arc<dyn Provider>,
    state: StateStore,
    writer: FileWriter,
    trigger: Arc<dyn Notifier>,
    prefix: String,
    write_lock: AsyncMutex<()>,
}

impl Engine {
    pub fn new(
        provider: Arc<dyn Provider>,
        state: StateStore,
        writer: FileWriter,
        trigger: Arc<dyn Notifier>,
        prefix: String,
    ) -> Self {
        Self {
            provider,
            state,
            writer,
            trigger,
            prefix,
            write_lock: AsyncMutex::new(()),
        }
    }

    pub async fn run_full_sync(&self) -> anyhow::Result<Vec<String>> {
        let params = self.provider.list_by_prefix(&self.prefix).await?;

        let _guard = self.write_lock.lock().await;
        let mut updates: BTreeMap<String, Option<StateEntry>> = BTreeMap::new();
        let mut changed = Vec::new();
        let mut seen = std::collections::BTreeSet::new();

        for p in &params {
            seen.insert(p.name.clone());
            if self.apply_one(p, &mut updates)? {
                changed.push(p.name.clone());
            }
        }

        for name in self.state.known_names() {
            if !seen.contains(&name) {
                self.writer.remove(&name)?;
                updates.insert(name.clone(), None);
                changed.push(name);
            }
        }

        if !updates.is_empty() {
            self.state.apply(updates)?;
        }
        drop(_guard);

        if !changed.is_empty() {
            self.trigger.notify(changed.clone());
        }
        Ok(changed)
    }

    pub async fn apply_change_events(&self, events: Vec<ChangeEvent>) -> anyhow::Result<()> {
        let mut fetch_names = Vec::new();
        let mut deletes = Vec::new();
        for ev in &events {
            match ev.operation {
                Operation::Deleted => deletes.push(ev.name.clone()),
                Operation::Created | Operation::Updated => fetch_names.push(ev.name.clone()),
            }
        }

        let params = if fetch_names.is_empty() {
            Vec::new()
        } else {
            self.provider.get_parameters(&fetch_names).await?
        };

        // Names we asked for but didn't get back were deleted between the event firing
        // and this fetch -- treat that race as a delete too, per the Provider contract
        // that a missing name means "already gone", not an error.
        let found: std::collections::BTreeSet<_> = params.iter().map(|p| p.name.clone()).collect();
        for name in &fetch_names {
            if !found.contains(name) {
                deletes.push(name.clone());
            }
        }

        let _guard = self.write_lock.lock().await;
        let mut updates: BTreeMap<String, Option<StateEntry>> = BTreeMap::new();
        let mut changed = Vec::new();

        for p in &params {
            if self.apply_one(p, &mut updates)? {
                changed.push(p.name.clone());
            }
        }
        for name in deletes {
            if self.state.get(&name).is_some() {
                self.writer.remove(&name)?;
                updates.insert(name.clone(), None);
                changed.push(name);
            }
        }

        if !updates.is_empty() {
            self.state.apply(updates)?;
        }
        drop(_guard);

        if !changed.is_empty() {
            self.trigger.notify(changed);
        }
        Ok(())
    }

    /// Writes the file and records a state update iff the value actually changed. Must be
    /// called while holding `write_lock`.
    fn apply_one(
        &self,
        p: &Parameter,
        updates: &mut BTreeMap<String, Option<StateEntry>>,
    ) -> anyhow::Result<bool> {
        let hash = hex::encode(Sha256::digest(p.value.as_bytes()));
        if let Some(existing) = self.state.get(&p.name)
            && existing.hash == hash
        {
            return Ok(false);
        }
        self.writer.write(&p.name, p.value.as_bytes())?;
        debug!(name = %p.name, type_ = %p.type_, version = p.version, "wrote changed parameter");
        updates.insert(
            p.name.clone(),
            Some(StateEntry {
                hash,
                version: p.version,
            }),
        );
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    struct FakeProvider {
        snapshot: Mutex<Vec<Parameter>>,
        gettable: Mutex<Vec<Parameter>>,
    }

    fn param(name: &str, value: &str, version: i64) -> Parameter {
        Parameter {
            name: name.to_string(),
            value: value.to_string(),
            version,
            type_: "String".to_string(),
        }
    }

    #[async_trait]
    impl Provider for FakeProvider {
        async fn list_by_prefix(&self, _prefix: &str) -> anyhow::Result<Vec<Parameter>> {
            Ok(self.snapshot.lock().unwrap().clone())
        }
        async fn get_parameters(&self, names: &[String]) -> anyhow::Result<Vec<Parameter>> {
            let gettable = self.gettable.lock().unwrap();
            Ok(gettable
                .iter()
                .filter(|p| names.contains(&p.name))
                .cloned()
                .collect())
        }
    }

    struct SpyNotifier {
        calls: Mutex<Vec<Vec<String>>>,
    }
    impl Notifier for SpyNotifier {
        fn notify(&self, names: Vec<String>) {
            self.calls.lock().unwrap().push(names);
        }
    }

    fn build_engine(
        snapshot: Vec<Parameter>,
        gettable: Vec<Parameter>,
        dir: &tempfile::TempDir,
    ) -> (Arc<Engine>, Arc<SpyNotifier>) {
        let provider = Arc::new(FakeProvider {
            snapshot: Mutex::new(snapshot),
            gettable: Mutex::new(gettable),
        });
        let notifier = Arc::new(SpyNotifier {
            calls: Mutex::new(Vec::new()),
        });
        let state = StateStore::load(&dir.path().join("state.json")).unwrap();
        let writer = FileWriter::new(dir.path().join("params"), 0o700, 0o600);
        let engine = Arc::new(Engine::new(
            provider.clone(),
            state,
            writer,
            notifier.clone() as Arc<dyn Notifier>,
            "/app".to_string(),
        ));
        (engine, notifier)
    }

    #[tokio::test]
    async fn full_sync_writes_new_and_notifies() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, notifier) = build_engine(vec![param("/app/a", "1", 1)], vec![], &dir);

        let changed = engine.run_full_sync().await.unwrap();
        assert_eq!(changed, vec!["/app/a".to_string()]);
        assert_eq!(notifier.calls.lock().unwrap().len(), 1);
        assert_eq!(
            std::fs::read_to_string(dir.path().join("params/app/a")).unwrap(),
            "1"
        );
    }

    #[tokio::test]
    async fn full_sync_is_a_noop_when_nothing_changed() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, notifier) = build_engine(vec![param("/app/a", "1", 1)], vec![], &dir);
        engine.run_full_sync().await.unwrap();
        notifier.calls.lock().unwrap().clear();

        let changed = engine.run_full_sync().await.unwrap();
        assert!(changed.is_empty());
        assert!(notifier.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn full_sync_prunes_removed_parameters() {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(FakeProvider {
            snapshot: Mutex::new(vec![param("/app/a", "1", 1)]),
            gettable: Mutex::new(vec![]),
        });
        let notifier = Arc::new(SpyNotifier {
            calls: Mutex::new(Vec::new()),
        });
        let state = StateStore::load(&dir.path().join("state.json")).unwrap();
        let writer = FileWriter::new(dir.path().join("params"), 0o700, 0o600);
        let engine = Engine::new(
            provider.clone(),
            state,
            writer,
            notifier.clone() as Arc<dyn Notifier>,
            "/app".to_string(),
        );
        engine.run_full_sync().await.unwrap();
        assert!(dir.path().join("params/app/a").exists());

        provider.snapshot.lock().unwrap().clear();
        let changed = engine.run_full_sync().await.unwrap();
        assert_eq!(changed, vec!["/app/a".to_string()]);
        assert!(!dir.path().join("params/app/a").exists());
    }

    #[tokio::test]
    async fn apply_change_events_handles_update_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, notifier) = build_engine(
            vec![param("/app/a", "1", 1)],
            vec![param("/app/a", "2", 2)],
            &dir,
        );
        engine.run_full_sync().await.unwrap();
        notifier.calls.lock().unwrap().clear();

        engine
            .apply_change_events(vec![ChangeEvent {
                name: "/app/a".to_string(),
                operation: Operation::Updated,
            }])
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("params/app/a")).unwrap(),
            "2"
        );
        assert_eq!(notifier.calls.lock().unwrap().len(), 1);

        engine
            .apply_change_events(vec![ChangeEvent {
                name: "/app/a".to_string(),
                operation: Operation::Deleted,
            }])
            .await
            .unwrap();
        assert!(!dir.path().join("params/app/a").exists());
    }
}
