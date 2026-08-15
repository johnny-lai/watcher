use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use super::fsutil;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateEntry {
    pub hash: String,
    pub version: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct State {
    #[serde(default)]
    parameters: BTreeMap<String, StateEntry>,
}

/// Tracks the last-known hash/version per parameter so the engine can tell a real change
/// from a no-op, and recall which names it previously knew about (for prune detection).
/// Stores only hashes/versions -- never plaintext values -- and persists atomically.
pub struct StateStore {
    path: PathBuf,
    state: Mutex<State>,
}

impl StateStore {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let state = if path.exists() {
            let text = std::fs::read_to_string(path)?;
            serde_json::from_str(&text)?
        } else {
            State::default()
        };
        Ok(Self {
            path: path.to_path_buf(),
            state: Mutex::new(state),
        })
    }

    pub fn get(&self, name: &str) -> Option<StateEntry> {
        self.state.lock().unwrap().parameters.get(name).cloned()
    }

    pub fn known_names(&self) -> BTreeSet<String> {
        self.state
            .lock()
            .unwrap()
            .parameters
            .keys()
            .cloned()
            .collect()
    }

    /// Applies a batch of updates -- `Some(entry)` upserts, `None` removes the key -- and
    /// persists the result atomically (temp file + rename, 0600).
    pub fn apply(&self, updates: BTreeMap<String, Option<StateEntry>>) -> anyhow::Result<()> {
        let text = {
            let mut guard = self.state.lock().unwrap();
            for (name, entry) in updates {
                match entry {
                    Some(e) => {
                        guard.parameters.insert(name, e);
                    }
                    None => {
                        guard.parameters.remove(&name);
                    }
                }
            }
            serde_json::to_string_pretty(&*guard)?
        };
        fsutil::write_atomic(&self.path, text.as_bytes(), 0o600)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        let store = StateStore::load(&path).unwrap();
        assert!(store.get("/a").is_none());

        let mut updates = BTreeMap::new();
        updates.insert(
            "/a".to_string(),
            Some(StateEntry {
                hash: "abc".to_string(),
                version: 1,
            }),
        );
        store.apply(updates).unwrap();

        // reload from disk into a fresh store to prove persistence, not just in-memory state
        let reloaded = StateStore::load(&path).unwrap();
        assert_eq!(
            reloaded.get("/a"),
            Some(StateEntry {
                hash: "abc".to_string(),
                version: 1,
            })
        );
        assert_eq!(reloaded.known_names(), BTreeSet::from(["/a".to_string()]));

        let perms = std::fs::metadata(&path).unwrap().permissions();
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(perms.mode() & 0o777, 0o600);
    }

    #[test]
    fn none_removes_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        let store = StateStore::load(&path).unwrap();

        let mut updates = BTreeMap::new();
        updates.insert(
            "/a".to_string(),
            Some(StateEntry {
                hash: "abc".to_string(),
                version: 1,
            }),
        );
        store.apply(updates).unwrap();

        let mut removal = BTreeMap::new();
        removal.insert("/a".to_string(), None);
        store.apply(removal).unwrap();

        assert!(store.get("/a").is_none());
        assert!(store.known_names().is_empty());
    }
}
