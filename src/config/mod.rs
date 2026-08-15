use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use serde::Deserialize;

mod defaults;
mod validate;

// ConfigError is part of the public validation API (and exercised by tests below);
// callers outside this crate would match on it, but nothing in this binary does yet.
#[allow(unused_imports)]
pub use validate::{ConfigError, validate, validate_for_setup};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub provider: ProviderConfig,
    pub destination: DestinationConfig,
    pub state: StateConfig,
    pub full_sync: FullSyncConfig,
    #[serde(default)]
    pub event_sync: EventSyncConfig,
    #[serde(default)]
    pub on_change: OnChangeConfig,
    #[serde(default)]
    pub setup: SetupConfig,
    #[serde(default)]
    pub log: LogConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProviderConfig {
    #[serde(rename = "type")]
    pub type_: String,
    pub parameter_store: Option<ParameterStoreConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParameterStoreConfig {
    pub region: Option<String>,
    pub prefix: String,
    #[serde(default = "defaults::decrypt")]
    pub decrypt: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DestinationConfig {
    pub root: PathBuf,
    #[serde(default = "defaults::dir_perm")]
    pub dir_perm: String,
    #[serde(default = "defaults::file_perm")]
    pub file_perm: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StateConfig {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FullSyncConfig {
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct EventSyncConfig {
    pub enabled: bool,
    pub queue_url: String,
    pub wait_time_seconds: i32,
    pub max_messages: i32,
    pub visibility_timeout: i32,
}

impl Default for EventSyncConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            queue_url: String::new(),
            wait_time_seconds: defaults::wait_time_seconds(),
            max_messages: defaults::max_messages(),
            visibility_timeout: defaults::visibility_timeout(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OnChangeConfig {
    pub command: String,
    pub workdir: String,
    pub env: BTreeMap<String, String>,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub debounce: Duration,
}

impl Default for OnChangeConfig {
    fn default() -> Self {
        Self {
            command: String::new(),
            workdir: String::new(),
            env: BTreeMap::new(),
            timeout: defaults::on_change_timeout(),
            debounce: defaults::on_change_debounce(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SetupConfig {
    pub queue_name: String,
    pub event_bus_name: String,
    pub rule_name: String,
    pub visibility_timeout: i32,
    pub message_retention_seconds: i32,
    pub dlq: DlqConfig,
}

impl Default for SetupConfig {
    fn default() -> Self {
        Self {
            queue_name: String::new(),
            event_bus_name: defaults::setup_event_bus_name(),
            rule_name: String::new(),
            visibility_timeout: defaults::setup_visibility_timeout(),
            message_retention_seconds: defaults::setup_message_retention_seconds(),
            dlq: DlqConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DlqConfig {
    pub enabled: bool,
    pub queue_name: String,
    pub max_receive_count: i32,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            queue_name: String::new(),
            max_receive_count: defaults::dlq_max_receive_count(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    pub level: String,
    pub format: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: defaults::log_level(),
            format: defaults::log_format(),
        }
    }
}

pub fn load(path: &Path) -> anyhow::Result<Config> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading config file {}", path.display()))?;
    let cfg: Config =
        toml::from_str(&text).with_context(|| format!("parsing config file {}", path.display()))?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_config(dir: &tempfile::TempDir, contents: &str) -> PathBuf {
        let path = dir.path().join("config.toml");
        std::fs::write(&path, contents).unwrap();
        path
    }

    const MINIMAL: &str = r#"
[provider]
type = "parameter_store"

[provider.parameter_store]
prefix = "/myapp/prod"

[destination]
root = "/var/lib/watcher/params"

[state]
path = "/var/lib/watcher/state.json"

[full_sync]
interval = "5m"
"#;

    #[test]
    fn example_config_parses_and_setup_validates() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.toml");
        let cfg = load(&path).unwrap();
        // queue_url is intentionally blank in the template (it's filled in with the
        // output of `watcher setup`), so full `validate` is expected to reject it --
        // but everything `watcher setup` itself needs should already be valid.
        assert!(matches!(validate(&cfg), Err(ConfigError::MissingQueueUrl)));
        validate_for_setup(&cfg).unwrap();
    }

    #[test]
    fn loads_minimal_config_with_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(&dir, MINIMAL);
        let cfg = load(&path).unwrap();

        assert_eq!(cfg.provider.type_, "parameter_store");
        assert!(cfg.provider.parameter_store.as_ref().unwrap().decrypt);
        assert_eq!(cfg.destination.dir_perm, "0700");
        assert_eq!(cfg.destination.file_perm, "0600");
        assert!(!cfg.event_sync.enabled);
        assert_eq!(cfg.event_sync.wait_time_seconds, 20);
        assert_eq!(cfg.on_change.timeout, Duration::from_secs(30));
        assert_eq!(cfg.on_change.debounce, Duration::from_secs(2));
        assert_eq!(cfg.log.level, "info");
        assert_eq!(cfg.setup.event_bus_name, "default");

        validate(&cfg).unwrap();
    }

    #[test]
    fn rejects_unsupported_provider_type() {
        let cfg = MINIMAL.replace("parameter_store\"\n", "vault\"\n");
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(&dir, &cfg);
        let cfg = load(&path).unwrap();
        assert!(matches!(
            validate(&cfg),
            Err(ConfigError::UnsupportedProviderType(_))
        ));
    }

    #[test]
    fn rejects_relative_prefix() {
        let cfg = MINIMAL.replace("/myapp/prod", "myapp/prod");
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(&dir, &cfg);
        let cfg = load(&path).unwrap();
        assert!(matches!(validate(&cfg), Err(ConfigError::InvalidPrefix)));
    }

    #[test]
    fn rejects_relative_destination_root() {
        let cfg = MINIMAL.replace(
            "root = \"/var/lib/watcher/params\"",
            "root = \"relative/params\"",
        );
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(&dir, &cfg);
        let cfg = load(&path).unwrap();
        assert!(matches!(
            validate(&cfg),
            Err(ConfigError::InvalidDestinationRoot)
        ));
    }

    #[test]
    fn rejects_short_full_sync_interval() {
        let cfg = MINIMAL.replace("interval = \"5m\"", "interval = \"10s\"");
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(&dir, &cfg);
        let cfg = load(&path).unwrap();
        assert!(matches!(validate(&cfg), Err(ConfigError::IntervalTooShort)));
    }

    #[test]
    fn rejects_event_sync_enabled_without_queue_url() {
        let cfg = format!("{MINIMAL}\n[event_sync]\nenabled = true\n");
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(&dir, &cfg);
        let cfg = load(&path).unwrap();
        assert!(matches!(validate(&cfg), Err(ConfigError::MissingQueueUrl)));
    }

    #[test]
    fn validate_for_setup_requires_region_and_names() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(&dir, MINIMAL);
        let cfg = load(&path).unwrap();
        assert!(matches!(
            validate_for_setup(&cfg),
            Err(ConfigError::MissingRegionForSetup)
        ));

        let with_region = MINIMAL.replace(
            "prefix = \"/myapp/prod\"",
            "prefix = \"/myapp/prod\"\nregion = \"us-east-1\"",
        );
        let path = write_config(&dir, &with_region);
        let cfg = load(&path).unwrap();
        assert!(matches!(
            validate_for_setup(&cfg),
            Err(ConfigError::MissingSetupQueueName)
        ));

        let with_setup = format!("{with_region}\n[setup]\nqueue_name = \"q\"\nrule_name = \"r\"\n");
        let path = write_config(&dir, &with_setup);
        let cfg = load(&path).unwrap();
        validate_for_setup(&cfg).unwrap();
    }

    #[test]
    fn validate_for_setup_does_not_require_queue_url() {
        // event_sync isn't even present here; setup validation must not care.
        let with_region = MINIMAL.replace(
            "prefix = \"/myapp/prod\"",
            "prefix = \"/myapp/prod\"\nregion = \"us-east-1\"",
        );
        let with_setup = format!("{with_region}\n[setup]\nqueue_name = \"q\"\nrule_name = \"r\"\n");
        let dir = tempfile::tempdir().unwrap();
        let path = write_config(&dir, &with_setup);
        let cfg = load(&path).unwrap();
        validate_for_setup(&cfg).unwrap();
    }
}
