use std::time::Duration;

use thiserror::Error;

use super::Config;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("unsupported provider.type {0:?}; supported types: [\"parameter_store\"]")]
    UnsupportedProviderType(String),
    #[error(
        "provider.parameter_store section is required when provider.type = \"parameter_store\""
    )]
    MissingParameterStoreSection,
    #[error("provider.parameter_store.prefix is required and must start with \"/\"")]
    InvalidPrefix,
    #[error("destination.root is required and must be an absolute path")]
    InvalidDestinationRoot,
    #[error("full_sync.interval must be at least 30s")]
    IntervalTooShort,
    #[error("event_sync.queue_url is required when event_sync.enabled = true")]
    MissingQueueUrl,
    #[error("setup.queue_name is required")]
    MissingSetupQueueName,
    #[error("setup.rule_name is required")]
    MissingSetupRuleName,
    #[error("provider.parameter_store.region is required for `watcher setup`")]
    MissingRegionForSetup,
}

/// Validates a config for running `watcher sync`.
pub fn validate(cfg: &Config) -> Result<(), ConfigError> {
    if cfg.provider.type_ != "parameter_store" {
        return Err(ConfigError::UnsupportedProviderType(
            cfg.provider.type_.clone(),
        ));
    }
    let ps = cfg
        .provider
        .parameter_store
        .as_ref()
        .ok_or(ConfigError::MissingParameterStoreSection)?;
    if !ps.prefix.starts_with('/') {
        return Err(ConfigError::InvalidPrefix);
    }
    if !cfg.destination.root.is_absolute() {
        return Err(ConfigError::InvalidDestinationRoot);
    }
    if cfg.full_sync.interval < Duration::from_secs(30) {
        return Err(ConfigError::IntervalTooShort);
    }
    if cfg.event_sync.enabled && cfg.event_sync.queue_url.trim().is_empty() {
        return Err(ConfigError::MissingQueueUrl);
    }
    Ok(())
}

/// Validates a config for running `watcher setup`. This is deliberately independent of
/// `validate` above: at setup time `event_sync.queue_url` legitimately doesn't exist yet
/// (setup is what creates it), so the full daemon validation doesn't apply.
pub fn validate_for_setup(cfg: &Config) -> Result<(), ConfigError> {
    if cfg.provider.type_ != "parameter_store" {
        return Err(ConfigError::UnsupportedProviderType(
            cfg.provider.type_.clone(),
        ));
    }
    let ps = cfg
        .provider
        .parameter_store
        .as_ref()
        .ok_or(ConfigError::MissingParameterStoreSection)?;
    if !ps.prefix.starts_with('/') {
        return Err(ConfigError::InvalidPrefix);
    }
    if ps.region.as_deref().unwrap_or("").trim().is_empty() {
        return Err(ConfigError::MissingRegionForSetup);
    }
    if cfg.setup.queue_name.trim().is_empty() {
        return Err(ConfigError::MissingSetupQueueName);
    }
    if cfg.setup.rule_name.trim().is_empty() {
        return Err(ConfigError::MissingSetupRuleName);
    }
    Ok(())
}
