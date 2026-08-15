use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::config::ProviderConfig;

pub mod paramstore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Clone)]
pub struct Parameter {
    pub name: String,
    pub value: String,
    pub version: i64,
    /// SSM's parameter type ("String" | "StringList" | "SecureString"), surfaced for
    /// operational logging (e.g. distinguishing secret vs. plain values in sync logs).
    pub type_: String,
}

#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub name: String,
    pub operation: Operation,
}

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("unsupported provider type {0:?}; supported types: [\"parameter_store\"]")]
    UnsupportedType(String),
    #[error(
        "provider.parameter_store config section is required when provider.type = \"parameter_store\""
    )]
    MissingParameterStoreConfig,
    #[error(transparent)]
    Init(#[from] anyhow::Error),
}

/// A read-only source of configuration/secret values. `parameter_store` is the only
/// implementation today; the trait is kept minimal so other backends can be added later
/// without reshaping the sync engine.
#[async_trait]
pub trait Provider: Send + Sync {
    async fn list_by_prefix(&self, prefix: &str) -> anyhow::Result<Vec<Parameter>>;

    /// Fetches specific names. A name that no longer exists is simply omitted from the
    /// result (not an error) -- callers should treat a missing name as "already deleted".
    async fn get_parameters(&self, names: &[String]) -> anyhow::Result<Vec<Parameter>>;

    /// Optional capability: providers that support push notifications (e.g. via
    /// EventBridge/SQS) implement `EventParser` and return `Some(self)` here. This is
    /// checked once at startup via a type-erased `Option`, playing the role Go's
    /// optional-interface type assertion would.
    fn as_event_parser(&self) -> Option<&dyn EventParser> {
        None
    }
}

/// Turns a raw push-notification message body into the parameter names it says changed.
/// Deliberately not part of `Provider` -- not every backend has (or needs) a push
/// mechanism, and this keeps SQS/EventBridge-shaped assumptions out of the core contract.
pub trait EventParser: Send + Sync {
    fn parse_event(&self, message_body: &[u8]) -> anyhow::Result<Vec<ChangeEvent>>;
}

/// The only provider factory. Errors on any `type` other than `"parameter_store"`.
pub async fn new(cfg: &ProviderConfig) -> Result<Arc<dyn Provider>, ProviderError> {
    match cfg.type_.as_str() {
        "parameter_store" => {
            let ps_cfg = cfg
                .parameter_store
                .as_ref()
                .ok_or(ProviderError::MissingParameterStoreConfig)?;
            let provider = paramstore::ParamStoreProvider::new(ps_cfg).await?;
            Ok(Arc::new(provider))
        }
        other => Err(ProviderError::UnsupportedType(other.to_string())),
    }
}
