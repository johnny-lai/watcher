use async_trait::async_trait;
use aws_sdk_ssm::Client;
use aws_sdk_ssm::types::Parameter as SsmParameter;

use crate::config::ParameterStoreConfig;
use crate::provider::{ChangeEvent, EventParser, Parameter, Provider};

pub mod event;

pub struct ParamStoreProvider {
    client: Client,
    decrypt: bool,
    prefix: String,
}

impl ParamStoreProvider {
    pub async fn new(cfg: &ParameterStoreConfig) -> anyhow::Result<Self> {
        let sdk_config = crate::aws::load_sdk_config(cfg.region.clone()).await;
        let client = Client::new(&sdk_config);
        Ok(Self {
            client,
            decrypt: cfg.decrypt,
            prefix: cfg.prefix.clone(),
        })
    }
}

#[async_trait]
impl Provider for ParamStoreProvider {
    async fn list_by_prefix(&self, prefix: &str) -> anyhow::Result<Vec<Parameter>> {
        let mut out = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .get_parameters_by_path()
                .path(prefix)
                .recursive(true)
                .with_decryption(self.decrypt);
            if let Some(token) = &next_token {
                req = req.next_token(token);
            }
            let resp = req.send().await?;
            for p in resp.parameters() {
                out.push(to_parameter(p));
            }
            next_token = resp.next_token().map(|s| s.to_string());
            if next_token.is_none() {
                break;
            }
        }
        Ok(out)
    }

    async fn get_parameters(&self, names: &[String]) -> anyhow::Result<Vec<Parameter>> {
        let mut out = Vec::new();
        for chunk in names.chunks(10) {
            let resp = self
                .client
                .get_parameters()
                .set_names(Some(chunk.to_vec()))
                .with_decryption(self.decrypt)
                .send()
                .await?;
            for p in resp.parameters() {
                out.push(to_parameter(p));
            }
        }
        Ok(out)
    }

    fn as_event_parser(&self) -> Option<&dyn EventParser> {
        Some(self)
    }
}

impl EventParser for ParamStoreProvider {
    fn parse_event(&self, message_body: &[u8]) -> anyhow::Result<Vec<ChangeEvent>> {
        event::parse_event(message_body, &self.prefix)
    }
}

fn to_parameter(p: &SsmParameter) -> Parameter {
    Parameter {
        name: p.name().unwrap_or_default().to_string(),
        value: p.value().unwrap_or_default().to_string(),
        version: p.version(),
        type_: p
            .r#type()
            .map(|t| t.as_str().to_string())
            .unwrap_or_default(),
    }
}
