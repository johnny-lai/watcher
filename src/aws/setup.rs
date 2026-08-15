use std::collections::HashMap;

use aws_sdk_eventbridge::Client as EventBridgeClient;
use aws_sdk_eventbridge::types::{RuleState, Target};
use aws_sdk_sqs::Client as SqsClient;
use aws_sdk_sqs::types::QueueAttributeName;

use super::event_pattern::build_event_pattern;
use super::policy::build_queue_policy;

pub struct DlqConfig {
    pub enabled: bool,
    pub queue_name: String,
    pub max_receive_count: i32,
}

pub struct SetupConfig {
    pub queue_name: String,
    pub event_bus_name: String,
    pub rule_name: String,
    pub prefix: String,
    pub visibility_timeout: i32,
    pub message_retention_seconds: i32,
    pub dlq: DlqConfig,
}

pub struct SetupResult {
    pub queue_url: String,
    pub queue_arn: String,
    pub rule_arn: String,
    pub dlq_url: Option<String>,
}

/// Idempotently provisions the SQS queue (+ optional DLQ), the EventBridge rule filtered
/// to the configured prefix, a least-privilege queue policy scoped to that rule, and the
/// rule's target. Safe to re-run: every step upserts the desired state rather than
/// diffing first.
pub async fn run_setup(
    sqs: &SqsClient,
    eventbridge: &EventBridgeClient,
    cfg: &SetupConfig,
) -> anyhow::Result<SetupResult> {
    let dlq = if cfg.dlq.enabled {
        Some(
            ensure_queue(
                sqs,
                &cfg.dlq.queue_name,
                cfg.visibility_timeout,
                cfg.message_retention_seconds,
                None,
            )
            .await?,
        )
    } else {
        None
    };

    let redrive_policy = dlq.as_ref().map(|(arn, _url)| {
        serde_json::json!({
            "deadLetterTargetArn": arn,
            "maxReceiveCount": cfg.dlq.max_receive_count,
        })
        .to_string()
    });

    let (queue_arn, queue_url) = ensure_queue(
        sqs,
        &cfg.queue_name,
        cfg.visibility_timeout,
        cfg.message_retention_seconds,
        redrive_policy.as_deref(),
    )
    .await?;

    let rule_arn = ensure_rule(
        eventbridge,
        &cfg.rule_name,
        &cfg.event_bus_name,
        &cfg.prefix,
    )
    .await?;

    ensure_queue_policy(sqs, &queue_url, &queue_arn, &rule_arn).await?;
    ensure_target(
        eventbridge,
        &cfg.rule_name,
        &cfg.event_bus_name,
        &cfg.queue_name,
        &queue_arn,
    )
    .await?;

    Ok(SetupResult {
        queue_url,
        queue_arn,
        rule_arn,
        dlq_url: dlq.map(|(_arn, url)| url),
    })
}

/// Returns (queue_arn, queue_url).
async fn ensure_queue(
    client: &SqsClient,
    name: &str,
    visibility_timeout: i32,
    message_retention_seconds: i32,
    redrive_policy: Option<&str>,
) -> anyhow::Result<(String, String)> {
    let mut attrs: HashMap<QueueAttributeName, String> = HashMap::new();
    attrs.insert(
        QueueAttributeName::VisibilityTimeout,
        visibility_timeout.to_string(),
    );
    attrs.insert(
        QueueAttributeName::MessageRetentionPeriod,
        message_retention_seconds.to_string(),
    );
    if let Some(policy) = redrive_policy {
        attrs.insert(QueueAttributeName::RedrivePolicy, policy.to_string());
    }

    let queue_url = match client.get_queue_url().queue_name(name).send().await {
        Ok(resp) => {
            let url = resp.queue_url().unwrap_or_default().to_string();
            client
                .set_queue_attributes()
                .queue_url(&url)
                .set_attributes(Some(attrs.clone()))
                .send()
                .await?;
            url
        }
        Err(err) => {
            let not_found = err
                .as_service_error()
                .map(|e| e.is_queue_does_not_exist())
                .unwrap_or(false);
            if !not_found {
                return Err(err.into());
            }
            let resp = client
                .create_queue()
                .queue_name(name)
                .set_attributes(Some(attrs))
                .send()
                .await?;
            resp.queue_url().unwrap_or_default().to_string()
        }
    };

    let attrs_resp = client
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await?;
    let arn = attrs_resp
        .attributes()
        .and_then(|a| a.get(&QueueAttributeName::QueueArn))
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("queue {name} has no ARN attribute"))?;

    Ok((arn, queue_url))
}

async fn ensure_rule(
    client: &EventBridgeClient,
    rule_name: &str,
    event_bus_name: &str,
    prefix: &str,
) -> anyhow::Result<String> {
    let pattern = build_event_pattern(prefix)?;
    let resp = client
        .put_rule()
        .name(rule_name)
        .event_bus_name(event_bus_name)
        .event_pattern(pattern)
        .state(RuleState::Enabled)
        .send()
        .await?;
    resp.rule_arn()
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("PutRule did not return a rule ARN"))
}

async fn ensure_queue_policy(
    client: &SqsClient,
    queue_url: &str,
    queue_arn: &str,
    rule_arn: &str,
) -> anyhow::Result<()> {
    let policy = build_queue_policy(queue_arn, rule_arn)?;
    let mut attrs = HashMap::new();
    attrs.insert(QueueAttributeName::Policy, policy);
    client
        .set_queue_attributes()
        .queue_url(queue_url)
        .set_attributes(Some(attrs))
        .send()
        .await?;
    Ok(())
}

async fn ensure_target(
    client: &EventBridgeClient,
    rule_name: &str,
    event_bus_name: &str,
    target_id: &str,
    queue_arn: &str,
) -> anyhow::Result<()> {
    let target = Target::builder().id(target_id).arn(queue_arn).build()?;
    client
        .put_targets()
        .rule(rule_name)
        .event_bus_name(event_bus_name)
        .targets(target)
        .send()
        .await?;
    Ok(())
}
