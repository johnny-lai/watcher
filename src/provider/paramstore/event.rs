use serde::Deserialize;

use crate::provider::{ChangeEvent, Operation};

#[derive(Debug, Deserialize)]
struct EventBridgeEnvelope {
    source: Option<String>,
    #[serde(rename = "detail-type")]
    detail_type: Option<String>,
    detail: Detail,
}

#[derive(Debug, Deserialize)]
struct Detail {
    name: String,
    operation: String,
}

/// Parses an SQS message body as an EventBridge "Parameter Store Change" event.
///
/// Re-checks `source`, `detail-type`, and the configured prefix itself rather than
/// trusting the EventBridge rule's filter alone (defense-in-depth): a mismatch returns
/// an empty (not erroring) result, since the message is safe to acknowledge/delete, just
/// not relevant to us.
pub fn parse_event(message_body: &[u8], prefix: &str) -> anyhow::Result<Vec<ChangeEvent>> {
    let envelope: EventBridgeEnvelope = serde_json::from_slice(message_body)?;

    if envelope.source.as_deref() != Some("aws.ssm") {
        return Ok(Vec::new());
    }
    if envelope.detail_type.as_deref() != Some("Parameter Store Change") {
        return Ok(Vec::new());
    }
    if !envelope.detail.name.starts_with(prefix) {
        return Ok(Vec::new());
    }

    let operation = match envelope.detail.operation.as_str() {
        "Create" => Operation::Created,
        "Update" | "LabelParameterVersion" => Operation::Updated,
        "Delete" => Operation::Deleted,
        other => anyhow::bail!("unrecognized SSM parameter operation {other:?}"),
    };

    Ok(vec![ChangeEvent {
        name: envelope.detail.name,
        operation,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> Vec<u8> {
        let path = format!(
            "{}/src/provider/paramstore/testdata/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read(path).unwrap()
    }

    #[test]
    fn parses_create_event() {
        let events = parse_event(&fixture("event_create.json"), "/myapp/prod").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "/myapp/prod/db/password");
        assert_eq!(events[0].operation, Operation::Created);
    }

    #[test]
    fn parses_update_event() {
        let events = parse_event(&fixture("event_update.json"), "/myapp/prod").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].operation, Operation::Updated);
    }

    #[test]
    fn parses_delete_event() {
        let events = parse_event(&fixture("event_delete.json"), "/myapp/prod").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].operation, Operation::Deleted);
    }

    #[test]
    fn ignores_events_outside_prefix() {
        let events = parse_event(&fixture("event_wrong_prefix.json"), "/myapp/prod").unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn errors_on_malformed_payload() {
        assert!(parse_event(&fixture("event_malformed.json"), "/myapp/prod").is_err());
    }
}
