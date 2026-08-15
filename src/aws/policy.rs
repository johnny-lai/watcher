/// Builds a least-privilege SQS queue policy that allows EventBridge to deliver messages
/// only from the specific rule ARN (not open to all of events.amazonaws.com account-wide).
pub fn build_queue_policy(queue_arn: &str, rule_arn: &str) -> anyhow::Result<String> {
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "events.amazonaws.com"},
            "Action": "sqs:SendMessage",
            "Resource": queue_arn,
            "Condition": {"ArnEquals": {"aws:SourceArn": rule_arn}}
        }]
    });
    Ok(serde_json::to_string(&policy)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_policy_to_queue_and_rule() {
        let policy_str = build_queue_policy(
            "arn:aws:sqs:us-east-1:123456789012:watcher-ssm-events",
            "arn:aws:events:us-east-1:123456789012:rule/watcher-ssm-parameter-change",
        )
        .unwrap();
        let policy: serde_json::Value = serde_json::from_str(&policy_str).unwrap();

        assert_eq!(policy["Version"], "2012-10-17");
        let statement = &policy["Statement"][0];
        assert_eq!(statement["Effect"], "Allow");
        assert_eq!(statement["Principal"]["Service"], "events.amazonaws.com");
        assert_eq!(statement["Action"], "sqs:SendMessage");
        assert_eq!(
            statement["Resource"],
            "arn:aws:sqs:us-east-1:123456789012:watcher-ssm-events"
        );
        assert_eq!(
            statement["Condition"]["ArnEquals"]["aws:SourceArn"],
            "arn:aws:events:us-east-1:123456789012:rule/watcher-ssm-parameter-change"
        );
    }
}
