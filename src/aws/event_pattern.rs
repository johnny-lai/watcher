/// Builds the EventBridge rule pattern matching SSM "Parameter Store Change" events
/// whose parameter name starts with the configured prefix.
pub fn build_event_pattern(prefix: &str) -> anyhow::Result<String> {
    let pattern = serde_json::json!({
        "source": ["aws.ssm"],
        "detail-type": ["Parameter Store Change"],
        "detail": {
            "name": [{"prefix": prefix}]
        }
    });
    Ok(serde_json::to_string(&pattern)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_prefix_filtered_pattern() {
        let pattern_str = build_event_pattern("/myapp/prod").unwrap();
        let pattern: serde_json::Value = serde_json::from_str(&pattern_str).unwrap();

        assert_eq!(pattern["source"][0], "aws.ssm");
        assert_eq!(pattern["detail-type"][0], "Parameter Store Change");
        assert_eq!(pattern["detail"]["name"][0]["prefix"], "/myapp/prod");
    }
}
