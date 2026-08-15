pub mod event_pattern;
pub mod policy;
pub mod poller;
pub mod setup;

/// Shared AWS SDK config resolution (standard credential chain, optional region override)
/// used both by the parameter store provider and by the SQS/EventBridge clients here.
pub async fn load_sdk_config(region: Option<String>) -> aws_config::SdkConfig {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(region) = region {
        loader = loader.region(aws_config::Region::new(region));
    }
    loader.load().await
}
