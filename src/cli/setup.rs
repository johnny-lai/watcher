use std::path::PathBuf;

use tracing::info;

use crate::aws;
use crate::config;

pub async fn run(config_path: PathBuf) -> anyhow::Result<()> {
    let cfg = config::load(&config_path)?;
    config::validate_for_setup(&cfg)?;

    crate::logging::init(&cfg.log.level, &cfg.log.format);

    let ps = cfg
        .provider
        .parameter_store
        .as_ref()
        .expect("validated: provider.parameter_store section present");

    let sdk_config = aws::load_sdk_config(ps.region.clone()).await;
    let sqs_client = aws_sdk_sqs::Client::new(&sdk_config);
    let eventbridge_client = aws_sdk_eventbridge::Client::new(&sdk_config);

    let setup_cfg = aws::setup::SetupConfig {
        queue_name: cfg.setup.queue_name.clone(),
        event_bus_name: cfg.setup.event_bus_name.clone(),
        rule_name: cfg.setup.rule_name.clone(),
        prefix: ps.prefix.clone(),
        visibility_timeout: cfg.setup.visibility_timeout,
        message_retention_seconds: cfg.setup.message_retention_seconds,
        dlq: aws::setup::DlqConfig {
            enabled: cfg.setup.dlq.enabled,
            queue_name: cfg.setup.dlq.queue_name.clone(),
            max_receive_count: cfg.setup.dlq.max_receive_count,
        },
    };

    let result = aws::setup::run_setup(&sqs_client, &eventbridge_client, &setup_cfg).await?;

    println!("Queue URL: {}", result.queue_url);
    println!("Queue ARN: {}", result.queue_arn);
    println!("Rule ARN:  {}", result.rule_arn);
    if let Some(dlq_url) = &result.dlq_url {
        println!("DLQ URL:   {dlq_url}");
    }
    println!();
    println!(
        "Paste the Queue URL above into `event_sync.queue_url` in {}.",
        config_path.display()
    );

    info!("setup complete");
    Ok(())
}
