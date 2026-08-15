use async_trait::async_trait;
use aws_sdk_sqs::Client;

#[derive(Debug, Clone)]
pub struct Message {
    pub body: String,
    pub receipt_handle: String,
}

/// Long-poll receive/delete against a single SQS queue. Kept as a small trait (rather
/// than a bare struct) purely so `EventLoop` can be tested against a fake without hitting
/// real SQS -- the transport itself is not meant to be pluggable the way `Provider` is.
#[async_trait]
pub trait Poller: Send + Sync {
    async fn poll(&self) -> anyhow::Result<Vec<Message>>;
    async fn delete(&self, receipt_handle: &str) -> anyhow::Result<()>;
}

pub struct SqsPoller {
    client: Client,
    queue_url: String,
    wait_time_seconds: i32,
    max_messages: i32,
    visibility_timeout: i32,
}

impl SqsPoller {
    pub fn new(
        client: Client,
        queue_url: String,
        wait_time_seconds: i32,
        max_messages: i32,
        visibility_timeout: i32,
    ) -> Self {
        Self {
            client,
            queue_url,
            wait_time_seconds,
            max_messages,
            visibility_timeout,
        }
    }
}

#[async_trait]
impl Poller for SqsPoller {
    async fn poll(&self) -> anyhow::Result<Vec<Message>> {
        let resp = self
            .client
            .receive_message()
            .queue_url(&self.queue_url)
            .wait_time_seconds(self.wait_time_seconds)
            .max_number_of_messages(self.max_messages)
            .visibility_timeout(self.visibility_timeout)
            .send()
            .await?;

        let messages = resp
            .messages()
            .iter()
            .filter_map(|m| match (m.body(), m.receipt_handle()) {
                (Some(body), Some(receipt)) => Some(Message {
                    body: body.to_string(),
                    receipt_handle: receipt.to_string(),
                }),
                _ => None,
            })
            .collect();
        Ok(messages)
    }

    async fn delete(&self, receipt_handle: &str) -> anyhow::Result<()> {
        self.client
            .delete_message()
            .queue_url(&self.queue_url)
            .receipt_handle(receipt_handle)
            .send()
            .await?;
        Ok(())
    }
}
