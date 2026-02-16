use std::collections::HashMap;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing::{debug, error};

/// Publishes serialized MAVLink messages to Kafka topics.
pub struct KafkaSink {
    producer: FutureProducer,
    topic_prefix: String,
}

impl KafkaSink {
    /// Creates a new Kafka producer connected to the given brokers.
    pub fn new(
        brokers: &str,
        topic_prefix: &str,
        extra_properties: &HashMap<String, String>,
    ) -> anyhow::Result<Self> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", brokers);
        config.set("message.timeout.ms", "5000");

        for (key, value) in extra_properties {
            config.set(key, value);
        }

        let producer: FutureProducer = config.create()?;

        Ok(Self {
            producer,
            topic_prefix: topic_prefix.to_string(),
        })
    }

    /// Publishes a payload to `<prefix>.<message_name>`, keyed by `system_id`.
    pub async fn publish(&self, message_name: &str, system_id: u8, payload: &[u8]) {
        let topic = crate::message::build_topic_name(&self.topic_prefix, message_name);
        let key = system_id.to_string();

        let record = FutureRecord::to(&topic).payload(payload).key(&key);

        match self.producer.send(record, Duration::from_secs(0)).await {
            Ok((partition, offset)) => {
                debug!(
                    topic = %topic,
                    partition,
                    offset,
                    "Message delivered"
                );
            }
            Err((err, _)) => {
                error!(
                    topic = %topic,
                    error = %err,
                    "Failed to deliver message to Kafka"
                );
            }
        }
    }
}
