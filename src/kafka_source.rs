use std::collections::HashMap;
use std::sync::Arc;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::mavlink_source::MavConnection;

pub struct KafkaCommandConsumer {
    consumer: StreamConsumer,
}

impl KafkaCommandConsumer {
    pub fn new(
        brokers: &str,
        command_topic: &str,
        group_id: &str,
        extra_properties: &HashMap<String, String>,
    ) -> anyhow::Result<Self> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", brokers);
        config.set("group.id", group_id);
        config.set("auto.offset.reset", "latest");

        for (key, value) in extra_properties {
            config.set(key, value);
        }

        let consumer: StreamConsumer = config.create()?;
        consumer.subscribe(&[command_topic])?;

        info!(
            topic = command_topic,
            group_id = group_id,
            "Subscribed to command topic"
        );

        Ok(Self { consumer })
    }

    pub async fn run(
        &self,
        conn: Arc<Box<MavConnection>>,
        cancel_token: CancellationToken,
    ) -> u64 {
        let mut command_count: u64 = 0;

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    info!(commands_sent = command_count, "Command consumer shutting down");
                    break;
                }
                result = self.consumer.recv() => {
                    match result {
                        Ok(msg) => {
                            let payload = match msg.payload() {
                                Some(bytes) => bytes,
                                None => {
                                    warn!("Received Kafka message with empty payload, skipping");
                                    continue;
                                }
                            };

                            let (header, mav_msg) = match crate::message::deserialize_command(payload) {
                                Ok(parsed) => parsed,
                                Err(e) => {
                                    warn!(error = %e, "Failed to deserialize command, skipping");
                                    continue;
                                }
                            };

                            let conn = conn.clone();
                            let send_result = tokio::task::spawn_blocking(move || {
                                conn.send(&header, &mav_msg)
                            }).await;

                            match send_result {
                                Ok(Ok(_)) => {
                                    command_count += 1;
                                    debug!(commands_sent = command_count, "Command sent to MAVLink");
                                }
                                Ok(Err(e)) => {
                                    error!(error = %e, "Failed to send MAVLink command");
                                }
                                Err(e) => {
                                    error!(error = %e, "Spawn blocking task panicked");
                                }
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "Error receiving from Kafka command topic");
                        }
                    }
                }
            }
        }

        command_count
    }
}
