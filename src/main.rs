mod config;
mod kafka_sink;
mod kafka_source;
mod mavlink_source;
mod message;

use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

const CHANNEL_SIZE: usize = 1000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app_config = config::AppConfig::load()?;

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&app_config.logging.level)),
        )
        .init();

    info!(
        mavlink_connection = %app_config.mavlink.connection_string,
        kafka_brokers = %app_config.kafka.brokers,
        topic_prefix = %app_config.kafka.topic_prefix,
        commands_enabled = app_config.kafka.commands.enabled,
        "Starting mavlink-to-kafka"
    );

    // Setup graceful shutdown
    let cancel_token = CancellationToken::new();
    let shutdown_token = cancel_token.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!(error = %e, "Failed to listen for ctrl+c");
            return;
        }
        info!("Received shutdown signal");
        shutdown_token.cancel();
    });

    // Connect to MAVLink (shared connection)
    let conn = mavlink_source::connect(&app_config.mavlink.connection_string)?;

    // Start MAVLink reader
    let mut rx = mavlink_source::MavlinkSource::run(
        conn.clone(),
        cancel_token.clone(),
        CHANNEL_SIZE,
    );

    // Conditionally start command consumer
    let command_handle = if app_config.kafka.commands.enabled {
        let command_consumer = kafka_source::KafkaCommandConsumer::new(
            &app_config.kafka.brokers,
            &app_config.kafka.commands.command_topic,
            &app_config.kafka.commands.consumer_group_id,
            &app_config.kafka.commands.consumer_properties,
        )?;
        let cmd_conn = conn.clone();
        let cmd_token = cancel_token.clone();
        Some(tokio::spawn(async move {
            command_consumer.run(cmd_conn, cmd_token).await
        }))
    } else {
        None
    };

    // Create Kafka producer
    let kafka_sink = kafka_sink::KafkaSink::new(
        &app_config.kafka.brokers,
        &app_config.kafka.topic_prefix,
        &app_config.kafka.producer_properties,
    )?;

    info!("Pipeline running, press Ctrl+C to stop");

    // Main loop
    let mut message_count: u64 = 0;
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!(messages_processed = message_count, "Shutting down");
                break;
            }
            msg = rx.recv() => {
                match msg {
                    Some((header, mav_msg)) => {
                        let message_name = message::extract_message_name(&mav_msg);

                        match message::serialize_for_kafka(&header, &mav_msg) {
                            Ok(payload) => {
                                kafka_sink.publish(message_name, header.system_id, &payload).await;
                                message_count += 1;

                                if message_count % 1000 == 0 {
                                    info!(messages_processed = message_count, "Progress");
                                }
                            }
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    message_name,
                                    "Failed to serialize message, skipping"
                                );
                            }
                        }
                    }
                    None => {
                        info!("MAVLink channel closed");
                        break;
                    }
                }
            }
        }
    }

    // Wait for command consumer to finish
    if let Some(handle) = command_handle {
        match handle.await {
            Ok(command_count) => {
                info!(commands_sent = command_count, "Command consumer finished");
            }
            Err(e) => {
                error!(error = %e, "Command consumer task failed");
            }
        }
    }

    info!(total_messages = message_count, "Shutdown complete");
    Ok(())
}
