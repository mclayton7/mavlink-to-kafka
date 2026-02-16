use std::collections::HashMap;

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::Deserialize;

/// Top-level application configuration, loaded from TOML / env / CLI.
#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub mavlink: MavlinkConfig,
    pub kafka: KafkaConfig,
    pub logging: LoggingConfig,
}

/// MAVLink connection settings.
#[derive(Debug, Deserialize)]
pub struct MavlinkConfig {
    /// Connection string, e.g. `"udpin:0.0.0.0:14550"`.
    pub connection_string: String,
}

/// Kafka producer and command consumer settings.
#[derive(Debug, Deserialize)]
pub struct KafkaConfig {
    /// Comma-separated broker addresses.
    pub brokers: String,
    /// Topic prefix — outbound messages go to `<prefix>.<MESSAGE_NAME>`.
    pub topic_prefix: String,
    /// Extra rdkafka producer properties (e.g. `linger.ms`, `compression.type`).
    #[serde(default)]
    pub producer_properties: HashMap<String, String>,
    /// Optional Kafka-to-MAVLink command consumer configuration.
    #[serde(default)]
    pub commands: CommandConfig,
}

/// Configuration for the Kafka command consumer (Kafka -> MAVLink path).
/// Disabled by default so existing deployments are unaffected.
#[derive(Debug, Deserialize)]
pub struct CommandConfig {
    /// Whether to start the command consumer.
    pub enabled: bool,
    /// Kafka topic to consume command messages from.
    pub command_topic: String,
    /// Consumer group ID for offset tracking.
    pub consumer_group_id: String,
    /// Extra rdkafka consumer properties.
    #[serde(default)]
    pub consumer_properties: HashMap<String, String>,
}

impl Default for CommandConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            command_topic: "mavlink.commands".to_string(),
            consumer_group_id: "mavlink-to-kafka".to_string(),
            consumer_properties: HashMap::new(),
        }
    }
}

/// Logging configuration.
#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    /// Log level filter: trace, debug, info, warn, or error.
    pub level: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            mavlink: MavlinkConfig {
                connection_string: "udpin:0.0.0.0:14550".to_string(),
            },
            kafka: KafkaConfig {
                brokers: "localhost:9092".to_string(),
                topic_prefix: "mavlink".to_string(),
                producer_properties: HashMap::new(),
                commands: CommandConfig::default(),
            },
            logging: LoggingConfig {
                level: "info".to_string(),
            },
        }
    }
}

impl AppConfig {
    /// Loads configuration by layering defaults, TOML file, and environment
    /// variables (prefixed with `MAVLINK_TO_KAFKA__`).
    pub fn load(config_path: Option<&str>) -> anyhow::Result<Self> {
        let mut figment = Figment::from(Serialized::defaults(AppConfig::default()));

        if let Some(path) = config_path {
            figment = figment.merge(Toml::file(path));
        } else {
            // Try default config.toml if it exists
            figment = figment.merge(Toml::file("config.toml"));
        }

        figment = figment.merge(Env::prefixed("MAVLINK_TO_KAFKA__").split("__"));

        let config: AppConfig = figment.extract()?;
        Ok(config)
    }
}

impl serde::Serialize for AppConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("AppConfig", 3)?;
        state.serialize_field("mavlink", &self.mavlink)?;
        state.serialize_field("kafka", &self.kafka)?;
        state.serialize_field("logging", &self.logging)?;
        state.end()
    }
}

impl serde::Serialize for MavlinkConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("MavlinkConfig", 1)?;
        state.serialize_field("connection_string", &self.connection_string)?;
        state.end()
    }
}

impl serde::Serialize for KafkaConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("KafkaConfig", 4)?;
        state.serialize_field("brokers", &self.brokers)?;
        state.serialize_field("topic_prefix", &self.topic_prefix)?;
        state.serialize_field("producer_properties", &self.producer_properties)?;
        state.serialize_field("commands", &self.commands)?;
        state.end()
    }
}

impl serde::Serialize for CommandConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("CommandConfig", 4)?;
        state.serialize_field("enabled", &self.enabled)?;
        state.serialize_field("command_topic", &self.command_topic)?;
        state.serialize_field("consumer_group_id", &self.consumer_group_id)?;
        state.serialize_field("consumer_properties", &self.consumer_properties)?;
        state.end()
    }
}

impl serde::Serialize for LoggingConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("LoggingConfig", 1)?;
        state.serialize_field("level", &self.level)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AppConfig::default();
        assert_eq!(config.mavlink.connection_string, "udpin:0.0.0.0:14550");
        assert_eq!(config.kafka.brokers, "localhost:9092");
        assert_eq!(config.kafka.topic_prefix, "mavlink");
        assert_eq!(config.logging.level, "info");
        assert!(config.kafka.producer_properties.is_empty());
    }

    #[test]
    fn test_default_command_config() {
        let config = AppConfig::default();
        assert!(!config.kafka.commands.enabled);
        assert_eq!(config.kafka.commands.command_topic, "mavlink.commands");
        assert_eq!(config.kafka.commands.consumer_group_id, "mavlink-to-kafka");
        assert!(config.kafka.commands.consumer_properties.is_empty());
    }

    #[test]
    fn test_load_defaults_without_file() {
        let config = AppConfig::load(None).unwrap();
        assert_eq!(config.kafka.topic_prefix, "mavlink");
        assert!(!config.kafka.commands.enabled);
    }

    #[test]
    fn test_toml_parsing() {
        let dir = std::env::temp_dir().join("mavlink_to_kafka_test");
        std::fs::create_dir_all(&dir).unwrap();
        let toml_path = dir.join("test_config.toml");
        std::fs::write(
            &toml_path,
            r#"
[mavlink]
connection_string = "tcpout:127.0.0.1:5760"

[kafka]
brokers = "kafka1:9092,kafka2:9092"
topic_prefix = "uav"

[kafka.commands]
enabled = true
command_topic = "uav.commands"
consumer_group_id = "my-group"

[logging]
level = "debug"
"#,
        )
        .unwrap();

        let config = AppConfig::load(Some(toml_path.to_str().unwrap())).unwrap();
        assert_eq!(config.mavlink.connection_string, "tcpout:127.0.0.1:5760");
        assert_eq!(config.kafka.brokers, "kafka1:9092,kafka2:9092");
        assert_eq!(config.kafka.topic_prefix, "uav");
        assert_eq!(config.logging.level, "debug");
        assert!(config.kafka.commands.enabled);
        assert_eq!(config.kafka.commands.command_topic, "uav.commands");
        assert_eq!(config.kafka.commands.consumer_group_id, "my-group");

        std::fs::remove_dir_all(&dir).ok();
    }
}
