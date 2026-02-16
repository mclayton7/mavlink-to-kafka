use mavlink::ardupilotmega::MavMessage;
use mavlink::{MavHeader, Message};
use serde::Serialize;

#[derive(Serialize)]
struct KafkaPayload<'a> {
    header: HeaderPayload,
    message: &'a MavMessage,
}

#[derive(Serialize)]
struct HeaderPayload {
    system_id: u8,
    component_id: u8,
    sequence: u8,
}

pub fn extract_message_name(msg: &MavMessage) -> &'static str {
    msg.message_name()
}

pub fn build_topic_name(prefix: &str, message_name: &str) -> String {
    format!("{prefix}.{message_name}")
}

pub fn serialize_for_kafka(header: &MavHeader, msg: &MavMessage) -> anyhow::Result<Vec<u8>> {
    let payload = KafkaPayload {
        header: HeaderPayload {
            system_id: header.system_id,
            component_id: header.component_id,
            sequence: header.sequence,
        },
        message: msg,
    };
    let json = serde_json::to_vec(&payload)?;
    Ok(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_message_name_heartbeat() {
        let msg = MavMessage::HEARTBEAT(mavlink::ardupilotmega::HEARTBEAT_DATA {
            custom_mode: 0,
            mavtype: mavlink::ardupilotmega::MavType::MAV_TYPE_QUADROTOR,
            autopilot: mavlink::ardupilotmega::MavAutopilot::MAV_AUTOPILOT_ARDUPILOTMEGA,
            base_mode: mavlink::ardupilotmega::MavModeFlag::empty(),
            system_status: mavlink::ardupilotmega::MavState::MAV_STATE_ACTIVE,
            mavlink_version: 3,
        });
        assert_eq!(extract_message_name(&msg), "HEARTBEAT");
    }

    #[test]
    fn test_build_topic_name() {
        assert_eq!(build_topic_name("mavlink", "HEARTBEAT"), "mavlink.HEARTBEAT");
        assert_eq!(build_topic_name("uav", "GPS_RAW_INT"), "uav.GPS_RAW_INT");
    }

    #[test]
    fn test_serialize_for_kafka_produces_valid_json() {
        let header = MavHeader {
            system_id: 1,
            component_id: 1,
            sequence: 42,
        };
        let msg = MavMessage::HEARTBEAT(mavlink::ardupilotmega::HEARTBEAT_DATA {
            custom_mode: 0,
            mavtype: mavlink::ardupilotmega::MavType::MAV_TYPE_QUADROTOR,
            autopilot: mavlink::ardupilotmega::MavAutopilot::MAV_AUTOPILOT_ARDUPILOTMEGA,
            base_mode: mavlink::ardupilotmega::MavModeFlag::empty(),
            system_status: mavlink::ardupilotmega::MavState::MAV_STATE_ACTIVE,
            mavlink_version: 3,
        });

        let bytes = serialize_for_kafka(&header, &msg).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        // Check header fields
        assert_eq!(value["header"]["system_id"], 1);
        assert_eq!(value["header"]["component_id"], 1);
        assert_eq!(value["header"]["sequence"], 42);

        // Check message is present
        assert!(value["message"].is_object(), "message should be an object");
    }

    #[test]
    fn test_serialize_different_message_types() {
        let header = MavHeader {
            system_id: 2,
            component_id: 1,
            sequence: 0,
        };
        let msg = MavMessage::SYS_STATUS(mavlink::ardupilotmega::SYS_STATUS_DATA {
            onboard_control_sensors_present: mavlink::ardupilotmega::MavSysStatusSensor::empty(),
            onboard_control_sensors_enabled: mavlink::ardupilotmega::MavSysStatusSensor::empty(),
            onboard_control_sensors_health: mavlink::ardupilotmega::MavSysStatusSensor::empty(),
            load: 500,
            voltage_battery: 12000,
            current_battery: 100,
            battery_remaining: 75,
            drop_rate_comm: 0,
            errors_comm: 0,
            errors_count1: 0,
            errors_count2: 0,
            errors_count3: 0,
            errors_count4: 0,
        });

        let bytes = serialize_for_kafka(&header, &msg).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["header"]["system_id"], 2);
        assert!(value["message"].is_object());
    }
}
