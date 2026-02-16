use std::sync::Arc;

use mavlink::MavHeader;
use mavlink::ardupilotmega::MavMessage;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub type MavlinkMsg = (MavHeader, MavMessage);

pub struct MavlinkSource;

impl MavlinkSource {
    pub fn run(
        connection_string: &str,
        cancel_token: CancellationToken,
        channel_size: usize,
    ) -> anyhow::Result<mpsc::Receiver<MavlinkMsg>> {
        let connection_string = connection_string.to_string();
        let (tx, rx) = mpsc::channel(channel_size);

        // Connect before spawning so we can return connection errors immediately
        let conn = mavlink::connect::<MavMessage>(&connection_string)?;
        info!(connection = %connection_string, "Connected to MAVLink source");

        tokio::task::spawn_blocking(move || {
            Self::read_loop(conn, tx, cancel_token);
        });

        Ok(rx)
    }

    fn read_loop(
        conn: Box<dyn mavlink::MavConnection<MavMessage> + Send + Sync>,
        tx: mpsc::Sender<MavlinkMsg>,
        cancel_token: CancellationToken,
    ) {
        let conn = Arc::new(conn);
        loop {
            if cancel_token.is_cancelled() {
                info!("MAVLink reader shutting down");
                break;
            }

            match conn.recv() {
                Ok((header, msg)) => {
                    if tx.blocking_send((header, msg)).is_err() {
                        warn!("Channel closed, MAVLink reader shutting down");
                        break;
                    }
                }
                Err(e) => {
                    if cancel_token.is_cancelled() {
                        info!("MAVLink reader shutting down");
                        break;
                    }
                    error!(error = %e, "Error receiving MAVLink message");
                }
            }
        }
    }
}
