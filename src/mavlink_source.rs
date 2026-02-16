use std::sync::Arc;

use mavlink::MavHeader;
use mavlink::ardupilotmega::MavMessage;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Type alias for a thread-safe MAVLink connection.
pub type MavConnection = dyn mavlink::MavConnection<MavMessage> + Send + Sync;

/// A received MAVLink message with its header.
pub type MavlinkMsg = (MavHeader, MavMessage);

/// Create a MAVLink connection wrapped in Arc for shared use.
pub fn connect(connection_string: &str) -> anyhow::Result<Arc<Box<MavConnection>>> {
    let conn = mavlink::connect::<MavMessage>(connection_string)?;
    info!(connection = %connection_string, "Connected to MAVLink");
    Ok(Arc::new(conn))
}

/// Reads MAVLink messages from a connection and sends them over an mpsc channel.
pub struct MavlinkSource;

impl MavlinkSource {
    /// Spawns a blocking read loop that receives MAVLink messages and forwards
    /// them through the returned channel. Stops when the cancel token is triggered.
    pub fn run(
        conn: Arc<Box<MavConnection>>,
        cancel_token: CancellationToken,
        channel_size: usize,
    ) -> mpsc::Receiver<MavlinkMsg> {
        let (tx, rx) = mpsc::channel(channel_size);

        tokio::task::spawn_blocking(move || {
            Self::read_loop(conn, tx, cancel_token);
        });

        rx
    }

    fn read_loop(
        conn: Arc<Box<MavConnection>>,
        tx: mpsc::Sender<MavlinkMsg>,
        cancel_token: CancellationToken,
    ) {
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
