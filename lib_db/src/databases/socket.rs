use crate::{types::Message, Database, DatabaseConnection};
use std::sync::Arc;
use std::{env, path::PathBuf};
use tokio::sync::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

const DEFAULT_SOCKET_PATH: &str = "/tmp/imessage-exporter.sock";
const CMD_INSERT: u8 = b'I';
const CMD_FLUSH: u8 = b'F';

pub(crate) struct SocketDatabase {
    connection: DatabaseConnection,
    _socket_path: PathBuf,
    stream: Arc<Mutex<Option<UnixStream>>>,
}

impl SocketDatabase {
    pub(crate) async fn create(
        connection: DatabaseConnection,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let socket_path = if let Ok(path) = env::var("DBPATH") {
            if path.starts_with('/') {
                PathBuf::from(path)
            } else {
                PathBuf::from(DEFAULT_SOCKET_PATH)
            }
        } else {
            PathBuf::from(DEFAULT_SOCKET_PATH)
        };

        eprintln!("Using Unix socket at: {}", socket_path.display());
        let stream = UnixStream::connect(&socket_path).await?;

        Ok(Self {
            connection,
            _socket_path: socket_path,
            stream: Arc::new(Mutex::new(Some(stream))),
        })
    }
}

impl Database for SocketDatabase {
    fn insert_batch(
        &self,
        messages: Vec<Message>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let runtime = &self.connection.runtime;

        runtime.block_on(async {
            if let Some(stream) = &mut *self.stream.lock().await {
                stream.write_u8(CMD_INSERT).await?;
                let data = serde_json::to_vec(&messages)?;
                stream.write_u32(data.len() as u32).await?;
                stream.write_all(&data).await?;
                stream.flush().await?;

                let mut response = [0u8; 1];
                stream.read_exact(&mut response).await?;
                if response[0] != b'K' {
                    return Err("Server error".into());
                }
            }
            Ok(())
        })
    }

    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let runtime = &self.connection.runtime;

        runtime.block_on(async {
            if let Some(stream) = &mut *self.stream.lock().await {
                stream.write_u8(CMD_FLUSH).await?;
                stream.flush().await?;
            }
            Ok(())
        })
    }
}
