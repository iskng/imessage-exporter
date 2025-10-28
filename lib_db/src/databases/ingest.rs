use crate::{Database, DatabaseConnection, Message};
use anyhow::{anyhow, Context, Result};
use ingest_protocol::{
    client::{self, SenderOptions},
    GENERIC_INGEST_PROTOCOL_VERSION,
};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

const DEFAULT_CHUNK_SIZE: usize = 500;
const DEFAULT_WRITE_TIMEOUT_SECS: u64 = 10;
const DEFAULT_SOURCE: &str = "imessage-exporter";

pub use ingest_protocol::wire;

pub(crate) struct IngestDatabase {
    _connection: DatabaseConnection,
    socket_path: PathBuf,
    source: String,
    chunk_size: usize,
    write_timeout: Duration,
    protocol_version: u16,
    next_watermark: Option<String>,
}

impl IngestDatabase {
    pub(crate) async fn create(
        connection: DatabaseConnection,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let socket_path = std::env::var("DBPATH")
            .map(PathBuf::from)
            .context("DBPATH must point to a Unix socket when using the ingest protocol")?;

        ensure_socket_parent_exists(&socket_path)?;
        if !socket_path.exists() {
            return Err(anyhow!("ingest socket '{}' does not exist", socket_path.display()).into());
        }

        let source = std::env::var("DB_SOURCE")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_SOURCE.to_string());

        let chunk_size = std::env::var("DB_CHUNK_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_CHUNK_SIZE);

        let write_timeout = std::env::var("DB_WRITE_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(DEFAULT_WRITE_TIMEOUT_SECS));

        let protocol_version = std::env::var("DB_PROTOCOL_VERSION")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(GENERIC_INGEST_PROTOCOL_VERSION);

        let next_watermark = std::env::var("DB_NEXT_WATERMARK").ok();

        Ok(Self {
            _connection: connection,
            socket_path,
            source,
            chunk_size,
            write_timeout,
            protocol_version,
            next_watermark,
        })
    }
}

impl Database for IngestDatabase {
    fn insert_batch(
        &self,
        messages: Vec<Message>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if messages.is_empty() {
            return Ok(());
        }

        ensure_socket_parent_exists(&self.socket_path)?;
        let opts = SenderOptions {
            chunk_size: self.chunk_size,
            write_timeout_secs: self.write_timeout.as_secs(),
            session_id: None,
            protocol_version: Some(self.protocol_version),
            next_watermark: self.next_watermark.clone(),
        };

        client::send_all(&messages, &self.socket_path, &self.source, &opts)?;
        Ok(())
    }

    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Each insert_batch completes its own session, so there is nothing to flush.
        Ok(())
    }
}

fn ensure_socket_parent_exists(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent directory '{}'", parent.display()))?;
    }
    Ok(())
}

// Re-export protocol helper types that callers relied on previously.
pub use ingest_protocol::{CommittedSession, GenericIngestArgs, GenericIngestState};
