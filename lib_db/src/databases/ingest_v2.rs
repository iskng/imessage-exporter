use crate::databases::ingest_v2_record::IngestRecord;
use crate::{Database, DatabaseConnection, Message};
use anyhow::{anyhow, Context, Result};
use ingest_protocol::v2::{
    sink::{CodecPreference, CompressionPreference, SessionSink, SinkBuilder},
    Codec, Compression,
};
use std::env;
use std::path::PathBuf;

const DEFAULT_CHUNK_SIZE: usize = 2000;
const DEFAULT_MAX_INFLIGHT: u16 = 8;
const DEFAULT_SOURCE: &str = "imessage-exporter";
const MAX_CHUNK_SIZE: usize = 4000;
const MIN_CHUNK_SIZE: usize = 250;
const MIN_INFLIGHT: u16 = 2;
const MAX_INFLIGHT: u16 = 32;

pub(crate) struct IngestV2Database {
    connection: DatabaseConnection,
    sink: SessionSink,
}

impl IngestV2Database {
    pub(crate) async fn create(
        connection: DatabaseConnection,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let socket_path = env::var("DBPATH")
            .map(PathBuf::from)
            .context("DBPATH must point to a Unix socket when using ingest_v2")?;
        if !socket_path.exists() {
            return Err(anyhow!(
                "ingest_v2 socket '{}' does not exist",
                socket_path.display()
            )
            .into());
        }

        let source = env::var("DB_SOURCE")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_SOURCE.to_string());

        let chunk_hint = env::var("DB_CHUNK_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|v| v.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE))
            .unwrap_or(DEFAULT_CHUNK_SIZE);

        let inflight_hint = env::var("DB_MAX_INFLIGHT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .map(|v| v.clamp(MIN_INFLIGHT, MAX_INFLIGHT))
            .unwrap_or(DEFAULT_MAX_INFLIGHT);

        let codec_pref = env::var("DB_CODEC")
            .ok()
            .and_then(|v| parse_codec_pref(&v).ok())
            .unwrap_or(CodecPreference::Auto);

        let compression_pref = env::var("DB_COMPRESSION")
            .ok()
            .and_then(|v| parse_compression_pref(&v).ok())
            .unwrap_or(CompressionPreference::Auto);

        let watermark = env::var("DB_NEXT_WATERMARK").ok();

        let sink = SinkBuilder::new(&socket_path)
            .source(source)
            .chunk_hint(chunk_hint)
            .max_inflight_hint(inflight_hint)
            .codec_preference(codec_pref)
            .compression_preference(compression_pref)
            .watermark(watermark)
            .build()?;

        Ok(Self { connection, sink })
    }
}

impl Database for IngestV2Database {
    fn insert_batch(
        &self,
        messages: Vec<Message>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if messages.is_empty() {
            return Ok(());
        }
        let records: Vec<IngestRecord> = messages.into_iter().map(IngestRecord::from).collect();
        self.connection
            .runtime
            .block_on(async { self.sink.send(records).await })
            .map_err(|err| err.into())
    }

    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

fn parse_codec_pref(value: &str) -> Result<CodecPreference> {
    match value.trim().to_lowercase().as_str() {
        "" | "auto" => Ok(CodecPreference::Auto),
        "json" => Ok(CodecPreference::Force(Codec::Json)),
        "binary" | "bin" => Ok(CodecPreference::Force(Codec::Binary)),
        other => Err(anyhow!("unsupported DB_CODEC '{}'", other)),
    }
}

fn parse_compression_pref(value: &str) -> Result<CompressionPreference> {
    match value.trim().to_lowercase().as_str() {
        "" | "none" | "off" => Ok(CompressionPreference::Disabled),
        "zstd" => Ok(CompressionPreference::Force(Compression::Zstd)),
        "auto" => Ok(CompressionPreference::Auto),
        other => Err(anyhow!("unsupported DB_COMPRESSION '{}'", other)),
    }
}
