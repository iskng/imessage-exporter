use crate::databases::ingest_v2_record::IngestRecord;
use crate::proto::ImessageRecord;
use crate::{Database, DatabaseConnection, Message};
use anyhow::{anyhow, bail, Context, Result};
use ingest_protocol::v2::{client, Codec, Compression};
use ingest_protocol::v2::client::{ClientOptions, ProduceOutput};
use std::env;
use std::path::{Path, PathBuf};

const DEFAULT_CHUNK_SIZE: usize = 2000;
const DEFAULT_MAX_INFLIGHT: u16 = 8;
const DEFAULT_SOURCE: &str = "imessage-exporter";
const MAX_CHUNK_SIZE: usize = 16000;
const MIN_CHUNK_SIZE: usize = 250;
const MIN_INFLIGHT: u16 = 1;
const MAX_INFLIGHT: u16 = 128;

pub(crate) struct IngestV2Database {
    connection: DatabaseConnection,
    socket_path: PathBuf,
    source: String,
    chunk_hint: usize,
    inflight_hint: u16,
    codec: Codec,
    compression: Compression,
    watermark: Option<String>,
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

        let codec = env::var("DB_CODEC")
            .ok()
            .as_deref()
            .map(parse_codec)
            .transpose()? // Option<Result<T>> -> Result<Option<T>>
            .unwrap_or(Codec::Protobuf); // default => protobuf

        let compression = env::var("DB_COMPRESSION")
            .ok()
            .as_deref()
            .map(parse_compression)
            .transpose()? // Option<Result<T>> -> Result<Option<T>>
            .unwrap_or(Compression::Zstd); // default => zstd

        let watermark = env::var("DB_NEXT_WATERMARK").ok();

        Ok(Self {
            connection,
            socket_path,
            source,
            chunk_hint,
            inflight_hint,
            codec,
            compression,
            watermark,
        })
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
        let res: Result<(), anyhow::Error> = self.connection.runtime.block_on(async {
            send_records_v2(
                &self.socket_path,
                &self.source,
                self.chunk_hint,
                self.inflight_hint,
                self.codec,
                self.compression,
                self.watermark.clone(),
                records,
            )
            .await
        });
        res.map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { err.into() })
    }

    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

fn parse_codec(value: &str) -> Result<Codec> {
    match value.trim().to_lowercase().as_str() {
        "" | "auto" => Ok(Codec::Protobuf),
        "json" => Ok(Codec::Json),
        "binary" | "bin" => Ok(Codec::Binary),
        "protobuf" | "proto" | "pb" => Ok(Codec::Protobuf),
        other => Err(anyhow!("unsupported DB_CODEC '{}'", other)),
    }
}

fn parse_compression(value: &str) -> Result<Compression> {
    match value.trim().to_lowercase().as_str() {
        "" | "auto" => Ok(Compression::Zstd),
        "none" | "off" => Ok(Compression::None),
        "zstd" => Ok(Compression::Zstd),
        other => Err(anyhow!("unsupported DB_COMPRESSION '{}'", other)),
    }
}

// unified client-based sender using high-level v2 client
async fn send_records_v2(
    socket_path: &Path,
    source: &str,
    chunk_size: usize,
    max_inflight: u16,
    codec: Codec,
    compression: Compression,
    watermark: Option<String>,
    items: Vec<IngestRecord>,
) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    if !socket_path.exists() {
        bail!("socket path '{}' does not exist", socket_path.display());
    }
    // Compute next watermark time (max of available message timestamps)
    let mut max_micros: Option<i64> = None;
    for r in &items {
        for candidate in [r.date, r.date_delivered, r.date_read] {
            if let Some(v) = candidate {
                max_micros = Some(max_micros.map_or(v, |cur| cur.max(v)));
            }
        }
    }
    let next_watermark_time: chrono::DateTime<chrono::Utc> =
        crate::databases::ingest_v2_record::micros_to_datetime(max_micros)
            .unwrap_or_else(|| chrono::Utc::now());

    // Map to prost record type required by WireRecord bound
    let mut recs: Vec<ImessageRecord> = Vec::with_capacity(items.len());
    for r in items { recs.push(to_prost_record(r)); }
    let mut opts = ClientOptions::default();
    opts.chunk_size = chunk_size.max(1);
    opts.max_inflight = max_inflight.max(1);
    opts.codec = codec;
    opts.compression = compression;
    opts.source = source.to_string();
    // Use high-level helper; return items and optional next watermark (time+token)
    let next_token = watermark;
    client::send_with_init(socket_path, &opts, move |_init| async move {
        Ok(ProduceOutput { items: recs, next_watermark_time: Some(next_watermark_time), next_watermark_token: next_token })
    }).await.map_err(|e| e.into())
}

// (custom wire helpers removed; using ingest_protocol::v2::client exclusively)

fn to_prost_record(msg: IngestRecord) -> ImessageRecord {
    ImessageRecord {
        id: msg.id,
        rowid: msg.rowid,
        guid: msg.guid,
        text: msg.text,
        service: msg.service,
        platform: msg.platform,
        handle_id: msg.handle_id,
        destination_caller_id: msg.destination_caller_id,
        subject: msg.subject,
        date: msg.date,
        date_read: msg.date_read,
        date_delivered: msg.date_delivered,
        is_from_me: msg.is_from_me,
        is_read: msg.is_read,
        item_type: msg.item_type,
        other_handle: msg.other_handle,
        share_status: msg.share_status,
        share_direction: msg.share_direction,
        group_title: msg.group_title,
        group_action_type: msg.group_action_type,
        associated_message_guid: msg.associated_message_guid,
        associated_message_type: msg.associated_message_type,
        balloon_bundle_id: msg.balloon_bundle_id,
        expressive_send_style_id: msg.expressive_send_style_id,
        thread_originator_guid: msg.thread_originator_guid,
        thread_originator_part: msg.thread_originator_part,
        date_edited: msg.date_edited,
        chat_id: msg.chat_id,
        unique_chat_id: msg.unique_chat_id,
        num_attachments: msg.num_attachments,
        deleted_from: msg.deleted_from,
        num_replies: msg.num_replies,
        full_message: msg.full_message,
        thread_name: msg.thread_name,
        attachment_paths: msg.attachment_paths,
        is_deleted: msg.is_deleted,
        is_edited: msg.is_edited,
        is_reply: msg.is_reply,
        associated_message_emoji: msg.associated_message_emoji,
        phone_number: msg.phone_number,
    }
}
