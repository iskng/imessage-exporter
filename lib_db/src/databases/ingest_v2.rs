use crate::databases::ingest_v2_record::IngestRecord;
use crate::{Database, DatabaseConnection, Message};
use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use ingest_protocol::v2::{wire as w, Codec, Compression, GENERIC_INGEST_PROTOCOL_VERSION_V2};
use ingest_models::IngestInit as ProtoIngestInit;
use serde::{de::DeserializeOwned, Serialize};
use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time;

const DEFAULT_CHUNK_SIZE: usize = 2000;
const DEFAULT_MAX_INFLIGHT: u16 = 8;
const DEFAULT_SOURCE: &str = "imessage-exporter";
const MAX_CHUNK_SIZE: usize = 16000;
const MIN_CHUNK_SIZE: usize = 250;
const MIN_INFLIGHT: u16 = 1;
const MAX_INFLIGHT: u16 = 128;

const FRAME_MAGIC: u32 = 0x4950_4752; // "IPGR"
const FRAME_HEADER_LEN: usize = 12;

pub(crate) struct IngestV2Database {
    connection: DatabaseConnection,
    socket_path: PathBuf,
    source: String,
    chunk_hint: usize,
    inflight_hint: u16,
    codec: Codec,
    compression: Compression,
    zstd_level: i32,
    skip_init: bool,
    parallel_sessions: usize,
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
            .unwrap_or(Codec::Binary); // default => binary

        let compression = env::var("DB_COMPRESSION")
            .ok()
            .as_deref()
            .map(parse_compression)
            .transpose()? // Option<Result<T>> -> Result<Option<T>>
            .unwrap_or(Compression::Zstd); // default => zstd

        let zstd_level = env::var("DB_ZSTD_LEVEL")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .map(|v| v.clamp(1, 22))
            .unwrap_or(1);

        let skip_init = env::var("DB_SKIP_INIT")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let parallel_sessions = env::var("DB_PARALLEL_SESSIONS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|v| v.clamp(1, 16))
            .unwrap_or(1);

        let watermark = env::var("DB_NEXT_WATERMARK").ok();

        Ok(Self {
            connection,
            socket_path,
            source,
            chunk_hint,
            inflight_hint,
            codec,
            compression,
            zstd_level,
            skip_init,
            parallel_sessions,
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
        let parallel = self.parallel_sessions.max(1);
        if self.codec == Codec::Protobuf {
            self.connection
                .runtime
                .block_on(async {
                    send_records_protobuf(
                        &self.socket_path,
                        &self.source,
                        self.chunk_hint,
                        self.inflight_hint,
                        self.compression,
                        self.skip_init,
                        self.watermark.clone(),
                        records,
                    )
                    .await
                })
                .map_err(|err| err.into())
        } else if parallel == 1 {
            self.connection
                .runtime
                .block_on(async {
                    send_records(
                        &self.socket_path,
                        &self.source,
                        self.chunk_hint,
                        self.inflight_hint,
                        self.codec,
                        self.compression,
                        self.zstd_level,
                        self.skip_init,
                        self.watermark.clone(),
                        records,
                    )
                    .await
                })
                .map_err(|err| err.into())
        } else {
            // Split into roughly even slices by total payload target ~ chunk*inflight per session
            let per_session = (self.chunk_hint as usize * self.inflight_hint as usize).max(1);
            let per_session = per_session * 4; // give each session multiple windows
            let mut tasks = Vec::new();
            let path = self.socket_path.clone();
            let source = self.source.clone();
            let chunk_hint = self.chunk_hint;
            let inflight_hint = self.inflight_hint;
            let codec = self.codec;
            let compression = self.compression;
            let zstd_level = self.zstd_level;
            let skip_init = self.skip_init;
            let watermark = self.watermark.clone();
            let mut idx = 0;
            while idx < records.len() {
                let end = (idx + per_session).min(records.len());
                let slice = records[idx..end].to_vec();
                idx = end;
                let path_clone = path.clone();
                let source_clone = source.clone();
                let wm = watermark.clone();
                tasks.push(tokio::spawn(async move {
                    send_records(
                        &path_clone,
                        &source_clone,
                        chunk_hint,
                        inflight_hint,
                        codec,
                        compression,
                        zstd_level,
                        skip_init,
                        wm,
                        slice,
                    )
                    .await
                }));
                if tasks.len() >= parallel { break; }
            }
            self.connection.runtime.block_on(async move {
                for t in tasks {
                    t.await??;
                }
                Ok(())
            })
            .map_err(|err| err.into())
        }
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

async fn send_records(
    socket_path: &Path,
    source: &str,
    chunk_size: usize,
    max_inflight: u16,
    codec: Codec,
    compression: Compression,
    zstd_level: i32,
    skip_init: bool,
    watermark: Option<String>,
    items: Vec<IngestRecord>,
) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    if !socket_path.exists() {
        bail!("socket path '{}' does not exist", socket_path.display());
    }

    let mut stream = UnixStream::connect(socket_path).await?;
    let total_entries = items.len();

    // Adopt local transport settings; may be overridden by optional server Init.
    let mut local_codec = codec;
    let mut local_compression = compression;
    let mut local_chunk = chunk_size.max(1);
    let mut local_inflight = max_inflight.max(1);
    let mut session_id = format!(
        "sess-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let mut hello_source = source.to_string();
    let mut nonce_echo: Option<String> = None;
    let mut job_digest: Option<String> = None;
    let mut local_watermark = watermark; // may be overridden by typed Init

    // Try to read an Init frame with a small timeout. If not present, proceed normally.
    if !skip_init {
    if let Ok(Ok((op, c, comp, payload))) = time::timeout(Duration::from_millis(500), read_frame(&mut stream)).await {
        if op == 6u16 {
            let init: w::Init = decode_payload(c, comp, &payload)?;
            local_codec = init.transport.codec;
            local_compression = init.transport.compression;
            if init.transport.chunk_size > 0 { local_chunk = init.transport.chunk_size; }
            if init.transport.max_inflight > 0 { local_inflight = init.transport.max_inflight; }
            session_id = init.session_id.clone();
            hello_source = init.source.clone();
            job_digest = init.job_digest.clone();
            nonce_echo = init.nonce.clone();

            // Decode typed Init if present and matches the unified init type
            if let (Some(url), Some(bytes)) = (init.job_type_url.as_deref(), init.job_payload.as_ref()) {
                if url == ProtoIngestInit::TYPE_URL {
                    if let Ok(job) = ProtoIngestInit::decode(bytes.as_slice()) {
                        adopt_proto_transport(&job, &mut local_codec, &mut local_compression, &mut local_chunk, &mut local_inflight);
                        // Adopt watermark token/time if provided
                        if let Some(wm) = job.watermark.as_ref() {
                            if let Some(token) = wm.token.as_ref() {
                                if !token.is_empty() { local_watermark = Some(token.clone()); }
                            } else if let Some(tiso) = wm.time_iso.as_ref() {
                                if !tiso.is_empty() { local_watermark = Some(tiso.clone()); }
                            }
                        }
                    }
                }
            }
        } else {
            bail!("unexpected frame {:?} before Hello", op);
        }
    }
    }

    let chunk = local_chunk;
    let total_batches = (total_entries + chunk - 1) / chunk;

    // Send Hello
    let hello = w::Hello {
        protocol_version: GENERIC_INGEST_PROTOCOL_VERSION_V2,
        session_id: session_id.clone(),
        source: hello_source,
        sent_at: Utc::now(),
        total_entries,
        total_batches,
        chunk_size: chunk,
        max_inflight: local_inflight,
        codec: local_codec,
        compression: local_compression,
        watermark: local_watermark.clone(),
        job_digest,
        nonce_echo,
    };
    write_message(&mut stream, local_codec, local_compression, zstd_level, &w::Message::<IngestRecord>::Hello(hello)).await?;

    // Expect Hello ack
    let mut negotiated_window = local_inflight;
    let hello_ack = read_ack(&mut stream).await?;
    if hello_ack.status == w::AckStatus::Rejected {
        bail!("ingest service rejected session");
    }
    if hello_ack.inflight > 0 {
        negotiated_window = hello_ack.inflight;
    }

    // Optional debug output
    if std::env::var("DB_DEBUG").ok().as_deref() == Some("1") {
        eprintln!(
            "ingest v2 negotiated: codec={:?} compression={:?} chunk={} inflight={} (init_skipped={})",
            local_codec, local_compression, chunk, negotiated_window, skip_init
        );
    }

    // Send batches with simple pipelining
    let mut iter = items.into_iter();
    let mut inflight: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
    let mut next_index = 0usize;
    let mut window = negotiated_window.max(1) as usize;
    loop {
        while inflight.len() < window {
            let mut entries = Vec::with_capacity(chunk);
            for _ in 0..chunk {
                match iter.next() {
                    Some(item) => entries.push(item),
                    None => break,
                }
            }
            if entries.is_empty() {
                break;
            }
            let batch = w::HistoryBatch::<IngestRecord> {
                session_id: session_id.clone(),
                batch_index: next_index,
                batch_count: total_batches,
                entries,
            };
            write_message(&mut stream, local_codec, local_compression, zstd_level, &w::Message::HistoryBatch(batch)).await?;
            inflight.push_back(next_index);
            next_index += 1;
        }
        if inflight.is_empty() {
            break;
        }
        let ack = read_ack(&mut stream).await?;
        match ack.status {
            w::AckStatus::Accepted => {
                if let Some(expected_seq) = inflight.pop_front() {
                    if ack.next_expected_batch != expected_seq + 1 {
                        bail!(
                            "server expected batch {}, got ack for {}",
                            expected_seq + 1,
                            ack.next_expected_batch
                        );
                    }
                }
                if ack.inflight > 0 {
                    window = ack.inflight as usize;
                    if window == 0 {
                        window = 1;
                    }
                }
            }
            w::AckStatus::Retry => bail!("server requested retry but client does not buffer batches yet"),
            w::AckStatus::Rejected => bail!("ingest service rejected batch"),
            w::AckStatus::Completed => break,
        }
    }

    // Send complete
    let complete = w::Complete {
        session_id,
        final_batches: total_batches,
        final_entries: total_entries,
        completed_at: Utc::now(),
        next_watermark: local_watermark,
    };
    write_message(&mut stream, local_codec, local_compression, zstd_level, &w::Message::<IngestRecord>::Complete(complete)).await?;
    let final_ack = read_ack(&mut stream).await?;
    if final_ack.status != w::AckStatus::Completed {
        bail!("ingest service did not confirm completion");
    }
    if final_ack.accepted_entries != total_entries {
        bail!(
            "ingest service reported {} entries, expected {}",
            final_ack.accepted_entries,
            total_entries
        );
    }
    Ok(())
}

async fn write_message<T>(
    stream: &mut UnixStream,
    codec: Codec,
    compression: Compression,
    zstd_level: i32,
    message: &w::Message<T>,
) -> Result<()>
where
    T: Serialize,
{
    let opcode = match message {
        w::Message::Hello(_) => 1u16,
        w::Message::HistoryBatch(_) => 2u16,
        w::Message::Complete(_) => 3u16,
        w::Message::Abort(_) => 4u16,
    };
    let payload = encode_payload(codec, compression, zstd_level, message)?;
    write_frame(stream, opcode, codec, compression, &payload).await
}

async fn write_frame(
    stream: &mut UnixStream,
    opcode: u16,
    codec: Codec,
    compression: Compression,
    payload: &[u8],
) -> Result<()> {
    let mut header = [0u8; FRAME_HEADER_LEN];
    header[0..4].copy_from_slice(&FRAME_MAGIC.to_be_bytes());
    header[4..6].copy_from_slice(&opcode.to_be_bytes());
    header[6] = codec as u8;
    header[7] = compression as u8;
    header[8..12].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    stream.write_all(&header).await?;
    stream.write_all(payload).await?;
    Ok(())
}

async fn read_ack(stream: &mut UnixStream) -> Result<w::Ack> {
    let (opcode, codec, compression, payload) = read_frame(stream).await?;
    if opcode != 5u16 {
        bail!("expected ack frame, got opcode {}", opcode);
    }
    decode_payload(codec, compression, &payload)
}

async fn read_frame(stream: &mut UnixStream) -> Result<(u16, Codec, Compression, Vec<u8>)> {
    let mut header = [0u8; FRAME_HEADER_LEN];
    stream.read_exact(&mut header).await?;
    let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
    if magic != FRAME_MAGIC {
        bail!("invalid frame magic {:x}", magic);
    }
    let opcode = u16::from_be_bytes([header[4], header[5]]);
    let codec = match header[6] {
        0 => Codec::Json,
        1 => Codec::Binary,
        2 => Codec::Protobuf,
        other => bail!("unknown codec {}", other),
    };
    let compression = match header[7] {
        0 => Compression::None,
        1 => Compression::Zstd,
        other => bail!("unknown compression {}", other),
    };
    let len = u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok((opcode, codec, compression, payload))
}

fn encode_payload(value_codec: Codec, value_compression: Compression, zstd_level: i32, value: &impl Serialize) -> Result<Vec<u8>> {
    let bytes = match value_codec {
        Codec::Json => serde_json::to_vec(value)?,
        Codec::Binary => postcard::to_allocvec(value)?,
        Codec::Protobuf => bail!("protobuf codec is not supported for this record type"),
    };
    let out = match value_compression {
        Compression::None => bytes,
        Compression::Zstd => zstd::stream::encode_all(bytes.as_slice(), zstd_level)?,
    };
    Ok(out)
}

fn decode_payload<T>(payload_codec: Codec, payload_compression: Compression, data: &[u8]) -> Result<T>
where
    T: DeserializeOwned,
{
    let decompressed = match payload_compression {
        Compression::None => data.to_vec(),
        Compression::Zstd => zstd::stream::decode_all(data)?,
    };
    let value = match payload_codec {
        Codec::Json => serde_json::from_slice(&decompressed)?,
        Codec::Binary => postcard::from_bytes(&decompressed)?,
        Codec::Protobuf => bail!("protobuf codec is not supported for this record type"),
    };
    Ok(value)
}

fn adopt_proto_transport(
    job: &ProtoIngestInit,
    codec: &mut Codec,
    compression: &mut Compression,
    chunk: &mut usize,
    inflight: &mut u16,
) {
    // TransportContext uses strings for codec/compression
    if let Some(tc) = job.transport.as_ref() {
        let c = tc.codec.trim().to_lowercase();
        match c.as_str() {
            "json" => *codec = Codec::Json,
            "binary" | "bin" => *codec = Codec::Binary,
            // protobuf not supported in our custom client encoder yet
            _ => {}
        }
        let comp = tc.compression.trim().to_lowercase();
        match comp.as_str() {
            "none" | "off" => *compression = Compression::None,
            "zstd" => *compression = Compression::Zstd,
            _ => {}
        }
        if tc.chunk_size > 0 {
            let v = tc.chunk_size as usize;
            *chunk = v.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE);
        }
        if tc.max_inflight > 0 {
            let v = tc.max_inflight as u16;
            *inflight = v.clamp(MIN_INFLIGHT, MAX_INFLIGHT);
        }
    }
}

async fn send_records_protobuf(
    socket_path: &Path,
    source: &str,
    chunk_size: usize,
    max_inflight: u16,
    compression: Compression,
    _skip_init: bool,
    watermark: Option<String>,
    items: Vec<IngestRecord>,
) -> Result<()> {
    if items.is_empty() { return Ok(()); }
    if !socket_path.exists() { bail!("socket path '{}' does not exist", socket_path.display()); }
    let mut recs: Vec<ImessageRecord> = Vec::with_capacity(items.len());
    for r in items { recs.push(to_prost_record(r)); }
    let mut opts = client::ClientOptions::default();
    opts.chunk_size = chunk_size.max(1);
    opts.max_inflight = max_inflight.max(1);
    opts.codec = Codec::Protobuf;
    opts.compression = compression;
    opts.source = source.to_string();
    opts.watermark = watermark;
    client::send_with_init_path_raw(socket_path, &opts, move |_init| Ok(recs)).await.map_err(|e| e.into())
}

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
