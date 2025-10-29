//! Ingest Protocol v2 Example
//!
//! Starts a binary-framed ingest v2 server, runs `imessage-exporter`
//! (`DB_PROTOCOL=ingest_v2`), and prints stats similar to other demos.

use anyhow::anyhow;
use ingest_protocol::v2::{server, wire as w, Codec, Compression, TransportConfig};
use ingest_models::{AuthContext, IngestInit, TransportContext as ProtoTransportContext, WatermarkContext};
use lib_db::{databases::ingest_v2_record::IngestRecord, Message};
use std::{
    io::ErrorKind,
    process::Command,
    sync::{Arc, Mutex},
};
use tempfile::TempDir;
use tokio::{net::UnixListener, sync::oneshot};

#[derive(Default, Debug, Clone)]
struct ServerStats {
    total_entries: usize,
    sessions: usize,
    first_record: Option<IngestRecord>,
    last_record: Option<IngestRecord>,
    watermark: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let socket_path = temp_dir.path().join("ingest_v2.sock");
    println!("Starting ingest v2 server at {:?}", socket_path);

    let stats = Arc::new(Mutex::new(ServerStats::default()));
    let listener = match UnixListener::bind(&socket_path) {
        Ok(listener) => listener,
        Err(err) if err.kind() == ErrorKind::PermissionDenied => {
            eprintln!(
                "Skipping ingest_v2 example: cannot bind socket at {:?}: {}",
                socket_path, err
            );
            return Ok(());
        }
        Err(err) => {
            return Err(anyhow!(
                "failed to bind v2 socket at {:?}: {err}",
                socket_path
            ));
        }
    };

    let stats_clone = stats.clone();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let reconstruct = std::env::var("INGEST_V2_RECONSTRUCT")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let server_task = tokio::spawn(run_server(listener, stats_clone, shutdown_rx));

    std::env::set_var("DBPATH", &socket_path);
    std::env::set_var("DB_PROTOCOL", "ingest_v2");
    std::env::set_var("DB_CHUNK_SIZE", "4000");
    std::env::set_var("DB_MAX_INFLIGHT", "16");
    // Use JSON/none in example server to simplify decoding
    std::env::set_var("DB_CODEC", "json");
    std::env::set_var("DB_COMPRESSION", "none");
    std::env::set_var("DB_SOURCE", "ingest-v2-example");

    println!("Running imessage-exporter through cargo...");
    run_imessage_exporter()?;

    let _ = shutdown_tx.send(());
    server_task.await??;

    let snapshot = stats.lock().unwrap().clone();
    println!(
        "Server received {} entries across {} sessions",
        snapshot.total_entries, snapshot.sessions
    );
    if let Some(entry) = snapshot.first_record {
        print_record("First entry", &entry, reconstruct);
    }
    if let Some(entry) = snapshot.last_record {
        print_record("Last entry", &entry, reconstruct);
    }
    if let Some(wm) = snapshot.watermark {
        println!("Final watermark: {wm}");
    }

    Ok(())
}

async fn run_server(
    listener: UnixListener,
    stats: Arc<Mutex<ServerStats>>,
    mut shutdown: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _)) => {
                        let stats_inner = stats.clone();
                        tokio::spawn(async move {
                            if let Err(err) = handle_connection(stream, stats_inner).await {
                                eprintln!("Ingest v2 connection error: {err:?}");
                            }
                        });
                    }
                    Err(err) => {
                        eprintln!("Failed to accept ingest connection: {err}");
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_connection(
    mut stream: tokio::net::UnixStream,
    stats: Arc<Mutex<ServerStats>>,
) -> anyhow::Result<()> {
    // Build typed init payload using ingest_models (advertise JSON/None for demo)
    let session_id = format!(
        "srv-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let job = IngestInit {
        version: 1,
        session_id: session_id.clone(),
        source_id: "imessage_v2".to_string(),
        transport: Some(ProtoTransportContext {
            codec: "json".to_string(),
            compression: "none".to_string(),
            chunk_size: 4000,
            max_inflight: 16,
        }),
        auth: Some(AuthContext {
            provider_id: String::new(),
            token_type: String::new(),
            access_token: String::new(),
            user_id: None,
            expires_at: None,
        }),
        watermark: Some(WatermarkContext {
            time_iso: None,
            token: None,
        }),
        job_type_url: String::new(),
        job_payload: Vec::new(),
        labels: Default::default(),
    };
    let init_wire = server::InitBuilder::new(session_id.clone(), "imessage_v2")
        .with_transport(TransportConfig { codec: Codec::Json, compression: Compression::None, max_inflight: 16, chunk_size: 4000 })
        .with_typed_job(&job)
        .finish();
    server::send_init_frame(&mut stream, &init_wire).await?;

    // Expect Hello
    let (opcode, codec, compression, payload) = read_frame(&mut stream).await?;
    if opcode != 1 {
        anyhow::bail!("expected Hello frame, got opcode {}", opcode);
    }
    // Decode wrapper message (JSON external tagging: {"hello": {...}})
    let msg: w::Message<IngestRecord> = decode_message(codec, compression, &payload)?;
    let w::Message::Hello(hello) = msg else { anyhow::bail!("expected Hello message payload") };
    if hello.total_batches == 0 {
        anyhow::bail!("hello must declare at least one batch");
    }
    let mut expected_next = 0usize;
    let mut accepted_entries = 0usize;
    {
        let mut guard = stats.lock().unwrap();
        guard.sessions += 1;
        guard.watermark = hello.watermark.clone();
    }
    // Ack Hello
    let ack = w::Ack {
        session_id: hello.session_id.clone(),
        status: w::AckStatus::Accepted,
        next_expected_batch: expected_next,
        accepted_entries,
        inflight: hello.max_inflight,
        watermark: hello.watermark.clone(),
    };
    write_ack(&mut stream, codec, compression, &ack).await?;

    loop {
        let (opcode, c, comp, payload) = read_frame(&mut stream).await?;
        match opcode {
            2 => {
                let msg: w::Message<IngestRecord> = decode_message(c, comp, &payload)?;
                let w::Message::HistoryBatch(batch) = msg else { anyhow::bail!("expected HistoryBatch message payload") };
                if batch.batch_index != expected_next {
                    anyhow::bail!(
                        "unexpected batch index {} (expected {})",
                        batch.batch_index, expected_next
                    );
                }
                accepted_entries += batch.entries.len();
                {
                    let mut guard = stats.lock().unwrap();
                    if guard.first_record.is_none() {
                        if let Some(first) = batch.entries.first() {
                            guard.first_record = Some(first.clone());
                        }
                    }
                    if let Some(last) = batch.entries.last() {
                        guard.last_record = Some(last.clone());
                    }
                    guard.total_entries += batch.entries.len();
                }

                expected_next += 1;
                let ack = w::Ack {
                    session_id: batch.session_id,
                    status: w::AckStatus::Accepted,
                    next_expected_batch: expected_next,
                    accepted_entries,
                    inflight: hello.max_inflight,
                    watermark: None,
                };
                write_ack(&mut stream, c, comp, &ack).await?;
            }
            3 => {
                let msg: w::Message<IngestRecord> = decode_message(c, comp, &payload)?;
                let w::Message::Complete(complete) = msg else { anyhow::bail!("expected Complete message payload") };
                if complete.final_entries != accepted_entries {
                    anyhow::bail!(
                        "final entries mismatch: got {}, expected {}",
                        complete.final_entries, accepted_entries
                    );
                }
                if complete.final_batches != expected_next {
                    anyhow::bail!(
                        "final batches mismatch: got {}, expected {}",
                        complete.final_batches, expected_next
                    );
                }
                let final_watermark = {
                    let mut guard = stats.lock().unwrap();
                    if complete.next_watermark.is_some() {
                        guard.watermark = complete.next_watermark.clone();
                    }
                    guard.watermark.clone()
                };
                let ack = w::Ack {
                    session_id: complete.session_id,
                    status: w::AckStatus::Completed,
                    next_expected_batch: expected_next,
                    accepted_entries,
                    inflight: hello.max_inflight,
                    watermark: final_watermark,
                };
                write_ack(&mut stream, c, comp, &ack).await?;
                break;
            }
            4 => {
                let msg: w::Message<IngestRecord> = decode_message(c, comp, &payload)?;
                let w::Message::Abort(abort) = msg else { anyhow::bail!("expected Abort message payload") };
                anyhow::bail!("client aborted: {}", abort.message);
            }
            other => anyhow::bail!("unexpected opcode {}", other),
        }
    }
    Ok(())
}

const FRAME_MAGIC: u32 = 0x4950_4752; // "IPGR"
const FRAME_HEADER_LEN: usize = 12;

async fn read_frame(
    stream: &mut tokio::net::UnixStream,
) -> anyhow::Result<(u16, Codec, Compression, Vec<u8>)> {
    use tokio::io::AsyncReadExt;
    let mut header = [0u8; FRAME_HEADER_LEN];
    stream.read_exact(&mut header).await?;
    let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
    if magic != FRAME_MAGIC {
        anyhow::bail!("invalid frame magic {:x}", magic);
    }
    let opcode = u16::from_be_bytes([header[4], header[5]]);
    let codec = match header[6] { 0 => Codec::Json, 1 => Codec::Binary, 2 => Codec::Protobuf, other => anyhow::bail!("unknown codec {}", other) };
    let compression = match header[7] { 0 => Compression::None, 1 => Compression::Zstd, other => anyhow::bail!("unknown compression {}", other) };
    let len = u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok((opcode, codec, compression, payload))
}

async fn write_ack(
    stream: &mut tokio::net::UnixStream,
    codec: Codec,
    compression: Compression,
    ack: &w::Ack,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    let payload = encode_payload(codec, compression, ack)?;
    let mut header = [0u8; FRAME_HEADER_LEN];
    header[0..4].copy_from_slice(&FRAME_MAGIC.to_be_bytes());
    header[4..6].copy_from_slice(&5u16.to_be_bytes()); // Ack opcode
    header[6] = codec as u8;
    header[7] = compression as u8;
    header[8..12].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    stream.write_all(&header).await?;
    stream.write_all(&payload).await?;
    Ok(())
}

// No custom write_init; server::send_init_frame handles Init framing

fn encode_payload(value_codec: Codec, value_compression: Compression, value: &impl serde::Serialize) -> anyhow::Result<Vec<u8>> {
    let bytes = match value_codec {
        Codec::Json => serde_json::to_vec(value)?,
        Codec::Binary => postcard::to_allocvec(value)?,
        Codec::Protobuf => anyhow::bail!("protobuf codec is not supported for this record type"),
    };
    let out = match value_compression {
        Compression::None => bytes,
        Compression::Zstd => zstd::stream::encode_all(bytes.as_slice(), 0)?,
    };
    Ok(out)
}

fn decode_payload<T: serde::de::DeserializeOwned>(payload_codec: Codec, payload_compression: Compression, data: &[u8]) -> anyhow::Result<T> {
    let decompressed = match payload_compression {
        Compression::None => data.to_vec(),
        Compression::Zstd => zstd::stream::decode_all(data)?,
    };
    let value = match payload_codec {
        Codec::Json => serde_json::from_slice(&decompressed)?,
        Codec::Binary => postcard::from_bytes(&decompressed)?,
        Codec::Protobuf => anyhow::bail!("protobuf codec is not supported for this record type"),
    };
    Ok(value)
}

fn decode_message<T: serde::de::DeserializeOwned>(codec: Codec, compression: Compression, data: &[u8]) -> anyhow::Result<w::Message<T>> {
    let decompressed = match compression {
        Compression::None => data.to_vec(),
        Compression::Zstd => zstd::stream::decode_all(data)?,
    };
    let msg = match codec {
        Codec::Json => serde_json::from_slice(&decompressed)?,
        Codec::Binary => postcard::from_bytes(&decompressed)?,
        Codec::Protobuf => anyhow::bail!("protobuf codec not supported in example server"),
    };
    Ok(msg)
}

fn run_imessage_exporter() -> anyhow::Result<()> {
    let status = Command::new("cargo")
        .args(["run", "--bin", "imessage-exporter", "--", "-f", "db"])
        .current_dir("../imessage-exporter")
        .status()?;
    println!("imessage-exporter completed with status: {status}");
    if !status.success() {
        anyhow::bail!("imessage-exporter failed");
    }
    Ok(())
}

fn print_record(label: &str, record: &IngestRecord, reconstruct: bool) {
    if reconstruct {
        let message: Message = record.clone().into();
        println!("{label} GUID={} text={:?}", message.guid, message.text);
    } else {
        println!("{label} GUID={} text={:?}", record.guid, record.text);
    }
}
