//! Ingest Protocol v2 Example
//!
//! Starts a binary-framed ingest v2 server, runs `imessage-exporter`
//! (`DB_PROTOCOL=ingest_v2`), and prints stats similar to other demos.

use anyhow::anyhow;
use ingest_protocol::v2::{
    server, Codec, Compression, GenericIngestArgs, GenericIngestState, TransportConfig, VecAccumulator,
};
use ingest_models::{AuthContext, IngestInit, TransportContext as ProtoTransportContext, WatermarkContext};
use lib_db::proto::ImessageRecord;
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
    first_record: Option<ImessageRecord>,
    last_record: Option<ImessageRecord>,
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
    // Encourage exporter to use protobuf + zstd for performance
    std::env::set_var("DB_CODEC", "protobuf");
    std::env::set_var("DB_COMPRESSION", "zstd");
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
    stream: tokio::net::UnixStream,
    stats: Arc<Mutex<ServerStats>>,
) -> anyhow::Result<()> {
    // Build typed init payload using ingest_models and advertise Protobuf+Zstd
    let session_id = format!(
        "srv-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );
    let job = IngestInit {
        version: 1,
        session_id: session_id.clone(),
        source_id: "imessage_v2".to_string(),
        transport: Some(ProtoTransportContext { codec: "protobuf".into(), compression: "zstd".into(), chunk_size: 4000, max_inflight: 16 }),
        auth: Some(AuthContext { provider_id: String::new(), token_type: String::new(), access_token: String::new(), user_id: None, expires_at: None }),
        watermark: Some(WatermarkContext { time: None, token: None }),
        job: None,
        labels: Default::default(),
    };
    let init = server::InitBuilder::new(session_id, "imessage_v2")
        .with_transport(TransportConfig { codec: Codec::Protobuf, compression: Compression::Zstd, max_inflight: 16, chunk_size: 4000 })
        .with_typed_job(&job)
        .finish();

    // Use generic ingest state (works with protobuf)
    let args: GenericIngestArgs<ImessageRecord> = GenericIngestArgs::new("imessage_v2".into());
    let state: GenericIngestState<ImessageRecord, VecAccumulator<ImessageRecord>> = GenericIngestState::from_args(args);
    server::handle_single_connection_with_init(stream, init, state, move |session| {
        let stats = stats.clone();
        async move {
            let mut guard = stats.lock().unwrap();
            guard.sessions += 1;
            guard.total_entries += session.total_entries;
            if guard.first_record.is_none() {
                if let Some(first) = session.entries.first() { guard.first_record = Some(first.clone()); }
            }
            if let Some(last) = session.entries.last() { guard.last_record = Some(last.clone()); }
            guard.watermark = session
                .watermark_token
                .clone()
                .or_else(|| session.watermark_time.map(|dt| dt.to_rfc3339()));
            Ok(())
        }
    })
    .await
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

fn print_record(label: &str, record: &ImessageRecord, _reconstruct: bool) {
    println!("{label} GUID={} text={:?}", record.guid, record.text);
}
