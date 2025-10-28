//! Ingest Protocol v2 Example
//!
//! Starts a binary-framed ingest v2 server, runs `imessage-exporter`
//! (`DB_PROTOCOL=ingest_v2`), and prints stats similar to other demos.

use anyhow::anyhow;
use ingest_protocol::v2::{
    accumulators::{StreamingAccumulator, StreamingHandler},
    server, GenericIngestArgs, GenericIngestState,
};
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

struct StatsHandler {
    stats: Arc<Mutex<ServerStats>>,
}

impl StreamingHandler<IngestRecord> for StatsHandler {
    fn handle_batch(&self, entries: Vec<IngestRecord>) -> anyhow::Result<()> {
        let mut guard = self.stats.lock().unwrap();
        if guard.first_record.is_none() {
            if let Some(first) = entries.first() {
                guard.first_record = Some(first.clone());
            }
        }
        if let Some(last) = entries.last() {
            guard.last_record = Some(last.clone());
        }
        guard.total_entries += entries.len();
        Ok(())
    }
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
    std::env::set_var("DB_CODEC", "binary");
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
    let handler = Arc::new(StatsHandler {
        stats: stats.clone(),
    });
    let factory = StreamingAccumulator::factory(handler);
    let args = GenericIngestArgs::new("imessage_v2".into());
    let state = GenericIngestState::<
        IngestRecord,
        StreamingAccumulator<IngestRecord, StatsHandler>,
    >::with_factory(args, factory);
    server::handle_single_connection_core(stream, state, move |session| {
        let stats = stats.clone();
        async move {
            let mut guard = stats.lock().unwrap();
            guard.sessions += 1;
            guard.watermark = session.watermark.clone();
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

fn print_record(label: &str, record: &IngestRecord, reconstruct: bool) {
    if reconstruct {
        let message: Message = record.clone().into();
        println!("{label} GUID={} text={:?}", message.guid, message.text);
    } else {
        println!("{label} GUID={} text={:?}", record.guid, record.text);
    }
}
