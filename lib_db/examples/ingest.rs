//! Ingest Protocol Example
//!
//! Mirrors the socket example but exercises the newline-delimited ingest
//! protocol. It starts a small async server, runs `imessage-exporter` with
//! `DB_PROTOCOL=ingest`, then prints a short summary of what the server saw.

use lib_db::{
    databases::ingest::wire::{
        Ack, AckStatus, GenericIngestWireRequest, GenericIngestWireResponse,
    },
    Message,
};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    process::Command,
    sync::{Arc, Mutex},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{UnixListener, UnixStream},
    sync::oneshot,
};

#[derive(Debug, Default, Clone)]
struct ServerStats {
    total_entries: usize,
    first_entry: Option<Message>,
    last_entry: Option<Message>,
    source: Option<String>,
    sessions: usize,
    sources: BTreeSet<String>,
}

#[derive(Debug)]
struct SessionInfo {
    session_id: String,
    expected_batches: usize,
    expected_entries: usize,
    received_batches: usize,
    received_entries: usize,
    first_entry: Option<Message>,
    last_entry: Option<Message>,
    source: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let socket_path = temp_dir.path().join("ingest.sock");

    let stats = Arc::new(Mutex::new(ServerStats::default()));
    println!("Starting ingest server at {:?}", socket_path);
    let (ready_tx, ready_rx) = oneshot::channel::<anyhow::Result<()>>();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_handle = tokio::spawn(run_server(
        socket_path.clone(),
        stats.clone(),
        ready_tx,
        shutdown_rx,
    ));

    ready_rx.await??;

    println!("Running ingest exporter through cargo...");
    std::env::set_var("DBPATH", &socket_path);
    std::env::set_var("DB_PROTOCOL", "ingest");
    std::env::set_var("DB_CHUNK_SIZE", "500");
    std::env::set_var("DB_SOURCE", "ingest-example");

    let exporter_status = run_imessage_exporter(false);
    let _ = shutdown_tx.send(());
    exporter_status?;

    server_handle.await??;

    let snapshot = stats.lock().unwrap().clone();
    println!(
        "Server received {} entries across {} sessions",
        snapshot.total_entries, snapshot.sessions
    );
    if let Some(entry) = snapshot.first_entry {
        println!("First entry GUID={} text={:?}", entry.guid, entry.text);
    }
    if let Some(entry) = snapshot.last_entry {
        println!("Last entry GUID={} text={:?}", entry.guid, entry.text);
    }
    if !snapshot.sources.is_empty() {
        println!(
            "Sources seen: {}",
            snapshot
                .sources
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    Ok(())
}

async fn run_server(
    socket_path: PathBuf,
    stats: Arc<Mutex<ServerStats>>,
    ready: oneshot::Sender<anyhow::Result<()>>,
    mut shutdown: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    if socket_path.exists() {
        std::fs::remove_file(&socket_path).ok();
    }

    let listener = match UnixListener::bind(&socket_path) {
        Ok(l) => {
            let _ = ready.send(Ok(()));
            l
        }
        Err(err) => {
            let err_msg = format!("failed to bind ingest socket at {:?}: {err}", socket_path);
            let err = anyhow::anyhow!(err_msg.clone());
            let _ = ready.send(Err(err));
            return Err(anyhow::anyhow!(err_msg));
        }
    };

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            accept_result = listener.accept() => {
                let (stream, _) = accept_result?;
                handle_connection(stream, stats.clone()).await?;
            }
        }
    }

    Ok(())
}

async fn handle_connection(
    stream: UnixStream,
    stats: Arc<Mutex<ServerStats>>,
) -> anyhow::Result<()> {
    let (reader, writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let mut writer = BufWriter::new(writer);
    let mut session: Option<SessionInfo> = None;

    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let request: GenericIngestWireRequest<Message> = serde_json::from_str(line)?;
        match request {
            GenericIngestWireRequest::Hello(hello) => {
                if hello.total_batches == 0 {
                    return Err(anyhow::anyhow!("hello declared zero batches"));
                }
                session = Some(SessionInfo {
                    session_id: hello.session_id.clone(),
                    expected_batches: hello.total_batches,
                    expected_entries: hello.total_entries,
                    received_batches: 0,
                    received_entries: 0,
                    first_entry: None,
                    last_entry: None,
                    source: hello.source.clone(),
                });
                write_response(
                    &mut writer,
                    GenericIngestWireResponse::Ack(Ack {
                        session_id: hello.session_id,
                        accepted_batches: 0,
                        accepted_entries: 0,
                        status: AckStatus::Accepted,
                    }),
                )
                .await?;
            }
            GenericIngestWireRequest::HistoryBatch(batch) => {
                let info = session
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("batch without session"))?;
                if batch.batch_index != info.received_batches {
                    return Err(anyhow::anyhow!(
                        "expected batch index {}, got {}",
                        info.received_batches,
                        batch.batch_index
                    ));
                }
                if batch.batch_count != info.expected_batches {
                    return Err(anyhow::anyhow!(
                        "expected batch count {}, got {}",
                        info.expected_batches,
                        batch.batch_count
                    ));
                }
                if info.received_entries + batch.entries.len() > info.expected_entries {
                    return Err(anyhow::anyhow!(
                        "incoming entries exceed expectation ({} > {})",
                        info.received_entries + batch.entries.len(),
                        info.expected_entries
                    ));
                }
                info.received_batches += 1;
                info.received_entries += batch.entries.len();
                if info.first_entry.is_none() {
                    if let Some(entry) = batch.entries.first() {
                        info.first_entry = Some(entry.clone());
                    }
                }
                if let Some(entry) = batch.entries.last() {
                    info.last_entry = Some(entry.clone());
                }

                write_response(
                    &mut writer,
                    GenericIngestWireResponse::Ack(Ack {
                        session_id: info.session_id.clone(),
                        accepted_batches: info.received_batches,
                        accepted_entries: info.received_entries,
                        status: AckStatus::Accepted,
                    }),
                )
                .await?;
            }
            GenericIngestWireRequest::Complete(complete) => {
                let info = session
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("complete without session"))?;
                if complete.final_batches != info.received_batches
                    || complete.final_entries != info.received_entries
                {
                    return Err(anyhow::anyhow!("session counts mismatch"));
                }

                let total = info.received_entries;
                let first_sample = info.first_entry.clone();
                let last_sample = info.last_entry.clone();
                let source = info.source.clone();
                {
                    let mut guard = stats.lock().unwrap();
                    guard.total_entries += total;
                    guard.sessions += 1;
                    if guard.first_entry.is_none() {
                        guard.first_entry = first_sample;
                    }
                    if let Some(last) = last_sample {
                        guard.last_entry = Some(last);
                    }
                    guard.source = Some(source.clone());
                    guard.sources.insert(source);
                }

                write_response(
                    &mut writer,
                    GenericIngestWireResponse::Ack(Ack {
                        session_id: complete.session_id,
                        accepted_batches: info.received_batches,
                        accepted_entries: info.received_entries,
                        status: AckStatus::Completed,
                    }),
                )
                .await?;
                break;
            }
            GenericIngestWireRequest::Abort(abort) => {
                write_response(&mut writer, GenericIngestWireResponse::Error(abort.clone()))
                    .await?;
                return Err(anyhow::anyhow!("client aborted: {}", abort.message));
            }
        }
    }

    Ok(())
}

async fn write_response(
    writer: &mut BufWriter<tokio::net::unix::OwnedWriteHalf>,
    resp: GenericIngestWireResponse,
) -> anyhow::Result<()> {
    let payload = serde_json::to_vec(&resp)?;
    writer.write_all(&payload).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

fn run_imessage_exporter(use_release: bool) -> anyhow::Result<std::process::ExitStatus> {
    let status = if use_release {
        println!("Running compiled imessage-exporter...");
        Command::new("/Users/user/dev/fork/imessage-exporter/target/release/imessage-exporter")
            .args(["-f", "db"])
            .status()?
    } else {
        println!("Running imessage-exporter through cargo...");
        Command::new("cargo")
            .args(["run", "--bin", "imessage-exporter", "--", "-f", "db"])
            .current_dir("../imessage-exporter")
            .status()?
    };

    println!("imessage-exporter completed with status: {}", status);
    Ok(status)
}
