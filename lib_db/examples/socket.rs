//! Unix Socket Server Example
//!
//! This example demonstrates a Unix domain socket server that receives iMessage data
//! using a simple binary protocol. The server accepts a single client connection and
//! processes messages until the client disconnects.
//!
//! Protocol Specification:
//! ----------------------
//! The protocol is a simple binary format designed for efficiency:
//!
//! Commands:
//! - 'I' (0x49): Insert messages
//!   Format: <I><length:u32><json_data>
//!   Response: 'K' for success, 'E' for error
//!
//!   The length is a 32-bit unsigned integer in big-endian format,
//!   followed by exactly that many bytes of JSON-encoded message data.
//!
//! - 'F' (0x46): Flush/commit messages
//!   Format: <F>
//!   Response: 'K' for success, 'E' for error
//!
//! Response Codes:
//! - 'K' (0x4B): Success/OK
//! - 'E' (0x45): Error
//!
//! Example Message Flow:
//! 1. Client connects to Unix socket
//! 2. Client sends: <I><00 00 20 00><json_data_8192_bytes>
//! 3. Server responds: <K>
//! 4. Client sends: <F>
//! 5. Server responds: <K>
//! 6. Client disconnects
//!
//! Security:
//! - Unix socket permissions restrict access to the current user
//! - Single client connection model prevents interference
//! - Length-prefixed messages prevent buffer overflow
//! - Local-only communication ensures data privacy
//!
//! Performance Considerations:
//! - Pre-allocated buffers reduce memory allocation
//! - Length-prefixed messages allow exact buffer sizing
//! - Single connection reduces overhead
//! - Binary protocol minimizes parsing
//!
//! Usage:
//! 1. Start this server example
//! 2. Run binary with DBPATH set to the socket path
//! 3. Server processes messages and maintains counts
//! 4. Clean shutdown on client disconnect
//!
//! This implementation is designed for testing the socket-based
//! database implementation in lib_db.
//!
use lib_db::{ DatabaseType, Database, Message };
use std::{ env, path::PathBuf, sync::Arc };
use tempfile::TempDir;
use tokio::{ sync::Mutex, net::{ UnixListener, UnixStream }, io::{ AsyncWriteExt, AsyncReadExt } };
use serde_json::{ json, Value };
use std::process::Command;

struct ServerState {
    messages: Vec<Message>,
    total_received: usize,
    unique_guids: std::collections::HashSet<String>,
}

async fn run_server(
    socket_path: PathBuf,
    state: Arc<Mutex<ServerState>>
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Starting Unix socket server at {:?}", socket_path);
    let listener = UnixListener::bind(&socket_path)?;

    if let Ok((mut socket, _)) = listener.accept().await {
        println!("Client connected");
        let mut cmd = [0u8; 1];
        let mut len_buf = [0u8; 4];

        while let Ok(_) = socket.read_exact(&mut cmd).await {
            match cmd[0] {
                b'I' => {
                    // Read length prefix
                    socket.read_exact(&mut len_buf).await?;
                    let len = u32::from_be_bytes(len_buf) as usize;

                    // Read exact message length
                    let mut buffer = vec![0u8; len];
                    socket.read_exact(&mut buffer).await?;

                    match serde_json::from_slice::<Vec<Message>>(&buffer) {
                        Ok(messages) => {
                            let mut state = state.lock().await;
                            let count = messages.len();
                            state.total_received += count;
                            state.messages.extend(messages);
                            socket.write_all(&[b'K']).await?;
                        }
                        Err(e) => {
                            eprintln!("Parse error: {}", e);
                            socket.write_all(&[b'E']).await?;
                        }
                    }
                }
                b'F' => {
                    socket.write_all(&[b'K']).await?;
                }
                _ => socket.write_all(&[b'E']).await?,
            }
            socket.flush().await?;
        }
        println!("Client disconnected");
    }
    Ok(())
}

fn run_imessage_exporter(
    dbpath: &str,
    use_release: bool
) -> Result<std::process::ExitStatus, Box<dyn std::error::Error + Send + Sync>> {
    let status = if use_release {
        println!("Running compiled imessage-exporter...");
        Command::new("/Users/user/dev/fork/imessage-exporter/target/release/imessage-exporter")
            .args(["-f", "db"])
            .env("DBPATH", dbpath)
            .status()?
    } else {
        println!("Running imessage-exporter through cargo...");
        Command::new("cargo")
            .args(["run", "--bin", "imessage-exporter", "--", "-f", "db"])
            .env("DBPATH", dbpath)
            .current_dir("../imessage-exporter")
            .status()?
    };

    println!("imessage-exporter completed with status: {}", status);
    Ok(status)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create temporary directory for socket
    let temp_dir = TempDir::new()?;
    let socket_path = temp_dir.path().join("imessage.sock");

    // Set environment variable for client
    env::set_var("DBPATH", socket_path.to_str().unwrap());

    // Create shared state
    let state = Arc::new(
        Mutex::new(ServerState {
            messages: Vec::new(),
            total_received: 0,
            unique_guids: std::collections::HashSet::new(),
        })
    );
    let server_state = Arc::clone(&state);

    // Start server in background
    let server_handle = tokio::spawn(async move { run_server(socket_path, server_state).await });

    // Wait for server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Run imessage-exporter through cargo
    run_imessage_exporter(&env::var("DBPATH").unwrap(), false)?;

    // Print server stats
    let state_lock = state.lock().await;
    println!(
        "Server received {} total messages ({} unique)",
        state_lock.total_received,
        state_lock.unique_guids.len()
    );

    // Print last message
    if let Some(last_msg) = state_lock.messages.last() {
        println!("\nLast message received:");
        println!("GUID: {}", last_msg.guid);
        println!("Text: {}", last_msg.text.as_deref().unwrap_or("<no text>"));
        println!("From: {}", if last_msg.is_from_me { "Me" } else { &last_msg.phone_number });
        if let Some(date) = last_msg.date {
            println!("Date: {}", date.to_rfc3339());
        }
    }

    // Clean up
    server_handle.abort();
    temp_dir.close()?;

    Ok(())
}
