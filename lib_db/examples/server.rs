use lib_db::{ DatabaseType, Database, Message };
use rcgen::{ Certificate, CertificateParams, DistinguishedName, KeyPair };
use std::{ env, fs, path::PathBuf, net::SocketAddr, sync::Arc };
use tempfile::TempDir;
use tokio::sync::Mutex;
use axum::{ routing::post, Router, Json, extract::State, http::StatusCode };
use serde_json::json;
use axum_server::tls_rustls::RustlsConfig;
use std::process::Command;
use rustls::crypto::CryptoProvider;

// Store messages in memory until flush is called
struct ServerState {
    messages: Vec<Message>,
    total_received: usize,
    unique_guids: std::collections::HashSet<String>,
}

async fn run_server(
    port: u16,
    state: Arc<Mutex<ServerState>>
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = Router::new()
        .route("/ingest", post(handle_messages))
        .route("/flush", post(handle_flush))
        .with_state(state);

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    // Check for TLS configuration
    if let (Ok(cert_path), Ok(key_path)) = (env::var("TLS_CERT"), env::var("TLS_KEY")) {
        eprintln!("Starting HTTPS server with TLS certificate");
        let config = RustlsConfig::from_pem_file(
            PathBuf::from(cert_path),
            PathBuf::from(key_path)
        ).await?;

        axum_server::bind_rustls(addr, config).serve(app.into_make_service()).await?;
    } else {
        eprintln!("Starting HTTP server");
        axum::serve(tokio::net::TcpListener::bind(addr).await?, app.into_make_service()).await?;
    }

    Ok(())
}

async fn handle_messages(
    State(state): State<Arc<Mutex<ServerState>>>,
    Json(messages): Json<Vec<Message>>
) -> StatusCode {
    let mut state = state.lock().await;
    let batch_size = messages.len();

    // Check for duplicates
    for msg in &messages {
        if !state.unique_guids.insert(msg.guid.clone()) {
            eprintln!("WARNING: Duplicate message GUID: {}", msg.guid);
        }
    }

    state.total_received += batch_size;
    state.messages.extend(messages);

    StatusCode::OK
}

async fn handle_flush(State(state): State<Arc<Mutex<ServerState>>>) -> (
    StatusCode,
    Json<serde_json::Value>,
) {
    let mut state = state.lock().await;
    let message_count = state.messages.len();

    // Here you could process the messages (e.g., save to database)
    // For now, we just clear them
    state.messages.clear();

    // Return statistics
    (
        StatusCode::OK,
        Json(json!({
        "message_count": message_count,
        "status": "success"
    })),
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Install default crypto provider
    let provider = rustls::crypto::ring::default_provider();
    CryptoProvider::install_default(provider).expect("Failed to install crypto provider");

    // Generate self-signed certificate
    let mut params = CertificateParams::default();
    params.distinguished_name = DistinguishedName::new();

    let key_pair = KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;

    // Only write the certificate, keep private key in memory
    let cert_dir = TempDir::new()?;
    let cert_path = cert_dir.path().join("cert.pem");
    fs::write(&cert_path, cert.pem())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&cert_path, fs::Permissions::from_mode(0o600))?;
    }

    // Configure rustls with in-memory private key
    let config = RustlsConfig::from_pem(
        cert.pem().into_bytes(),
        key_pair.serialize_pem().into_bytes()
    ).await?;

    // Only share certificate path with client
    env::set_var("TLS_CERT", cert_path.to_str().unwrap());
    env::set_var("DBPATH", "https://localhost:3000");

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
    let server_handle = tokio::spawn(async move { run_server(3000, server_state).await });

    // Wait for server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Run imessage-exporter from release build
    println!("Running imessage-exporter...");
    let status = Command::new(
        "/Users/user/dev/fork/imessage-exporter/target/release/imessage-exporter"
    )
        .env("DBPATH", "https://localhost:3000")
        .env("TLS_CERT", cert_path.to_str().unwrap())
        .args(["-f", "db"])
        .status()?;

    println!("imessage-exporter completed with status: {}", status);

    // Print server stats
    let state_lock = state.lock().await;
    println!(
        "Server received {} total messages ({} unique)",
        state_lock.total_received,
        state_lock.unique_guids.len()
    );

    // Clean up
    server_handle.abort();
    cert_dir.close()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;
    use std::time::Duration;

    #[test]
    fn test_server() {
        let rt = Runtime::new().unwrap();

        // Create test state
        let state = Arc::new(
            Mutex::new(ServerState {
                messages: Vec::new(),
                total_received: 0,
                unique_guids: std::collections::HashSet::new(),
            })
        );
        let server_state = Arc::clone(&state);

        // Run server in background
        let server = rt.spawn(async move { run_server(3000, server_state).await });

        // Give server time to start
        std::thread::sleep(Duration::from_millis(100));

        // Test client requests
        rt.block_on(async {
            let client = reqwest::Client::new();

            // Test message insertion
            let messages = vec![Message::default()];
            let response = client
                .post("http://localhost:3000/ingest")
                .json(&messages)
                .send().await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);

            // Test flush
            let response = client.post("http://localhost:3000/flush").send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);

            let stats: serde_json::Value = response.json().await.unwrap();
            assert_eq!(stats["message_count"], 1);
        });
    }
}
