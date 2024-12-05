use axum::{ routing::post, Router, Json, http::StatusCode, extract::State };
use rcgen::{ Certificate, CertificateParams, DistinguishedName, KeyPair };
use std::{ env, sync::Arc };
use tokio::sync::Mutex;
use axum_server::tls_rustls::RustlsConfig;
use rustls::crypto::CryptoProvider;
use serde_json::{ json, Value };
use std::net::TcpListener;

struct ServerState {
    messages: Vec<Value>,
}

fn find_available_port() -> Option<u16> {
    (6988..9000).find(|port| { TcpListener::bind(("127.0.0.1", *port)).is_ok() })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Install crypto provider
    let provider = rustls::crypto::ring::default_provider();
    CryptoProvider::install_default(provider).expect("Failed to install crypto provider");

    // Generate self-signed certificate
    let mut params = CertificateParams::default();
    params.distinguished_name = DistinguishedName::new();
    let key_pair = KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;

    // Create shared state
    let state = Arc::new(Mutex::new(ServerState { messages: Vec::new() }));

    // Create router
    let app = Router::new().route("/test", post(handle_message)).with_state(state.clone());

    // Configure TLS
    let config = RustlsConfig::from_pem(
        cert.pem().into_bytes(),
        key_pair.serialize_pem().into_bytes()
    ).await?;

    // Find available port
    let port = find_available_port().expect("No available ports found");
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    println!("Starting HTTPS server on {}", addr);

    // Start server in background
    let server_handle = tokio::spawn(async move {
        axum_server::bind_rustls(addr, config).serve(app.into_make_service()).await.unwrap();
    });

    // Wait for server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Test with client
    println!("Testing HTTPS connection...");
    let client = reqwest::Client::builder().danger_accept_invalid_certs(true).build()?;

    let test_message =
        json!({
        "id": 1,
        "content": "Hello TLS!",
        "timestamp": chrono::Utc::now().to_rfc3339()
    });

    let url = format!("https://localhost:{}/test", port);
    let response = client.post(&url).json(&test_message).send().await?;

    println!("Server response status: {}", response.status());
    println!("Server response body: {}", response.text().await?);

    // Print server state
    let state_lock = state.lock().await;
    println!("\nServer received {} messages:", state_lock.messages.len());
    for msg in &state_lock.messages {
        println!("{}", serde_json::to_string_pretty(msg)?);
    }

    // Clean up
    server_handle.abort();

    Ok(())
}

async fn handle_message(
    State(state): State<Arc<Mutex<ServerState>>>,
    Json(message): Json<Value>
) -> (StatusCode, Json<Value>) {
    let mut state = state.lock().await;
    state.messages.push(message.clone());

    (
        StatusCode::OK,
        Json(
            json!({
            "status": "success",
            "message": "Message received",
            "received": message
        })
        ),
    )
}
