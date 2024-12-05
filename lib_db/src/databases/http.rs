use crate::{ Database, DatabaseConnection, types::Message };
use reqwest::{ Client, ClientBuilder, Certificate };
use std::{ env, fs, error::Error as StdError };
use tokio::runtime::Runtime;
use url::Url;

const DEFAULT_HTTPS_ENDPOINT: &str = "https://localhost:3000";

pub(crate) struct HttpDatabase {
    connection: DatabaseConnection,
    client: Client,
    base_url: String,
}

impl HttpDatabase {
    pub(crate) async fn create(
        connection: DatabaseConnection
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut client = ClientBuilder::new().danger_accept_invalid_certs(true).build()?;

        // Determine base URL from DBPATH or fallback
        let base_url = if let Ok(path) = env::var("DBPATH") {
            eprintln!("Exporter sees DBPATH: {}", path);
            if let Ok(url) = Url::parse(&path) {
                if url.scheme() == "https" { path } else { DEFAULT_HTTPS_ENDPOINT.to_string() }
            } else {
                DEFAULT_HTTPS_ENDPOINT.to_string()
            }
        } else {
            return Err(format!("Invalid URL scheme").into());
        };

        eprintln!("Using HTTP API endpoint: {}", base_url);

        Ok(Self {
            connection,
            client,
            base_url,
        })
    }

    fn validate_response(
        response: reqwest::Response,
        context: &str
    ) -> Result<reqwest::Response, Box<dyn std::error::Error + Send + Sync>> {
        let status = response.status();
        if !status.is_success() {
            return Err(format!("Failed to {}. Status: {}", context, status).into());
        }
        Ok(response)
    }
}

impl Database for HttpDatabase {
    fn insert_batch(
        &self,
        messages: Vec<Message>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let batch_size = messages.len();

        let client = self.client.clone();
        let base_url = self.base_url.clone();

        let handle = std::thread::spawn(move || {
            let rt = Runtime::new()?;

            rt.block_on(async {
                let response = match client.post(&base_url).json(&messages).send().await {
                    Ok(resp) => resp,
                    Err(e) => {
                        eprintln!("HTTP request failed: {}", e);
                        if let Some(source) = e.source() {
                            eprintln!("Caused by: {}", source);
                        }
                        return Err(e.into());
                    }
                };

                if !response.status().is_success() {
                    let status = response.status();
                    let error_text = response.text().await?;
                    eprintln!("Server returned error: {}", error_text);
                    return Err(format!("HTTP error {}: {}", status, error_text).into());
                }

                Ok(())
            })
        });

        handle.join().unwrap()
    }

    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}
