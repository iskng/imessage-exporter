use crate::{
    Database,
    DatabaseConnection,
    types::Message,
    LYNX_MESSAGES_TABLE,
    FALLBACK_DB_ENDPOINT,
    LYNX_NAMESPACE,
    LYNX_DATABASE,
    DEFAULT_DB_USERNAME,
    DEFAULT_DB_PASSWORD,
};

use surrealdb::opt::auth::Root;
use surrealdb::Surreal;
use tokio::runtime::Runtime;
use std::env;
use surrealdb::engine::any::Any;
use dirs;
use std::sync::LazyLock;

// Static database connection
static DB: LazyLock<Surreal<Any>> = LazyLock::new(Surreal::init);

pub(crate) struct SurrealDatabase {
    connection: DatabaseConnection,
}

#[derive(Debug, serde::Deserialize)]
struct ThreadCount {
    thread_count: i64,
}

#[derive(Debug, serde::Deserialize)]
struct PersonCount {
    person_count: i64,
}

#[derive(Debug, serde::Deserialize)]
struct InThreadCount {
    in_thread_count: i64,
}

#[derive(Debug, serde::Deserialize)]
struct SentCount {
    sent_count: i64,
}

#[derive(Debug, serde::Deserialize)]
struct MessageCount {
    message_count: i64,
}

#[derive(Debug, serde::Deserialize)]
struct MessagedInCount {
    messaged_in_count: i64,
}

impl SurrealDatabase {
    pub(crate) async fn create(
        connection: DatabaseConnection
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Get username and password from env vars if both are set
        let (username, password) = match (env::var("DBUSER"), env::var("DBPASS")) {
            (Ok(user), Ok(pass)) => (user, pass),
            _ => (DEFAULT_DB_USERNAME.to_string(), DEFAULT_DB_PASSWORD.to_string()),
        };

        let root = Root {
            username: &username,
            password: &password,
        };

        // Get the default cache directory path
        let default_path = dirs
            ::cache_dir()
            .map(|cache_dir| cache_dir.join("export").join("db"))
            .unwrap_or_else(|| std::path::PathBuf::from("/export/db"));

        // Determine endpoint based on DBPATH environment variable
        let (endpoint, is_websocket) = match env::var("DBPATH") {
            Ok(path) => {
                if path == "1" {
                    if let Err(_) = std::fs::create_dir_all(&default_path) {
                        (FALLBACK_DB_ENDPOINT.to_string(), true)
                    } else {
                        (format!("rocksdb:{}", default_path.to_string_lossy()), false)
                    }
                } else if path == "remote" {
                    (FALLBACK_DB_ENDPOINT.to_string(), true)
                } else {
                    let path = std::path::PathBuf::from(&path);
                    if path.is_absolute() || path.components().count() > 0 {
                        if let Err(_) = std::fs::create_dir_all(&path) {
                            (FALLBACK_DB_ENDPOINT.to_string(), true)
                        } else {
                            (format!("rocksdb:{}", path.to_string_lossy()), false)
                        }
                    } else {
                        (FALLBACK_DB_ENDPOINT.to_string(), true)
                    }
                }
            }
            Err(_) => {
                if let Err(_) = std::fs::create_dir_all(&default_path) {
                    (FALLBACK_DB_ENDPOINT.to_string(), true)
                } else {
                    (format!("rocksdb:{}", default_path.to_string_lossy()), false)
                }
            }
        };

        eprintln!("Using database path: {}", endpoint);

        // Connect using the static DB instance
        DB.connect(&endpoint).await?;

        // Only sign in if using WebSocket connection
        if is_websocket {
            DB.signin(root).await?;
        }

        // Select namespace and database
        DB.use_ns(LYNX_NAMESPACE).use_db(LYNX_DATABASE).await?;

        let instance = Self { connection };

        Ok(instance)
    }
}

impl Database for SurrealDatabase {
    fn insert_batch(
        &self,
        messages: Vec<Message>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let handle = std::thread::spawn(move || {
            let rt = Runtime::new()?;

            rt.block_on(async {
                for message in messages {
                    let _: Option<Message> = DB.create(LYNX_MESSAGES_TABLE).content(message).await?;
                }
                Ok(())
            })
        });
        handle.join().unwrap()
    }

    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let handle = std::thread::spawn(move || {
            let rt = Runtime::new()?;
            rt.block_on(async move {
                DB.query("COMMIT TRANSACTION").await?;
                Ok(())
            })
        });
        handle.join().unwrap()
    }
}
