pub mod databases;
pub mod proto;
mod types;

use std::sync::Arc;

use databases::{ingest_v2::IngestV2Database, socket::SocketDatabase};
use tokio::runtime::Runtime;
pub use types::Message;

#[derive(Debug, Clone)]
pub enum DatabaseType {
    Socket,
    IngestV2,
}

/// Public database interface implemented by the supported backends.
pub trait Database: Send + Sync {
    fn insert_batch(
        &self,
        messages: Vec<Message>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Shared runtime resources passed to the backend implementation.
pub struct DatabaseConnection {
    pub runtime: Arc<Runtime>,
    pub db_type: DatabaseType,
}

impl dyn Database {
    pub fn new(
        db_type: DatabaseType,
    ) -> Result<Box<dyn Database + Send + Sync>, Box<dyn std::error::Error + Send + Sync>> {
        let runtime = Arc::new(Runtime::new()?);
        let connection = DatabaseConnection {
            runtime: runtime.clone(),
            db_type: db_type.clone(),
        };

        match db_type {
            DatabaseType::Socket => {
                let db = runtime.block_on(async { SocketDatabase::create(connection).await })?;
                Ok(Box::new(db) as Box<dyn Database + Send + Sync>)
            }
            DatabaseType::IngestV2 => {
                let db = runtime.block_on(async { IngestV2Database::create(connection).await })?;
                Ok(Box::new(db) as Box<dyn Database + Send + Sync>)
            }
        }
    }
}
