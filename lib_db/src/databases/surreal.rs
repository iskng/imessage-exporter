use crate::{ Database, DatabaseConnection, types::Message, LYNX_MESSAGES_TABLE };

use surrealdb::opt::auth::Root;
use surrealdb::Surreal;
use std::sync::Arc;
use tokio::runtime::Runtime;
use surrealdb::sql::Thing;
use serde::{ Serialize, Deserialize };

use std::env;
use surrealdb::engine::any;
use surrealdb::engine::any::Any;

use dirs;

pub(crate) struct SurrealDatabase {
    db: Arc<Surreal<Any>>,
    connection: DatabaseConnection,
}

pub(crate) struct SurrealConfig {
    pub namespace: String,
    pub database: String,
    pub endpoint: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<Thing>,
    name: String,
    value: i32,
}

impl SurrealDatabase {
    pub(crate) async fn create(
        config: SurrealConfig,
        connection: DatabaseConnection
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // If ENDPOINT env var exists, use cache dir, otherwise use localhost
        let endpoint = if env::var("LOCAL").is_ok() {
            if let Some(cache_dir) = dirs::cache_dir() {
                let db_path = cache_dir.join("lynx").join("db");
                // Create the directory if it doesn't exist
                std::fs::create_dir_all(&db_path)?;
                format!("rocksdb:{}", db_path.to_string_lossy().to_string())
            } else {
                "ws://localhost:8000".to_owned()
            }
        } else {
            "ws://localhost:8000".to_owned()
        };

        // Create the Surreal instance. This will create `Surreal<Any>`.
        let db = any::connect(endpoint).await?;
        // Sign in as root user
        db.signin(Root {
            username: &config.username,
            password: &config.password,
        }).await?;

        // Select namespace and database
        db.use_ns(config.namespace).use_db(config.database).await?;

        let db = Arc::new(db);
        let instance = Self { db, connection };

        instance.setup_db()?;

        Ok(instance)
    }
}

impl Database for SurrealDatabase {
    fn insert_batch(
        &self,
        messages: Vec<Message>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let db = self.db.clone();

        let handle = std::thread::spawn(move || {
            let rt = Runtime::new()?;

            rt.block_on(async {
                // Process messages one at a time
                for message in messages {
                    // Create with explicit table and ID like in connect.rs example
                    let _: Option<Message> = db.create(LYNX_MESSAGES_TABLE).content(message).await?;
                }
                Ok(())
            })
        });
        handle.join().unwrap()
    }

    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let db = self.db.clone();
        let handle = std::thread::spawn(move || {
            let rt = Runtime::new()?;
            rt.block_on(async move {
                db.query("COMMIT TRANSACTION").await?;
                Ok(())
            })
        });
        handle.join().unwrap()
    }

    fn create_graph(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let db = self.db.clone();
        let handle = std::thread::spawn(move || {
            let rt = Runtime::new()?;
            rt.block_on(async move {
                db.query(
                    r#"
                   BEGIN TRANSACTION;

-- Retrieve all messages
LET $messages = (SELECT * FROM messages);

-- Iterate over each message to process persons, threads, and relationships
FOR $msg IN $messages {

    -- Create or retrieve person based on phone number and ownership
    LET $person = (SELECT * FROM persons WHERE phone_number = $msg.phone_number LIMIT 1)[0] ??
        (CREATE persons CONTENT { phone_number: $msg.phone_number, is_me: $msg.is_from_me });

    -- Create or retrieve thread based on unique chat ID
    LET $thread = (SELECT * FROM threads WHERE unique_chat_id = $msg.unique_chat_id LIMIT 1)[0] ??
        (CREATE threads CONTENT { unique_chat_id: $msg.unique_chat_id });

    -- Create relationships between person, message, and thread
    RELATE $person->messaged_in->$thread;
    RELATE $person->sent->$msg;
    RELATE $msg->in_thread->$thread;

};

COMMIT TRANSACTION;
                "#
                ).await?;

                Ok(())
            })
        });
        handle.join().unwrap()
    }

    fn setup_db(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let db = self.db.clone();
        let handle = std::thread::spawn(move || {
            let rt = Runtime::new()?;
            rt.block_on(async move {
                // Setup schema with uniqueness constraints
                db.query(
                    "
                    -- Define tables
                    DEFINE TABLE persons SCHEMALESS;
                    DEFINE TABLE threads SCHEMALESS;
                    DEFINE TABLE messages SCHEMALESS;
                    DEFINE TABLE messaged_in SCHEMALESS;
                    DEFINE TABLE sent SCHEMALESS;
                    DEFINE TABLE in_thread SCHEMALESS;

                    -- Define unique fields
                    DEFINE FIELD phone_number ON persons TYPE string ASSERT $value != NONE AND $value != '';
                    DEFINE INDEX person_phone ON persons FIELDS phone_number UNIQUE;

                    DEFINE FIELD unique_chat_id ON threads TYPE string ASSERT $value != NONE AND $value != '';
                    DEFINE INDEX thread_chat_id ON threads FIELDS unique_chat_id UNIQUE;
                    
                    "
                ).await?;

                Ok(())
            })
        });
        handle.join().unwrap()
    }
}
