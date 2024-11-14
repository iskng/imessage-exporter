use crate::{
    Database,
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

static DB: LazyLock<Surreal<Any>> = LazyLock::new(Surreal::init);

pub(crate) struct SurrealDatabase;

impl SurrealDatabase {
    pub(crate) async fn create() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (username, password) = match (env::var("DBUSER"), env::var("DBPASS")) {
            (Ok(user), Ok(pass)) => (user, pass),
            _ => (DEFAULT_DB_USERNAME.to_string(), DEFAULT_DB_PASSWORD.to_string()),
        };

        let root = Root {
            username: &username,
            password: &password,
        };

        let default_path = dirs
            ::cache_dir()
            .map(|cache_dir| cache_dir.join("export").join("db"))
            .unwrap_or_else(|| std::path::PathBuf::from("/export/db"));

        let (endpoint, is_websocket) = match env::var("DBPATH") {
            Ok(path) => {
                if path == "1" {
                    if let Err(_) = std::fs::create_dir_all(&default_path) {
                        (FALLBACK_DB_ENDPOINT.to_string(), true)
                    } else {
                        (format!("rocksdb:{}", default_path.to_string_lossy()), false)
                    }
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

        DB.connect(&endpoint).await?;

        if is_websocket {
            DB.signin(root).await?;
        }

        DB.use_ns(LYNX_NAMESPACE).use_db(LYNX_DATABASE).await?;

        let instance = Self;
        instance.setup_db()?;

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

    fn create_graph(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let handle = std::thread::spawn(move || {
            let rt = Runtime::new()?;
            rt.block_on(async move {
                DB.query(
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
        let handle = std::thread::spawn(move || {
            let rt = Runtime::new()?;
            rt.block_on(async move {
                // Setup schema with uniqueness constraints
                DB.query(
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
