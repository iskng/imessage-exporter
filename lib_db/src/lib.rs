mod databases;
mod types;

use std::sync::Arc;
use chrono::{ DateTime, TimeZone, Utc };

use databases::surreal::SurrealDatabase;
use tokio::runtime::Runtime;
pub use types::Message;

pub const LYNX_NAMESPACE: &str = "lynx";
pub const LYNX_DATABASE: &str = "lynx";
pub const LYNX_TABLE_CHUNKS: &str = "chunks";
pub const LYNX_MESSAGES_TABLE: &str = "messages";
pub const LYNX_PERSONS_TABLE: &str = "persons";
pub const LYNX_THREADS_TABLE: &str = "threads";
pub const DEFAULT_DB_USERNAME: &str = "root";
pub const DEFAULT_DB_PASSWORD: &str = "root";
pub const FALLBACK_DB_ENDPOINT: &str = "ws://localhost:8000";

#[derive(Debug, Clone)]
pub enum DatabaseType {
    Surreal,
    // Add other database types here
}

// Public database interface
pub trait Database: Send + Sync {
    fn insert_batch(
        &self,
        messages: Vec<Message>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

// Add this struct to hold shared resources
pub struct DatabaseConnection {
    pub runtime: Arc<Runtime>,
    pub db_type: DatabaseType,
}

impl dyn Database {
    pub fn new(
        db_type: DatabaseType
    ) -> Result<Box<dyn Database + Send + Sync>, Box<dyn std::error::Error + Send + Sync>> {
        // Create the runtime once at the top level
        let runtime = Arc::new(Runtime::new()?);

        let connection = DatabaseConnection {
            runtime: runtime.clone(),
            db_type,
        };

        match db_type {
            DatabaseType::Surreal => {
                // Use the shared runtime instead of creating a new one
                let db = runtime.block_on(async { SurrealDatabase::create(connection).await })?;

                Ok(Box::new(db) as Box<dyn Database + Send + Sync>)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use std::thread;
    use std::sync::Arc;
    use chrono::{ DateTime, TimeZone, Utc };

    // Define static strings for test data
    const TEST_DATE: &'static str = "2024-01-01T00:00:00Z";

    // Helper function to parse test date
    fn get_test_datetime() -> Option<DateTime<Utc>> {
        Some(Utc.datetime_from_str(TEST_DATE, "%Y-%m-%dT%H:%M:%SZ").unwrap())
    }

    // Define the test message once
    fn get_test_message(i: usize) -> Message {
        // Create unique identifiers
        let phone_number = format!("+1{:010}", i); // Creates unique phone numbers like +10000000001
        let chat_id = Some(1000 + (i as i32)); // Creates unique chat IDs starting from 1000
        let unique_chat_id = format!("chat-{}", i); // Creates unique chat identifiers

        Message {
            id: None,
            rowid: i as i32,
            guid: format!("test-guid-{}", i),
            text: Some(format!("Test message {}", i)),
            service: Some("iMessage".to_string()),
            handle_id: Some(i as i32), // Unique handle_id
            destination_caller_id: None,
            subject: None,
            date: get_test_datetime(),
            date_read: get_test_datetime(),
            date_delivered: get_test_datetime(),
            is_from_me: i % 2 == 0,
            is_read: true,
            item_type: 0,
            other_handle: 0,
            share_status: false,
            share_direction: false,
            group_title: None,
            group_action_type: 0,
            associated_message_guid: None,
            associated_message_type: None,
            balloon_bundle_id: None,
            expressive_send_style_id: None,
            thread_originator_guid: None,
            thread_originator_part: None,
            date_edited: get_test_datetime(),
            associated_message_emoji: None,
            chat_id,
            unique_chat_id,
            num_attachments: 0,
            deleted_from: None,
            num_replies: 0,
            full_message: format!("Test message {}", i),
            thread_name: None,
            attachment_paths: Vec::new(),
            is_deleted: false,
            is_edited: false,
            is_reply: false,
            phone_number,
        }
    }

    #[test]
    fn test_database_operations() {
        // Create database
        let db = <dyn Database>::new(DatabaseType::Surreal).expect("Failed to create database");

        // Create and insert messages
        let messages = (0..10).map(get_test_message).collect::<Vec<_>>();
        db.insert_batch(messages).expect("Failed to insert messages");

        thread::sleep(Duration::from_millis(100));
        // Commit any pending transactions
        db.flush().expect("Failed to flush changes");
    }

    #[test]
    fn test_large_batch() {
        // Create database
        let db = <dyn Database>::new(DatabaseType::Surreal).expect("Failed to create database");

        // Create and insert messages
        let messages = (0..1000).map(get_test_message).collect::<Vec<_>>();
        db.insert_batch(messages).expect("Failed to insert large batch");
    }

    #[test]
    fn test_concurrent_operations() {
        let db = Arc::new(
            <dyn Database>::new(DatabaseType::Surreal).expect("Failed to create database")
        );

        let mut handles = vec![];
        for i in 0..5 {
            let db = Arc::clone(&db);
            let handle = thread::spawn(move || {
                let messages = vec![get_test_message(i)];
                db.insert_batch(messages).expect("Failed to insert from thread");
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        db.flush().expect("Failed to flush changes");
    }
}
