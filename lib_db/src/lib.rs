mod databases;
mod types;

use databases::surreal::SurrealDatabase;
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
}

pub trait Database: Send + Sync {
    fn insert_batch(
        &self,
        messages: Vec<Message>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn create_graph(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn setup_db(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

impl dyn Database {
    pub fn new(
        db_type: DatabaseType
    ) -> Result<Box<dyn Database + Send + Sync>, Box<dyn std::error::Error + Send + Sync>> {
        match db_type {
            DatabaseType::Surreal => {
                let db = tokio::runtime::Runtime
                    ::new()?
                    .block_on(async { SurrealDatabase::create().await })?;
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

    // Define static strings for test data
    const TEST_DATE: &'static str = "2024-01-01";
    const TEST_SERVICE: &'static str = "iMessage";

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
            service: Some(TEST_SERVICE.to_string()),
            handle_id: Some(i as i32), // Unique handle_id
            destination_caller_id: None,
            subject: None,
            date: TEST_DATE.to_string(),
            date_read: TEST_DATE.to_string(),
            date_delivered: TEST_DATE.to_string(),
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
            date_edited: TEST_DATE.to_string(),
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
        db.create_graph().expect("Failed to create graph");
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
