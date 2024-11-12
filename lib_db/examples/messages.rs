use serde::{ Deserialize, Serialize };
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::sql::Value;
use tokio;

#[derive(Serialize, Deserialize)]
struct Message {
    id: Option<String>,
    content: String,
    timestamp: i64,
    // Add other fields as needed
}

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
    // Initialize the database connection
    let db = Surreal::new::<Ws>("localhost:8000").await?;

    // Sign in as a namespace, database, or root user
    db.signin(Root {
        username: "root",
        password: "roots",
    }).await?;

    // Select a specific namespace / database
    db.use_ns("lynx").use_db("lynx").await?;
    // Example Vec<Message> to insert
    let messages = vec![
        Message {
            id: None,
            content: "Hello, world!".to_string(),
            timestamp: 1636414800,
        },
        Message {
            id: None,
            content: "Another message".to_string(),
            timestamp: 1636414860,
        }
        // Add more messages as needed
    ];

    // Insert each message
    for message in messages {
        let _response: Value = db.create("messages").content(message).await?.expect("REASON"); // Pass the `Message` struct directly
    }

    println!("Messages inserted successfully");
    Ok(())
}
