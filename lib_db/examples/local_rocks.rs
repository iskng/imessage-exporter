use dirs;
use serde::{Deserialize, Serialize};
use surrealdb::engine::local::RocksDb;
use surrealdb::opt::Config;
use surrealdb::Surreal;

#[derive(Debug, Serialize, Deserialize)]
struct Person {
    name: String,
    age: i32,
    city: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get the default cache directory path
    let default_path = dirs::cache_dir()
        .map(|cache_dir| cache_dir.join("lynx").join("path.db"))
        .unwrap_or_else(|| std::path::PathBuf::from("/export/db"));

    // Create the directory if it doesn't exist
    std::fs::create_dir_all(&default_path.parent().unwrap())?;

    println!("Using database path: {}", default_path.display());

    // Create a new database instance with configuration
    let db = Surreal::new::<RocksDb>((default_path, Config::default())).await?;

    // Select a namespace + database
    db.use_ns("test").use_db("test").await?;

    // Define the table and its schema
    db.query(
        "
        -- Define the people table
        DEFINE TABLE people SCHEMALESS;
        
        -- Define fields with types and constraints
        DEFINE FIELD name ON people TYPE string ASSERT $value != NONE AND $value != '';
        DEFINE FIELD age ON people TYPE number ASSERT $value > 0;
        DEFINE FIELD city ON people TYPE string ASSERT $value != NONE AND $value != '';
        
        -- Create an index on name
        DEFINE INDEX idx_people_name ON people FIELDS name UNIQUE;
        ",
    )
    .await?;

    println!("Table schema defined.");

    // Create some test data
    let test_people = vec![
        Person {
            name: "Alice".into(),
            age: 28,
            city: "New York".into(),
        },
        Person {
            name: "Bob".into(),
            age: 35,
            city: "San Francisco".into(),
        },
        Person {
            name: "Charlie".into(),
            age: 42,
            city: "Chicago".into(),
        },
    ];

    println!("\nInserting test data...");

    // Insert the test data
    for person in test_people {
        let created: Option<Person> = db.create(("people", &person.name)).content(person).await?;
        println!("Created: {:?}", created);
    }

    println!("\nQuerying all people...");

    // Query all people
    let people: Vec<Person> = db.select("people").await?;

    // Print results
    for person in &people {
        println!("Found: {:?}", person);
    }

    println!("\nQuerying people over 30...");

    // Query with a filter
    let older_people: Vec<Person> = db
        .query("SELECT * FROM people WHERE age > 30")
        .await?
        .take(0)?;

    // Print filtered results
    for person in &older_people {
        println!("Found person over 30: {:?}", person);
    }

    Ok(())
}
