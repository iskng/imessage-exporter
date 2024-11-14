use surrealdb::engine::any::Any;
use surrealdb::Surreal;
use serde::{ Serialize, Deserialize };
use std::sync::LazyLock;
use dirs;

// Static database connection
static DB: LazyLock<Surreal<Any>> = LazyLock::new(Surreal::init);

#[derive(Debug, Serialize, Deserialize)]
struct Person {
    name: String,
    age: i32,
    city: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get the default cache directory path
    let default_path = dirs
        ::cache_dir()
        .map(|cache_dir| cache_dir.join("lynx").join("pathoth"))
        .unwrap_or_else(|| std::path::PathBuf::from("/export/db"));

    // Create the directory if it doesn't exist
    std::fs::create_dir_all(&default_path)?;

    let endpoint = format!("rocksdb:{}", default_path.to_string_lossy());
    println!("Connecting to database at: {}", endpoint);

    // Connect to the database
    DB.connect(&endpoint).await?;

    // Select a namespace + database
    DB.use_ns("test").use_db("test").await?;

    // Define the table and its schema
    DB.query(
        "
        -- Define the people table
        DEFINE TABLE people SCHEMALESS;
        
        -- Define fields with types and constraints
        DEFINE FIELD name ON people TYPE string ASSERT $value != NONE AND $value != '';
        DEFINE FIELD age ON people TYPE number ASSERT $value > 0;
        DEFINE FIELD city ON people TYPE string ASSERT $value != NONE AND $value != '';
        
        -- Create an index on name
        DEFINE INDEX idx_people_name ON people FIELDS name UNIQUE;
        "
    ).await?;

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
        }
    ];

    println!("\nInserting test data...");

    // Insert the test data
    for person in test_people {
        let created: Option<Person> = DB.create(("people", &person.name)).content(person).await?;
        println!("Created: {:?}", created);
    }

    println!("\nQuerying all people...");

    // Query all people
    let people: Vec<Person> = DB.select("people").await?;

    // Print results
    for person in &people {
        println!("Found: {:?}", person);
    }

    println!("\nQuerying people over 30...");

    // Query with a filter
    let older_people: Vec<Person> = DB.query("SELECT * FROM people WHERE age > 30").await?.take(0)?;

    // Print filtered results
    for person in &older_people {
        println!("Found person over 30: {:?}", person);
    }

    Ok(())
}
