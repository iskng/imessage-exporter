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

    let endpoint = format!("rocksdb:/{}", default_path.to_string_lossy());
    println!("Connecting to database at: {}", endpoint);

    // Connect to the database
    DB.connect(&endpoint).await?;

    // Select the same namespace + database as in local.rs
    DB.use_ns("test").use_db("test").await?;

    println!("\nQuerying all people...");

    // Query all people
    let people: Vec<Person> = DB.select("people").await?;

    if people.is_empty() {
        println!("No people found in database. Did you run the local.rs example first?");
    } else {
        // Print results
        println!("Found {} people:", people.len());
        for person in &people {
            println!("Found: {:?}", person);
        }
    }

    println!("\nQuerying people over 30...");

    // Query with a filter
    let older_people: Vec<Person> = DB.query("SELECT * FROM people WHERE age > 30").await?.take(0)?;

    if older_people.is_empty() {
        println!("No people over 30 found in database.");
    } else {
        // Print filtered results
        println!("Found {} people over 30:", older_people.len());
        for person in &older_people {
            println!("Found person over 30: {:?}", person);
        }
    }

    // Additional query to show table info
    // println!("\nChecking database tables...");
    // let tables: Vec<String> = DB.query("INFO FOR DB").await?.take(0)?;
    // println!("Database tables: {:?}", tables);

    Ok(())
}
