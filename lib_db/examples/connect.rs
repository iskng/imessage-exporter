use surrealdb::engine::remote::ws::Ws;
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;
use serde::{ Deserialize, Serialize };
use surrealdb::sql::Thing;

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<Thing>,
    name: String,
    value: i32,
}

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
    // Connect to the server
    let db = Surreal::new::<Ws>("ws://localhost:8000").await?;

    // Sign in as a namespace, database, or root user
    db.signin(Root {
        username: "root",
        password: "root",
    }).await?;

    // Select a specific namespace / database
    db.use_ns("lynx").use_db("lynx").await?;
    let reco = Record {
        id: None,
        name: "Test Record".to_string(),
        value: 42,
    };
    // Create a new record
    let created: Option<Record> = db.create("test").content(reco).await?;
    println!("Created: {:?}", created);

    // Select all records
    let records: Vec<Record> = db.select("test").await?;
    println!("All Records: {:?}", records);

    // Perform a custom query
    let mut response = db.query("SELECT * FROM test WHERE value > $min").bind(("min", 30)).await?;
    let result: Vec<Record> = response.take(0)?;
    println!("Query Result: {:?}", result);

    Ok(())
}
