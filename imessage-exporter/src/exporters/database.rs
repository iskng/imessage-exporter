use crate::app::error::RuntimeError;
use std::collections::HashMap;

pub trait DatabaseExporter {
    fn insert_messages(&self, messages: Vec<DbMessage>) -> Result<(), RuntimeError>;
    fn flush(&self) -> Result<(), RuntimeError>;
}

pub struct SurrealDBExporter {
    // Add SurrealDB-specific fields
}

impl DatabaseExporter for SurrealDBExporter {
    fn insert_messages(&self, messages: Vec<DbMessage>) -> Result<(), RuntimeError> {
        // Implement SurrealDB-specific insertion
        Ok(())
    }

    fn flush(&self) -> Result<(), RuntimeError> {
        // Implement SurrealDB-specific flush
        Ok(())
    }
}

/// A generic message type that can be converted to various database formats
#[derive(Debug, Clone)]
pub struct DbMessage {
    pub fields: HashMap<String, DbValue>,
}

#[derive(Debug, Clone)]
pub enum DbValue {
    Integer(i64),
    Text(String),
    Boolean(bool),
    Float(f64),
    Array(Vec<DbValue>),
    Null,
    // Add other types as needed
}

impl DbMessage {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    pub fn insert<K: Into<String>, V: Into<DbValue>>(&mut self, key: K, value: V) {
        self.fields.insert(key.into(), value.into());
    }
}

// Implement conversions for common types
impl From<i64> for DbValue {
    fn from(v: i64) -> Self {
        DbValue::Integer(v)
    }
}

impl From<i32> for DbValue {
    fn from(v: i32) -> Self {
        DbValue::Integer(v as i64)
    }
}

impl From<String> for DbValue {
    fn from(v: String) -> Self {
        DbValue::Text(v)
    }
}

impl From<&str> for DbValue {
    fn from(v: &str) -> Self {
        DbValue::Text(v.to_string())
    }
}

impl From<bool> for DbValue {
    fn from(v: bool) -> Self {
        DbValue::Boolean(v)
    }
}

impl From<()> for DbValue {
    fn from(_: ()) -> Self {
        DbValue::Null
    }
}

impl<T: Into<DbValue>> From<Option<T>> for DbValue {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => v.into(),
            None => DbValue::Null,
        }
    }
}

impl<T: Into<DbValue>> From<Vec<T>> for DbValue {
    fn from(v: Vec<T>) -> Self {
        DbValue::Array(v.into_iter().map(Into::into).collect())
    }
}
