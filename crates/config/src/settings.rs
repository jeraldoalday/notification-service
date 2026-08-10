use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub server: ServerSettings,
    pub database: DatabaseSettings,
    pub queue: QueueSettings,
    pub smtp: SmtpSettings,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            server: ServerSettings::default(),
            database: DatabaseSettings::default(),
            queue: QueueSettings::default(),
            smtp: SmtpSettings::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerSettings {
    pub host: String,
    pub port: u16,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DatabaseSettings {
    pub url: String,
    pub max_connections: u32,
}

impl Default for DatabaseSettings {
    fn default() -> Self {
        Self {
            url: "sqlite://notifications.db".to_string(),
            max_connections: 10,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct QueueSettings {
    pub poll_interval_ms: u64,
    pub batch_size: u32,
    pub max_retry_attempts: u32,
}

impl Default for QueueSettings {
    fn default() -> Self {
        Self {
            poll_interval_ms: 500,
            batch_size: 25,
            max_retry_attempts: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SmtpSettings {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub from_address: String,
}

impl Default for SmtpSettings {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 587,
            username: String::new(),
            from_address: "notifications@example.com".to_string(),
        }
    }
}
