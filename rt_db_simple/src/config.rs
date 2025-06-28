use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub database: DatabaseConfig,
    pub query: QueryConfig,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    pub server: String,
    pub database: String,
    pub username: String,
    pub password: String,
    pub port: Option<u16>,
}

#[derive(Debug, Deserialize)]
pub struct QueryConfig {
    pub history_table: String,
    pub tag_table: String,
    pub days_back: i32,
}

impl Config {
    pub fn load_from_file(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}

pub fn load_config(path: &str) -> Result<Config> {
    Config::load_from_file(path)
}