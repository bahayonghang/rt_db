use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct HistoryRecord {
    pub tag_name: String,
    pub timestamp: DateTime<Utc>,
    pub value: f64,
    pub tag_quality: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TagRecord {
    pub tag_id: i32,
    pub tag_name: String,
    pub tag_opc_name: Option<String>,
    pub opc_server_name: Option<String>,
    pub tag_unit: Option<String>,
    pub tag_type: Option<String>,
    pub tag_descrip: Option<String>,
    pub tag_val: Option<f64>,
    pub tag_min_val: Option<f64>,
    pub tag_max_val: Option<f64>,
    pub data_rec_flag: Option<String>,
    pub in_or_out_flag: Option<String>,
    pub tag_quality: Option<String>,
}

impl HistoryRecord {
    pub fn new(tag_name: String, timestamp: DateTime<Utc>, value: f64, tag_quality: Option<String>) -> Self {
        Self {
            tag_name,
            timestamp,
            value,
            tag_quality,
        }
    }
}

impl TagRecord {
    pub fn new(
        tag_id: i32,
        tag_name: String,
        tag_opc_name: Option<String>,
        opc_server_name: Option<String>,
        tag_unit: Option<String>,
        tag_type: Option<String>,
        tag_descrip: Option<String>,
        tag_val: Option<f64>,
        tag_min_val: Option<f64>,
        tag_max_val: Option<f64>,
        data_rec_flag: Option<String>,
        in_or_out_flag: Option<String>,
        tag_quality: Option<String>,
    ) -> Self {
        Self {
            tag_id,
            tag_name,
            tag_opc_name,
            opc_server_name,
            tag_unit,
            tag_type,
            tag_descrip,
            tag_val,
            tag_min_val,
            tag_max_val,
            data_rec_flag,
            in_or_out_flag,
            tag_quality,
        }
    }
}