use prometheus_client::metrics::{counter::Counter, gauge::Gauge};

lazy_static::lazy_static! {
    pub static ref S3_REQUESTS: Counter = Default::default();
    pub static ref S3_KEYS_LISTED: Gauge = Default::default();
}
