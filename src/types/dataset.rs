#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Dataset {
    pub id: String,
    pub height: Option<u64>,
}
