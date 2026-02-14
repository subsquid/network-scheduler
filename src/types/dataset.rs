#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Dataset {
    pub name: String,
    pub height: Option<u64>,
}
