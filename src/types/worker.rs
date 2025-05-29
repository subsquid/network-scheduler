use libp2p_identity::PeerId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    Online,
    Stale,
    UnsupportedVersion,
    Offline,
}

impl std::fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerStatus::Online => write!(f, "online"),
            WorkerStatus::Stale => write!(f, "stale"),
            WorkerStatus::UnsupportedVersion => write!(f, "unsupported_version"),
            WorkerStatus::Offline => write!(f, "offline"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Worker {
    #[serde(rename = "peer_id")]
    pub id: PeerId,
    pub status: WorkerStatus,
}

impl Worker {
    pub fn reliable(&self) -> bool {
        matches!(self.status, WorkerStatus::Online)
    }
}
