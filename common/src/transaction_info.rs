use chrono::DateTime;
use uuid::Uuid;

type UtcDateTime = DateTime<chrono::Utc>;

#[derive(Clone, Debug)]
pub struct TransactionInfo {
    pub id: Uuid,
    pub started: UtcDateTime,
    pub overall_timeout: std::time::Duration,
}
