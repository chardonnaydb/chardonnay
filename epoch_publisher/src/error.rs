use flatbuf::epoch_publisher_flatbuffers::epoch_publisher::Status;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Invalid Request Format Error")]
    InvalidRequestFormat,
    #[error("Invalid Response Format Error")]
    InvalidResponseFormat,
    #[error("Timeout Error")]
    Timeout,
    #[error("Epoch Unknown Error")]
    EpochUnknown,
    #[error("Unknown Internal Error")]
    InternalError,
}

impl Error {
    pub fn from_flatbuf_status(status: Status) -> Result<(), Self> {
        match status {
            Status::Ok => Ok(()),
            Status::InvalidRequestFormat => Err(Self::InvalidRequestFormat),
            Status::Timeout => Err(Self::Timeout),
            Status::EpochUnknown => Err(Self::EpochUnknown),
            _ => Err(Self::InternalError),
        }
    }
}
