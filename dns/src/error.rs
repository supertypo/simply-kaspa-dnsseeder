use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("dns proto error: {0}")]
    Proto(#[from] hickory_proto::ProtoError),
    #[error("store error: {0}")]
    Store(#[from] simply_kaspa_dnsseeder_store::Error),
}
