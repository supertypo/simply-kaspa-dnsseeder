use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("store error: {0}")]
    Store(#[from] simply_kaspa_dnsseeder_store::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum ProbeError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("store error: {0}")]
    Store(#[from] simply_kaspa_dnsseeder_store::Error),
    #[error("handshake error: {0}")]
    Handshake(String),
    #[error("network mismatch: local {local}, remote {remote}")]
    NetworkMismatch { local: String, remote: String },
    #[error("addresses exchange error: {0}")]
    Addresses(String),
    #[error("timed out")]
    Timeout,
    #[error("address list exceeded limit: {0}")]
    TooManyAddresses(usize),
}
