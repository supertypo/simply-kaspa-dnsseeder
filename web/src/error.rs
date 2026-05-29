use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("store error: {0}")]
    Store(#[from] simply_kaspa_dnsseeder_store::Error),
    #[error("tls error: {0}")]
    Tls(std::io::Error),
    #[error("no listen addresses configured")]
    NoListenAddrs,
}
