use std::net::SocketAddr;
use std::path::PathBuf;

use thiserror::Error;

pub mod api;

pub(crate) use api::ApiError;

#[derive(Debug, Clone, Copy)]
pub enum TlsFile {
    Cert,
    Key,
}

impl std::fmt::Display for TlsFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Cert => "certificate",
            Self::Key => "private key",
        })
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("store error: {0}")]
    Store(#[from] simply_kaspa_dnsseeder_store::Error),
    #[error("tls {kind} load failed for {path}: {source}")]
    Tls {
        kind: TlsFile,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("no listen addresses configured")]
    NoListenAddrs,
    #[error("failed to bind {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
}
