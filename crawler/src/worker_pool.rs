//! Fixed-size worker pool that consumes addresses from a bounded channel
//! and runs concurrent probes against them.
//!
//! Decouples *scheduling* (when to probe, which peer is overdue) from
//! *execution* (running N probes in parallel, freeing slots, propagating
//! shutdown).

use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashSet;
use log::debug;
use simply_kaspa_dnsseeder_store::{NetAddress, PeerStore};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::metrics::CrawlerMetrics;
use crate::probe::Probe;
use crate::probe_runner::probe_one;

/// Probe queue depth per worker. Bounds how many ready peers can sit waiting
/// for a worker before the producer back-pressures via `try_send`.
const CHANNEL_PER_THREAD: usize = 4;

/// Dependencies a probe worker needs to run.
#[derive(Clone)]
pub(crate) struct WorkerCtx {
    pub probe: Arc<dyn Probe>,
    pub store: PeerStore,
    pub in_flight: Arc<DashSet<SocketAddr>>,
    pub metrics: Arc<CrawlerMetrics>,
    pub cancel: CancellationToken,
    pub default_port: u16,
    pub strict_port: bool,
}

/// Outcome of an enqueue attempt.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum EnqueueOutcome {
    /// Queued for a worker.
    Accepted,
    /// Address is already in flight (or queued).
    Duplicate,
    /// Channel is full; producer should back off until workers drain.
    Full,
    /// Pool is shutting down.
    Closed,
}

/// Bounded-channel + fixed-worker-pool dispatcher.
pub(crate) struct WorkerPool {
    tx: mpsc::Sender<NetAddress>,
    in_flight: Arc<DashSet<SocketAddr>>,
    dispatcher: JoinHandle<()>,
}

impl WorkerPool {
    /// Spawn the dispatcher task; returns a handle whose `try_enqueue` feeds it.
    pub fn spawn(ctx: WorkerCtx, threads: usize) -> Self {
        let threads = threads.max(1);
        let (tx, rx) = mpsc::channel::<NetAddress>(threads.saturating_mul(CHANNEL_PER_THREAD));
        let in_flight = ctx.in_flight.clone();
        let dispatcher = tokio::spawn(run_dispatcher(rx, ctx, threads));
        Self { tx, in_flight, dispatcher }
    }

    /// Try to enqueue an address for probing. Non-blocking.
    pub fn try_enqueue(&self, net: NetAddress) -> EnqueueOutcome {
        let addr = SocketAddr::new(net.ip, net.port);
        if !self.in_flight.insert(addr) {
            return EnqueueOutcome::Duplicate;
        }
        match self.tx.try_send(net) {
            Ok(()) => EnqueueOutcome::Accepted,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.in_flight.remove(&addr);
                EnqueueOutcome::Full
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.in_flight.remove(&addr);
                EnqueueOutcome::Closed
            }
        }
    }

    /// Current number of addresses queued or in flight.
    pub fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }

    /// Drop the sender and wait for the dispatcher to finish draining/aborting workers.
    pub async fn shutdown(self) {
        let Self { tx, dispatcher, .. } = self;
        drop(tx);
        if let Err(err) = dispatcher.await {
            log::warn!("crawler: worker dispatcher exited abnormally: {err}");
        }
    }
}

async fn run_dispatcher(mut rx: mpsc::Receiver<NetAddress>, ctx: WorkerCtx, threads: usize) {
    let mut workers: JoinSet<()> = JoinSet::new();
    loop {
        tokio::select! {
            () = ctx.cancel.cancelled() => break,
            Some(net) = rx.recv(), if workers.len() < threads => {
                let addr = SocketAddr::new(net.ip, net.port);
                let ctx = ctx.clone();
                workers.spawn(probe_worker(ctx, addr));
            }
            Some(_) = workers.join_next(), if !workers.is_empty() => {}
            else => break,
        }
    }
    workers.abort_all();
    while workers.join_next().await.is_some() {}
}

async fn probe_worker(ctx: WorkerCtx, addr: SocketAddr) {
    let _guard = InFlightGuard::new(addr, ctx.in_flight.clone(), ctx.metrics.clone());
    tokio::select! {
        () = probe_one(ctx.probe.as_ref(), &ctx.store, addr, ctx.default_port, ctx.strict_port, Some(&ctx.metrics)) => {}
        () = ctx.cancel.cancelled() => debug!("crawler: probe {addr} dropped on shutdown"),
    }
}

/// RAII slot: increments the gauge + holds the `in_flight` entry, frees both on drop (including panic).
struct InFlightGuard {
    addr: SocketAddr,
    in_flight: Arc<DashSet<SocketAddr>>,
    metrics: Arc<CrawlerMetrics>,
}

impl InFlightGuard {
    fn new(addr: SocketAddr, in_flight: Arc<DashSet<SocketAddr>>, metrics: Arc<CrawlerMetrics>) -> Self {
        metrics.in_flight_inc();
        Self { addr, in_flight, metrics }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.metrics.in_flight_dec();
        self.in_flight.remove(&self.addr);
    }
}
