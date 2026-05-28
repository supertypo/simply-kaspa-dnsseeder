//! Periodic stats dump: aggregates subsystem counters, a one-pass
//! [`PeerStore`] summary, and process metadata. Counters persist to the store
//! after every dump so totals survive restarts (worst case: last interval's
//! increments are lost).

mod format;
mod render;
mod snapshot;

use std::sync::Arc;
use std::time::{Duration, Instant};

use kaspa_consensus_core::network::NetworkId;
use log::{debug, info, warn};
use simply_kaspa_dnsseeder_common::now_ms;
use simply_kaspa_dnsseeder_crawler::CrawlerMetrics;
use simply_kaspa_dnsseeder_dns::DnsMetrics;
use simply_kaspa_dnsseeder_store::PeerStore;
use simply_kaspa_dnsseeder_web::WebMetrics;
use tokio::sync::broadcast;

use self::render::{Block, render};

/// Gathered metrics ready to render and persist. Pure data — no I/O.
pub(super) struct MetricsReport {
    block: Block,
    now_ms: i64,
}

impl MetricsReport {
    pub(super) fn render(&self) -> Vec<String> {
        render(&self.block)
    }
}

pub struct Metrics {
    pub crawler: Arc<CrawlerMetrics>,
    pub dns: Arc<DnsMetrics>,
    pub web: Arc<WebMetrics>,
    started: Instant,
    network: NetworkId,
    version: &'static str,
    stale_good: Duration,
}

impl Metrics {
    #[must_use]
    pub fn new(network: NetworkId, version: &'static str, stale_good: Duration) -> Arc<Self> {
        Arc::new(Self {
            crawler: Arc::new(CrawlerMetrics::new()),
            dns: Arc::new(DnsMetrics::new()),
            web: Arc::new(WebMetrics::new()),
            started: Instant::now(),
            network,
            version,
            stale_good,
        })
    }

    pub fn load_from(&self, store: &PeerStore) {
        snapshot::load(store, &self.crawler, &self.dns, &self.web);
    }

    /// Gather a snapshot of subsystem counters plus a one-pass store summary.
    /// Returns `None` if the store summary cannot be fetched.
    fn snapshot(&self, store: &PeerStore) -> Option<MetricsReport> {
        let now = now_ms();
        let stale_good_ms = i64::try_from(self.stale_good.as_millis()).unwrap_or(i64::MAX);
        let summary = match store.summary(now, stale_good_ms) {
            Ok(s) => s,
            Err(err) => {
                warn!("stats: store summary failed: {err}");
                return None;
            }
        };
        let block = Block {
            uptime: self.started.elapsed(),
            network: self.network,
            version: self.version,
            summary_good: summary.good,
            summary_stale: summary.stale,
            summary_failed: summary.failed,
            summary_v4: summary.v4,
            summary_v6: summary.v6,
            avg_age: Duration::from_millis(summary.avg_success_age_ms),
            crawler: self.crawler.snapshot(),
            dns: self.dns.snapshot(),
            web: self.web.snapshot(),
        };
        Some(MetricsReport { block, now_ms: now })
    }

    /// Emit a single stats block and persist the snapshot.
    pub fn dump(&self, store: &PeerStore) {
        let Some(report) = self.snapshot(store) else { return };
        for line in report.render() {
            info!("{line}");
        }
        snapshot::save(store, &report.block.crawler, &report.block.dns, &report.block.web, report.now_ms);
    }
}

pub async fn stats_loop(metrics: Arc<Metrics>, store: PeerStore, interval: Duration, mut shutdown: broadcast::Receiver<()>) {
    if interval.is_zero() {
        return;
    }
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = shutdown.recv() => {
                debug!("stats: shutdown signal received");
                dump_blocking(&metrics, &store).await;
                return;
            }
            _ = ticker.tick() => {
                dump_blocking(&metrics, &store).await;
            }
        }
    }
}

async fn dump_blocking(metrics: &Arc<Metrics>, store: &PeerStore) {
    let metrics = metrics.clone();
    let store = store.clone();
    if let Err(err) = tokio::task::spawn_blocking(move || metrics.dump(&store)).await {
        warn!("stats: dump task panicked: {err}");
    }
}
