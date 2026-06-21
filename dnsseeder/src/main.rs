//! Top-level binary that wires the crawler, DNS server and HTTP server
//! together. Logging, signal handling and a graceful shutdown broadcast
//! are managed here; everything domain-specific lives in the library crates.

mod metrics_source;
mod network_gate;
#[cfg(test)]
mod network_gate_tests;
mod stats;

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use kaspa_consensus_core::network::NetworkId;
use log::{error, info, warn};
use simply_kaspa_dnsseeder_cli::CliArgs;
use simply_kaspa_dnsseeder_common::generate_api_key;
use simply_kaspa_dnsseeder_crawler::{KaspadProbe, ProbeInitializerConfig, Scheduler, SchedulerConfig, TokioResolver};
use simply_kaspa_dnsseeder_dns::{DnsConfig, SeederHandler, build_serving_cache};
use simply_kaspa_dnsseeder_store::PeerStore;
use simply_kaspa_dnsseeder_web::{AppState, MetricsSource, SchedulerProber, WebConfig, run_web_server};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::broadcast;

use crate::metrics_source::SubsystemMetrics;
use crate::network_gate::{effective_default_port, require_seeder_for_unknown_network};
use crate::stats::{Metrics, ValidityCriteria, stats_loop};

#[tokio::main]
async fn main() {
    let cli_args = CliArgs::parse();
    configure_logging(&cli_args);
    info!("simply-kaspa-dnsseeder {} ({})", CliArgs::version(), CliArgs::commit_id());

    if let Err(err) = run(cli_args).await {
        error!("fatal: {err:#}");
        std::process::exit(1);
    }
}

async fn run(cli: CliArgs) -> Result<()> {
    cli.validate().map_err(|e| anyhow!(e))?;
    let network_id =
        NetworkId::from_str(&cli.network_id).map_err(|err| anyhow!("invalid --network-id `{}`: {err}", cli.network_id))?;
    require_seeder_for_unknown_network(network_id, cli.crawler.seeder.as_deref())?;
    let default_port = effective_default_port(network_id, cli.crawler.seeder.as_deref());

    let datadir = prepare_datadir(&cli.datadir, network_id).await?;
    let store_path = datadir.join("peers.redb");
    let store = PeerStore::open(&store_path).with_context(|| format!("opening store at {}", store_path.display()))?;
    info!("store: persistence at {}", store_path.display());

    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    spawn_signal_handler(shutdown_tx.clone());

    let validity = ValidityCriteria {
        min_protocol_version: cli.dns.min_protocol_version,
        min_user_agent: cli.dns.min_user_agent.clone(),
        strict_default_port: cli.crawler.strict_port.then_some(default_port),
    };
    let metrics = Metrics::new(network_id, CliArgs::version(), cli.crawler.stale_good, validity);
    metrics.load_from(&store);

    let probe_cfg = ProbeInitializerConfig::new(network_id, cli.crawler.probe_timeout, cli.crawler.probes_per_peer);
    let probe = Arc::new(KaspadProbe::new(probe_cfg));

    let scheduler_cfg = SchedulerConfig {
        network_id,
        default_port,
        threads: cli.crawler.threads,
        probe_tick: cli.crawler.probe_tick,
        stale_good: cli.crawler.stale_good,
        stale_bad: cli.crawler.stale_bad,
        dead_after: cli.crawler.dead_after,
        seeders: cli.crawler.seeder.iter().cloned().collect(),
        strict_port: cli.crawler.strict_port,
    };
    let resolver = Arc::new(TokioResolver);
    let scheduler = Scheduler::new(scheduler_cfg, store.clone(), probe.clone(), resolver, metrics.crawler.clone());

    let scheduler_shutdown = shutdown_tx.subscribe();
    let scheduler_task = tokio::spawn(async move {
        if let Err(err) = scheduler.run(scheduler_shutdown).await {
            error!("scheduler exited: {err}");
        }
    });

    let mut serving_cache_handle: Option<Arc<simply_kaspa_dnsseeder_dns::ServingCache>> = None;
    let mut dns_limiter: Option<Arc<simply_kaspa_dnsseeder_common::RateLimiter>> = None;
    let dns_task = if cli.dns_enabled() {
        let dns_listen = cli.dns.dns_listen.clone();
        let dns_cfg = DnsConfig {
            stale_good: cli.crawler.stale_good,
            min_protocol_version: cli.dns.min_protocol_version,
            min_user_agent: cli.dns.min_user_agent.clone(),
            max_records: cli.dns.dns_max_records.into(),
            ..DnsConfig::new(
                network_id,
                default_port,
                dns_listen.clone(),
                cli.dns.dns_zone.clone().expect("dns_enabled implies dns_zone"),
                cli.dns.dns_nameserver.clone().expect("dns_enabled implies dns_nameserver"),
            )
        };
        let tcp_idle = dns_cfg.tcp_idle_timeout;
        let dns_shutdown = shutdown_tx.subscribe();
        let (serving_cache, _refresher) = build_serving_cache(&dns_cfg, store.clone(), shutdown_tx.subscribe());
        serving_cache_handle = Some(serving_cache.clone());
        let handler = SeederHandler::with_metrics(dns_cfg, serving_cache, metrics.dns.clone()).context("building dns handler")?;
        dns_limiter = Some(handler.rate_limiter());
        Some(tokio::spawn(async move {
            match simply_kaspa_dnsseeder_dns::run_dns_server_with_handler(handler, dns_listen, tcp_idle, dns_shutdown).await {
                Ok(()) => info!("dns: shut down cleanly"),
                Err(err) => panic!("dns server exited: {err}"),
            }
        }))
    } else {
        info!("dns: disabled (set --dns-zone and --dns-nameserver to enable)");
        None
    };

    const API_KEY_KV: &str = "api_key";
    let api_key = if let Some(key) = cli.http.api_key.clone() {
        info!("web: X-API-KEY: {key}");
        key
    } else {
        store
            .blocking(|s| {
                match s.get_blob(API_KEY_KV) {
                    Ok(Some(bytes)) => match String::from_utf8(bytes) {
                        Ok(key) => {
                            info!("web: X-API-KEY: {key} (loaded)");
                            return key;
                        }
                        Err(_) => warn!("web: stored api-key is not valid UTF-8, regenerating"),
                    },
                    Ok(None) => {}
                    Err(err) => warn!("web: failed to read api-key from store: {err}"),
                }
                let key = generate_api_key();
                if let Err(err) = s.put_blob(API_KEY_KV, key.as_bytes()) {
                    warn!("web: failed to persist api-key to store: {err}");
                }
                info!("web: X-API-KEY: {key} (generated)");
                key
            })
            .await
    };

    let web_cfg = WebConfig {
        listen: cli.http.http_listen.clone(),
        api_key,
        allowed_origins: cli.http.allowed_origins.clone(),
        post_rate_limit: cli.http.post_rate_limit,
        rate_limit_window: cli.http.rate_limit_window,
        network_default_port: default_port,
        strict_port: cli.crawler.strict_port,
        api_prefix: cli.http.api_prefix.clone(),
        db_path: store_path.clone(),
        stale_good: cli.crawler.stale_good,
        min_protocol_version: cli.dns.min_protocol_version,
        min_user_agent: cli.dns.min_user_agent.clone(),
        service_name: "simply-kaspa-dnsseeder",
        service_version: CliArgs::version(),
        service_commit: CliArgs::commit_id(),
        service_network: network_id.to_string(),
        tls_cert: cli.http.tls_cert.clone(),
        tls_key: cli.http.tls_key.clone(),
    };
    let prober = Arc::new(SchedulerProber::new(probe.clone(), store.clone()));
    let metrics_source: Arc<dyn MetricsSource> = Arc::new(SubsystemMetrics {
        crawler: metrics.crawler.clone(),
        probe: probe.clone(),
        dns: metrics.dns.clone(),
        dns_limiter: dns_limiter.clone(),
        serving_cache: serving_cache_handle.clone(),
    });
    let state = AppState::builder(store.clone(), prober, web_cfg)
        .metrics(metrics.web.clone())
        .metrics_source(metrics_source)
        .build();
    let web_shutdown = shutdown_tx.subscribe();
    let web_task = tokio::spawn(async move {
        match run_web_server(state, web_shutdown).await {
            Ok(()) => info!("http: shut down cleanly"),
            Err(err) => error!("http server exited: {err}"),
        }
    });

    let stats_task = if cli.stats_interval > Duration::ZERO {
        info!("stats: dumping every {:?}", cli.stats_interval);
        let stats_shutdown = shutdown_tx.subscribe();
        let stats_store = store.clone();
        let stats_metrics = metrics.clone();
        Some(tokio::spawn(async move {
            stats_loop(stats_metrics, stats_store, cli.stats_interval, stats_shutdown).await;
        }))
    } else {
        info!("stats: periodic dump disabled");
        None
    };

    if let Err(err) = scheduler_task.await {
        error!("scheduler task ended unexpectedly: {err}");
    }
    if let Some(dns) = dns_task
        && let Err(err) = dns.await
    {
        error!("dns task ended unexpectedly: {err}");
    }
    if let Err(err) = web_task.await {
        error!("web task ended unexpectedly: {err}");
    }
    if let Some(stats) = stats_task
        && let Err(err) = stats.await
    {
        error!("stats task ended unexpectedly: {err}");
    }
    Ok(())
}

async fn prepare_datadir(raw: &str, network_id: NetworkId) -> Result<PathBuf> {
    let dir = PathBuf::from(raw).join(network_id.to_string());
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("creating datadir {}", dir.display()))?;
    Ok(dir)
}

fn configure_logging(cli: &CliArgs) {
    env_logger::Builder::new()
        .target(env_logger::Target::Stdout)
        .format_target(false)
        .format_timestamp_millis()
        .parse_filters(&cli.logging.log_level)
        .write_style(if cli.logging.log_no_color {
            env_logger::WriteStyle::Never
        } else {
            env_logger::WriteStyle::Always
        })
        .init();
}

fn spawn_signal_handler(shutdown: broadcast::Sender<()>) {
    let sent = Arc::new(AtomicBool::new(false));
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        loop {
            let name = tokio::select! {
                _ = sigint.recv() => "SIGINT",
                _ = sigterm.recv() => "SIGTERM",
            };
            if sent.load(Ordering::Relaxed) {
                warn!("{name} received again, terminating immediately");
                std::process::exit(1);
            }
            warn!("{name} received, stopping... (repeat for forced close)");
            let _ = shutdown.send(());
            sent.store(true, Ordering::Relaxed);
        }
    });
}
