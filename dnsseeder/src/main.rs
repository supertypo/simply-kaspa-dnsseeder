//! Top-level binary that wires the crawler, DNS server and HTTP server
//! together. Logging, signal handling and a graceful shutdown broadcast
//! are managed here; everything domain-specific lives in the library crates.

mod metrics_source;
mod stats;

use std::net::SocketAddr;
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
use simply_kaspa_dnsseeder_crawler::{KaspadProbe, ProbeInitializerConfig, Scheduler, SchedulerConfig, TokioResolver};
use simply_kaspa_dnsseeder_dns::{DnsConfig, SeederHandler};
use simply_kaspa_dnsseeder_store::PeerStore;
use simply_kaspa_dnsseeder_web::{AppState, MetricsSource, SchedulerProber, WebConfig, run_web_server};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::broadcast;

use crate::metrics_source::SubsystemMetrics;
use crate::stats::{Metrics, stats_loop};

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
    let network_id =
        NetworkId::from_str(&cli.network_id).map_err(|err| anyhow!("invalid --network-id `{}`: {err}", cli.network_id))?;

    let datadir = prepare_datadir(&cli.datadir, network_id).await?;
    let store_path = datadir.join("peers.redb");
    let store = PeerStore::open(&store_path).with_context(|| format!("opening store at {store_path:?}"))?;
    info!("store: persistence at {store_path:?}");

    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    spawn_signal_handler(shutdown_tx.clone());

    let metrics = Metrics::new(network_id, CliArgs::version(), cli.stale_good);
    metrics.load_from(&store);

    let probe_cfg = ProbeInitializerConfig::new(network_id, cli.probe_timeout);
    let probe = Arc::new(KaspadProbe::new(probe_cfg));

    let scheduler_cfg = SchedulerConfig {
        network_id,
        threads: cli.threads,
        probe_tick: cli.probe_tick,
        stale_good: cli.stale_good,
        stale_bad: cli.stale_bad,
        dead_after: cli.dead_after,
        seeders: cli.seeder.iter().cloned().collect(),
        strict_port: cli.strict_port,
    };
    let resolver = Arc::new(TokioResolver);
    let scheduler = Scheduler::with_metrics(scheduler_cfg, store.clone(), probe.clone(), resolver, metrics.crawler.clone());

    let scheduler_shutdown = shutdown_tx.subscribe();
    let scheduler_task = tokio::spawn(async move {
        if let Err(err) = scheduler.run(scheduler_shutdown).await {
            error!("scheduler exited: {err}");
        }
    });

    let dns_task = if cli.dns_enabled() {
        let dns_listen: SocketAddr = cli
            .dns_listen
            .parse()
            .with_context(|| format!("invalid --dns-listen `{}`", cli.dns_listen))?;
        let dns_cfg = DnsConfig {
            stale_good: cli.stale_good,
            min_protocol_version: cli.min_protocol_version,
            min_user_agent: cli.min_user_agent.clone(),
            ..DnsConfig::new(
                network_id,
                dns_listen,
                cli.dns_zone.clone().expect("dns_enabled implies dns_zone"),
                cli.dns_nameserver.clone().expect("dns_enabled implies dns_nameserver"),
            )
        };
        let tcp_idle = dns_cfg.tcp_idle_timeout;
        let handler = SeederHandler::with_metrics(dns_cfg, store.clone(), metrics.dns.clone()).context("building dns handler")?;
        let dns_shutdown = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            match simply_kaspa_dnsseeder_dns::run_dns_server_with_handler(handler, dns_listen, tcp_idle, dns_shutdown).await {
                Ok(()) => info!("dns: shut down cleanly"),
                Err(err) => error!("dns server exited: {err}"),
            }
        }))
    } else {
        info!("dns: disabled (set --dns-zone and --dns-nameserver to enable)");
        None
    };

    let http_listen: SocketAddr = cli
        .http_listen
        .parse()
        .with_context(|| format!("invalid --http-listen `{}`", cli.http_listen))?;
    let web_cfg = WebConfig {
        listen: http_listen,
        api_key: cli.api_key.clone(),
        allowed_origins: cli.allowed_origins.clone(),
        post_rate_limit: cli.post_rate_limit,
        rate_limit_window: cli.rate_limit_window,
        network_default_port: network_id.default_p2p_port(),
        strict_port: cli.strict_port,
        api_prefix: cli.api_prefix.clone(),
        db_path: store_path.clone(),
        stale_good: cli.stale_good,
        min_protocol_version: cli.min_protocol_version,
        min_user_agent: cli.min_user_agent.clone(),
        service_name: "simply-kaspa-dnsseeder",
        service_version: CliArgs::version(),
    };
    let prober = Arc::new(SchedulerProber::new(probe.clone(), store.clone()));
    let metrics_source: Arc<dyn MetricsSource> = Arc::new(SubsystemMetrics {
        crawler: metrics.crawler.clone(),
        dns: metrics.dns.clone(),
    });
    let state = AppState::full(store.clone(), prober, web_cfg, metrics.web.clone(), metrics_source);
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
    let base = PathBuf::from(raw);
    let dir = if network_id.network_type == kaspa_consensus_core::network::NetworkType::Mainnet {
        base
    } else {
        base.join(network_id.to_string())
    };
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("creating datadir {dir:?}"))?;
    Ok(dir)
}

fn configure_logging(cli: &CliArgs) {
    env_logger::Builder::new()
        .target(env_logger::Target::Stdout)
        .format_target(false)
        .format_timestamp_millis()
        .parse_filters(&cli.log_level)
        .write_style(if cli.log_no_color {
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
