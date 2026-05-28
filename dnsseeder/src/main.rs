//! Top-level binary that wires the crawler, DNS server and HTTP server
//! together. Logging, signal handling and a graceful shutdown broadcast
//! are managed here; everything domain-specific lives in the library crates.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use kaspa_consensus_core::network::NetworkId;
use log::{error, info, warn};
use simply_kaspa_dnsseeder_cli::CliArgs;
use simply_kaspa_dnsseeder_crawler::{
    KaspadProbe, ProbeInitializerConfig, Scheduler, SchedulerConfig, TokioResolver,
};
use simply_kaspa_dnsseeder_dns::{DnsConfig, run_dns_server};
use simply_kaspa_dnsseeder_store::PeerStore;
use simply_kaspa_dnsseeder_web::{AppState, SchedulerProber, WebConfig, run_web_server};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::broadcast;

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
    let network_id = NetworkId::from_str(&cli.network_id)
        .map_err(|err| anyhow!("invalid --network-id `{}`: {err}", cli.network_id))?;

    let datadir = prepare_datadir(&cli.datadir, network_id).await?;
    let store_path = datadir.join("peers.redb");
    let store = PeerStore::open(&store_path).with_context(|| format!("opening store at {store_path:?}"))?;
    info!("store: persistence at {store_path:?}");

    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    spawn_signal_handler(shutdown_tx.clone());

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
    let scheduler = Scheduler::new(scheduler_cfg, store.clone(), probe.clone(), resolver);

    let scheduler_shutdown = shutdown_tx.subscribe();
    let scheduler_task = tokio::spawn(async move {
        if let Err(err) = scheduler.run(scheduler_shutdown).await {
            error!("scheduler exited: {err}");
        }
    });

    let dns_task = if cli.dns_enabled() {
        let dns_listen: SocketAddr =
            cli.dns_listen.parse().with_context(|| format!("invalid --dns-listen `{}`", cli.dns_listen))?;
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
        let dns_store = store.clone();
        let dns_shutdown = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            match run_dns_server(dns_cfg, dns_store, dns_shutdown).await {
                Ok(()) => info!("dns: shut down cleanly"),
                Err(err) => error!("dns server exited: {err}"),
            }
        }))
    } else {
        info!("dns: disabled (set --dns-zone and --dns-nameserver to enable)");
        None
    };

    let http_listen: SocketAddr =
        cli.http_listen.parse().with_context(|| format!("invalid --http-listen `{}`", cli.http_listen))?;
    let web_cfg = WebConfig {
        listen: http_listen,
        api_key: cli.api_key.clone(),
        allowed_origins: cli.allowed_origins.clone(),
        post_rate_limit: cli.post_rate_limit,
        rate_limit_window: cli.rate_limit_window,
        network_default_port: network_id.default_p2p_port(),
        strict_port: cli.strict_port,
    };
    let prober = Arc::new(SchedulerProber::new(probe.clone(), store.clone()));
    let state = AppState::new(store.clone(), prober, web_cfg);
    let web_shutdown = shutdown_tx.subscribe();
    let web_task = tokio::spawn(async move {
        match run_web_server(state, web_shutdown).await {
            Ok(()) => info!("http: shut down cleanly"),
            Err(err) => error!("http server exited: {err}"),
        }
    });

    let _ = scheduler_task.await;
    if let Some(dns) = dns_task {
        let _ = dns.await;
    }
    let _ = web_task.await;
    Ok(())
}

async fn prepare_datadir(raw: &str, network_id: NetworkId) -> Result<PathBuf> {
    let base = PathBuf::from(raw);
    let dir = if network_id.network_type == kaspa_consensus_core::network::NetworkType::Mainnet {
        base
    } else {
        base.join(network_id.to_string())
    };
    tokio::fs::create_dir_all(&dir).await.with_context(|| format!("creating datadir {dir:?}"))?;
    Ok(dir)
}

fn configure_logging(cli: &CliArgs) {
    env_logger::Builder::new()
        .target(env_logger::Target::Stdout)
        .format_target(false)
        .format_timestamp_millis()
        .parse_filters(&cli.log_level)
        .write_style(if cli.log_no_color { env_logger::WriteStyle::Never } else { env_logger::WriteStyle::Always })
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
