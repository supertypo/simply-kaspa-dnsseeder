# AGENTS.md

## What is this

A Kaspa DNS seeder: it crawls the Kaspa P2P network, stores reachable peers, and serves a subset of them over an authoritative DNS zone so new nodes can bootstrap.

## Core mental model

Three cooperating loops driven from one binary:

1. **Crawler** (`crawler::Scheduler`) — periodic ticker drains eligible peers from the store, dispatches bounded-concurrency probes, persists results.
2. **DNS server** (`dns::SeederHandler`) — answers `A` / `AAAA` / `NS` / `SOA` from a small in-memory `ServingCache` periodically rebuilt from the store; the handler itself never touches redb on the request path.
3. **HTTP API** (`web`) — `axum` router under a configurable `--api-prefix`: `/health`, `/metrics`, `/peers` (GET/POST), `/peers/{addr_port}` (GET/DELETE). Handlers live in `web/src/http/handlers/{health,metrics,peers}.rs`; routing in `web/src/http/router.rs`; api-key middleware in `web/src/http/middleware.rs`; auth helpers in `web/src/http/auth.rs`; request inspection in `web/src/http/request.rs`; sysinfo collection in `web/src/runtime/system.rs`. Response shapes live exclusively in `web/src/dto/` — handlers must not define DTOs inline, and subsystem JSON contributed via `MetricsSource` must come from typed `dto::subsystems::*` DTOs serialized through `serde_json::to_value`, never the `json!` macro.

A fourth task — `dnsseeder::stats::stats_loop` — emits a single info-level stats block on `--stats-interval` (set to `0s` to disable) and persists cumulative counters to the store so totals survive restarts.

The store (`store::PeerStore`, redb-backed) is the only shared state. There is no in-memory peer list — every selection goes through the store. This matters: nothing the crawler discovers becomes visible to DNS until it's been *successfully probed* (see "Two-tier eligibility" below).

## Tricky parts

### Discovery never enqueues directly
When a probe succeeds and the peer advertises addresses, those addresses are written to the store via `PeerStore::insert_stub_if_missing` and **nothing else**. The scheduler's `probe_tick` is the *only* code path that picks peers to probe. This is intentional — letting discovery enqueue caused ephemeral-port floods where one chatty peer could pin every worker.

A "stub" record has `last_success_ms = 0` and `last_attempt_ms = 0`. The DNS filter rejects stubs (see eligibility filter), and the scheduler treats them as "never succeeded" peers for cadence purposes.

### In-flight back-pressure
`enqueue_probes` overfetches a small multiple of `--threads` from the store's most-overdue index, then walks the candidates handing each to `WorkerPool::try_enqueue`. The pool deduplicates against an in-flight set and reports `Full` once its bounded channel is saturated — the scheduler stops dispatching for that tick and counts the rest as `skipped_backpressure`. Without this cap, defaults of `probe_timeout >= probe_tick` would grow the backlog unboundedly. Probes themselves are bounded by `Semaphore::new(threads)` acquired *inside* each spawned task.

### Two-tier eligibility
`store::is_eligible_for_probe` decides what to re-probe:
- Past the dead cutoff (`--dead-after`): **never** — `prune_dead` will eventually delete it.
- Succeeded at least once: re-probe every `--stale-good`.
- Never succeeded (stub or repeated failures): re-probe every `--stale-bad`.

The DNS filter uses the same `stale_good` window on `last_success_ms` — so DNS only ever returns peers we've verifiably reached recently. Stubs (`last_success_ms = 0`) are filtered out automatically because `now - 0` always exceeds the window.

### Multi-round address harvesting
Each successful probe issues `--probes-per-peer` back-to-back `RequestAddresses` rounds (capped 1..=10, separated by `PROBE_REPEAT_DELAY`) and unions the responses through a `HashSet` with early-exit on no new addresses or the `MAX_ADDRESSES_RECEIVE` ceiling. This matches Go's behaviour of milking each healthy connection for more peer addresses before closing it. Logic lives in `crawler::probe::initializer::collect_addresses`.

### DNS serving cache
The DNS request path never queries redb. `dns::ServingCache` holds an `Arc<Snapshot>` of per-family `Box<[IpAddr]>` lists chosen by `last_success_ms` (top `max_records * SNAPSHOT_MULTIPLIER`), rebuilt every `REFRESH_INTERVAL` by a background task (`dns::build_serving_cache`) using `spawn_blocking` for the redb scan. Each query takes a lock long enough to clone the `Arc`, then samples without further synchronization. Per-record TTLs are subsystem constants in `dns/src/handler.rs` (`A_TTL_SECONDS`, `NS_TTL_SECONDS`) and mirror the Go seeder's per-type values.

### Shutdown responsiveness
`Scheduler::run` selects on a `broadcast::Receiver<()>` plus two tickers. The dispatch path inside `enqueue_probes` **must stay non-blocking** — each probe acquires its own semaphore permit *inside* its spawned task, not in the dispatch loop. If you ever move `semaphore.acquire().await` back into the loop, a saturated worker pool will delay Ctrl+C by up to one full probe-timeout window. The signal handler in `main.rs` arms a one-shot graceful shutdown and force-exits on the second signal.

### Address canonicalization
IPv4-mapped IPv6 addresses (`::ffff:1.2.3.4`) are collapsed to plain IPv4 via `common::canonicalize_ip` *before* hitting the store. The store key is `(ip, port)` so without this, the same peer can occupy two rows.

### Port policy
`strict_port` (CLI flag, default off) makes both the crawler and the DNS filter reject any address whose port differs from the network's default P2P port. Important on Mainnet to filter out misconfigured nodes; relax for testnets where operators bind alt ports.

### Unknown-network bootstrap
`dnsseeder::network_gate` is the startup gate. Built-in networks are those with a non-empty `Params::dns_seeders` (currently mainnet + testnet-10). Anything else (devnet, simnet, custom testnet suffixes like `testnet-12`) requires `--seeder IP:port` — hostnames are rejected. `--seeder` is parsed as a literal `SocketAddr` only.

`network_gate::effective_default_port` produces the *effective* default P2P port plumbed into `SchedulerConfig.default_port`, `DnsConfig.default_port` and `WebConfig.network_default_port`:
- Built-in networks: `NetworkId::default_p2p_port()`. A `--seeder` peer's port is unrelated to the network default — the seeder is just an extra crawl target.
- Unknown networks: the port carried by `--seeder IP:port`. This is what DNS answers for, what `strict_port` enforces, and what `is_acceptable_address` substitutes when a peer advertises port 0.

`crawler::seeders::dns_seed_many` short-circuits to an empty list for any network not in `NetworkId::iter()` (avoids the panic-prone `Params::from(NetworkId)` for unknown suffixes).


### Filter knobs surfaced through the DNS query path
`store::Filter` lets the DNS handler filter on `min_protocol_version`, `min_user_agent` (semver), family (A vs AAAA), default port, and `stale_good_ms`. These come from `DnsConfig`, which is hydrated from CLI flags in `dnsseeder/src/main.rs`. Keep `DnsConfig::new` as the "all-defaults" constructor and use struct-update syntax for CLI overrides — don't multiply constructors.

### `MetricsSource` keeps the web crate independent
`/api/metrics` needs to surface crawler and dns counters, but the `web` crate must not depend on `crawler` or `dns`. The bridge is `web::MetricsSource { fn extra(&self) -> serde_json::Value; }`, implemented in `dnsseeder::metrics_source::SubsystemMetrics` and injected via `AppState::builder(...).metrics_source(...)`. Add new subsystem metrics there, not in the web crate.

### `ThreadRng` is `!Send`
Any shuffle/sample needs to be in a scoped block (`{ let mut rng = rand::rng(); ... }`) so the RNG drops before any `.await`. Clippy will catch this but the error message is opaque.

### Log prefixes are subsystem tags
Every log line starts with the *owning* subsystem (`crawler:`, `dns:`, `web:`, `store:`, `stats:`). DNS *bootstrap lookups* still use `crawler:` because the crawler owns them — only the inbound DNS server gets `dns:`. HTTP handlers tag method + route (`web: GET /peers store error: ...`).

## Conventions

- Edition 2024. `cargo clippy --workspace --all-targets -- -D warnings` must pass; CI mirrors this.
- After every code change, before declaring the task done, run all three of:
  - `cargo fmt --all`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace`
  Treat any warning as a failure. Don't skip these even for "trivial" edits — formatting and clippy drift add up fast.
- Tests live in sibling `*_tests.rs` files (e.g. `peer_store.rs` ↔ `peer_store_tests.rs`), not inline `#[cfg(test)] mod tests`.
- JSON over the HTTP API: always `serde(rename_all = "camelCase")`.
- CLI durations: parsed with `humantime` (suffixes `s`, `m`, `h`, `d`, ...).
- Never run `cargo clean` — the rusty-kaspa git deps are expensive to rebuild.
- Comments: only when the *why* is non-obvious. The code is the *what*. No step-by-step narration, no task/PR references, no "added for X" history.
- One responsibility per file. Split when a module starts mixing concerns (the `web::http::handlers/`, `web::runtime/`, and `dnsseeder::stats/` splits are the templates). Use the modern `name.rs` + `name/` module pattern; no `mod.rs`.
- Persisted store keys (redb table names, blob keys) are never versioned in their name. On an incompatible on-disk shape, warn and overwrite at read time; don't bump a `_v2` suffix.

## Keeping this document useful

This file is the agent onboarding cheat sheet. When you change something that contradicts it — new endpoint, renamed module, changed default, new invariant, removed knob — update the relevant section in the same change. Keep it short and to the point: facts and invariants only, no historical narrative, no exhaustive API lists (the README owns those). If a section gets longer than ~5 lines, that's a hint to either tighten it or split the concept out of the codebase.

## Keeping the Dockerfile in sync

`docker/Dockerfile` has a dependency-prefetch stub block that lists every workspace member's `Cargo.toml` and creates a placeholder `lib.rs` / `main.rs` / `build.rs` for each. When you add, remove, or rename a workspace member (or its crate root layout changes), update both the `COPY ... Cargo.toml` lines and the `RUN mkdir ... && echo ...` block to match. Forgetting this silently skips the dep cache or breaks the image build. The Rust base image version is read from `Cargo.toml`'s `rust-version` by `docker/build.sh` and passed in as `--build-arg RUST_VERSION=...`, so bumping the toolchain only requires editing `Cargo.toml` + `rust-toolchain.toml`.

## Quick-start commands

```bash
cargo build                                              # build everything
cargo fmt --all                                          # format (run before lint/test)
cargo test --workspace                                   # run all tests
cargo clippy --workspace --all-targets -- -D warnings    # lint gate
cargo run -p simply-kaspa-dnsseeder -- --help            # see flags
```
