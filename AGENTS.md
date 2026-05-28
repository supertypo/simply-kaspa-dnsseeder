# AGENTS.md

## What is this

A Kaspa DNS seeder: it crawls the Kaspa P2P network, stores reachable peers, and serves a subset of them over an authoritative DNS zone so new nodes can bootstrap. Rust port of the classic Go `dnsseeder` (kept under `../dnsseeder` for reference).

Workspace layout (just so the names map cleanly): `cli` (clap args), `store` (redb peer DB), `crawler` (probing + scheduler), `dns` (hickory authoritative server), `web` (HTTP API for ad-hoc submissions / introspection), `dnsseeder` (binary wiring it all together).

## Core mental model

Two cooperating loops driven from one binary:

1. **Crawler** (`crawler::Scheduler`) — periodic ticker drains eligible peers from the store, dispatches bounded-concurrency probes, persists results.
2. **DNS server** (`dns::SeederHandler`) — answers `A` / `AAAA` / `NS` / `SOA` from the same store, filtered for freshness and quality.

The store (`store::PeerStore`, redb-backed) is the only shared state. There is no in-memory peer list — every selection goes through the store. This matters: nothing the crawler discovers becomes visible to DNS until it's been *successfully probed* (see "Two-tier eligibility" below).

## Tricky parts

### Discovery never enqueues directly
When a probe succeeds and the peer advertises addresses, those addresses are written to the store via `PeerStore::insert_stub_if_missing` and **nothing else**. The scheduler's tick (`probe_tick`, default 10s) is the *only* code path that picks peers to probe. This is intentional — letting discovery enqueue caused ephemeral-port floods where one chatty peer could pin every worker.

A "stub" record has `last_success_ms = 0` and `last_attempt_ms = 0`. The DNS filter rejects stubs (see eligibility filter), and the scheduler treats them as "never succeeded" peers for cadence purposes.

### Two-tier eligibility (matches Go dnsseeder's `isGood`)
`scheduler::is_eligible` decides what to re-probe:
- Past the dead cutoff (`dead_after`, default 24h since last contact): **never** — `prune_dead` will eventually delete it.
- Succeeded at least once: re-probe every `stale_good` (default **15 min**).
- Never succeeded (stub or repeated failures): re-probe every `stale_bad` (default **2 h**).

The DNS handler uses the same `stale_good` window as a filter on `last_success_ms` — so DNS only ever returns peers we've verifiably reached within the last 15 min. Stubs (last_success_ms = 0) are filtered out automatically because `now - 0` is always greater than the window.

### Shutdown responsiveness
`Scheduler::run` selects on a `broadcast::Receiver<()>` plus two tickers. The dispatch path inside `enqueue_probes` **must stay non-blocking** — each probe acquires its own semaphore permit *inside* its spawned task, not in the dispatch loop. If you ever move `semaphore.acquire().await` back into the loop, a saturated worker pool will delay Ctrl+C by up to one full probe-timeout window. The signal handler in `main.rs` arms a one-shot graceful shutdown and force-exits on the second signal.

### Address canonicalization
IPv4-mapped IPv6 addresses (`::ffff:1.2.3.4`) are collapsed to plain IPv4 via `crawler::model::canonicalize_ip` *before* hitting the store. The store key is `(ip, port)` so without this, the same peer can occupy two rows.

### Port policy
`strict_port` (CLI flag, default off) makes both the crawler and the DNS filter reject any address whose port differs from the network's default P2P port. Important on Mainnet to filter out misconfigured nodes; relax for testnets where operators bind alt ports.

### Filter knobs surfaced through the DNS query path
`store::Filter` lets the DNS handler filter on `min_protocol_version`, `min_user_agent` (semver), family (A vs AAAA), default port, and `stale_good_ms`. These come from `DnsConfig`, which is hydrated from CLI flags in `dnsseeder/src/main.rs`. Keep `DnsConfig::new` as the "all-defaults" constructor and use struct-update syntax for CLI overrides — don't multiply constructors.

### `ThreadRng` is `!Send`
Any shuffle/sample needs to be in a scoped block (`{ let mut rng = rand::thread_rng(); ... }`) so the RNG drops before any `.await`. Clippy will catch this but the error message is opaque.

## Conventions

- Edition 2024. `cargo clippy --workspace --all-targets -- -D warnings` must pass; CI mirrors this.
- Tests live in sibling `*_tests.rs` files (e.g. `peer_store.rs` ↔ `peer_store_tests.rs`), not inline `#[cfg(test)] mod tests`.
- JSON over the HTTP API: always `serde(rename_all = "camelCase")`.
- CLI durations: parsed with `humantime` (`--probe-tick 10s`, `--stale-good 15m`).
- Never run `cargo clean` — the rusty-kaspa git deps are expensive to rebuild.
- Comments: only when the *why* is non-obvious. The code is the *what*.

## Quick-start commands

```bash
cargo build                                              # build everything
cargo test --workspace                                   # run all tests
cargo clippy --workspace --all-targets -- -D warnings    # lint gate
cargo run -p simply-kaspa-dnsseeder -- --help            # see flags
```

Reference implementation for behavior questions: `../dnsseeder` (Go). When in doubt about cadence, filter semantics, or bootstrap behavior, check what the Go version does.
