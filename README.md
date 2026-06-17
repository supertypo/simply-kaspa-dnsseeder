# simply-kaspa-dnsseeder

A DNS seeder for the [Kaspa](https://kaspa.org) network, written in Rust.

It continuously crawls reachable Kaspa nodes, stores the good ones, and answers `A` / `AAAA` queries on a domain you control so fresh nodes can bootstrap into the network without hard-coded peer lists.

A small HTTP API on the side allows ad-hoc peer submissions and introspection.

## Build

### Prerequisites

- **Rust toolchain** pinned in [`rust-toolchain.toml`](rust-toolchain.toml) — installed automatically by `rustup`.
- **Protocol Buffers compiler** (`protoc`) — required by the rusty-kaspa P2P dependency.
  - macOS: `brew install protobuf`
  - Debian/Ubuntu: `apt install protobuf-compiler`
  - Alpine: `apk add protoc protobuf-dev`

```bash
cargo build --release
```

The binary lands at `target/release/simply-kaspa-dnsseeder`.

## Install

### Pre-built binaries

Each [GitHub release](https://github.com/supertypo/simply-kaspa-dnsseeder/releases) ships compressed binaries for Linux:

```bash
# AMD64
curl -L https://github.com/supertypo/simply-kaspa-dnsseeder/releases/latest/download/simply-kaspa-dnsseeder-amd64.gz \
  | gunzip > simply-kaspa-dnsseeder && chmod +x simply-kaspa-dnsseeder

# ARM64
curl -L https://github.com/supertypo/simply-kaspa-dnsseeder/releases/latest/download/simply-kaspa-dnsseeder-arm64.gz \
  | gunzip > simply-kaspa-dnsseeder && chmod +x simply-kaspa-dnsseeder
```

### Docker

A Docker image can be built from the included [`docker/Dockerfile`](docker/Dockerfile):

```bash
# Build image
docker/build.sh nopush dev
```

Run with a persistent data volume:

```bash
docker run -d \
  --network host \
  -v /srv/seeder/data:/data \
  supertypo/simply-kaspa-dnsseeder \
  --dns-zone seed.mydomain.org \
  --dns-nameserver ns.mydomain.org
```

## Run

### Minimum: pure crawler (no arguments)

```bash
simply-kaspa-dnsseeder
```

With no arguments the seeder:

- Crawls **mainnet** using the built-in bootstrap DNS seeders.
- Persists discovered peers in `./data/mainnet/peers.redb`.
- Runs the HTTP API on `0.0.0.0:5380` and `[::]:5380`.
- DNS is **disabled** (no zone configured).
- Generates a **persistent API key** on first startup, stores it in the peer database, and reuses it on subsequent restarts. Pass `--api-key` to use a specific key instead.

This mode is useful for maintaining a local peer database, feeding a custom tool, or just exploring what's on the network.

### Minimum: DNS seeder mode

You need two pieces of DNS infrastructure in place before starting:

1. A zone `NS` record that delegates a subdomain (e.g. `n-testnet-10.mydomain.org`) to the host running this seeder.
2. A glue/`A` record for the nameserver name (e.g. `ns-testnet-10.mydomain.org`) pointing at the seeder's public IP.

```bash
simply-kaspa-dnsseeder \
  --dns-zone seed.mydomain.org \
  --dns-nameserver ns.mydomain.org
```

- DNS is enabled as soon as both `--dns-zone` and `--dns-nameserver` are set.
- The DNS server listens on `0.0.0.0:53` and `[::]:53` by default. Binding port 53 typically needs `sudo` or `CAP_NET_BIND_SERVICE`.
- The HTTP API listens on `0.0.0.0:5380` and `[::]:5380` by default.

### HTTP endpoints

All endpoints are served under `--api-prefix` (default `/api`). Swagger UI is available at `http://host:5380/api`; pass `--api-prefix ""` to serve at the root (Swagger moves to `/swagger`).

| Endpoint | Method | Auth | Description |
| --- | --- | --- | --- |
| `/api/health` | GET | — | `200 OK` while at least one peer succeeded inside `--stale-good`, otherwise `503` |
| `/api/metrics` | GET | — | JSON dump: process (cpu/mem), disk usage, peer-store summary, per-subsystem counters |
| `/api/peers` | GET | — | All peers sorted by most-recent success first. `ip` field is omitted unless authenticated |
| `/api/peers` | POST | required | JSON body `{ "addrPort": "ip:port" }`; probes the peer and stores it on success (rate-limited per source IP) |
| `/api/peers/{addr_port}` | GET | required | Single peer lookup. IPv6 must be bracketed: `[::1]:port` |
| `/api/peers/{addr_port}` | DELETE | required | Remove a peer from the store. Returns `204` on success, `404` if absent |

Note: "required" endpoints and the `ip` field on `GET /api/peers` require the `X-API-KEY: <key>` request header.

### Useful tuning flags

| Flag | Default | Purpose |
| --- | --- | --- |
| `--threads` | `8` | Concurrent probe workers |
| `--probes-per-peer` | `3` | Back-to-back `RequestAddresses` rounds per healthy probe |
| `--probe-tick` | `5s` | How often the crawler scans for eligible peers |
| `--stale-good` | `30m` | Re-probe interval for known-good peers (and DNS freshness window) |
| `--stale-bad` | `2h` | Re-probe interval for peers that have never succeeded |
| `--dead-after` | `7d` | Peers not seen for this long are pruned |
| `--strict-port` | off | Reject addresses whose port differs from the network default |
| `--min-protocol-version` | — | Filter DNS answers by minimum protocol version |
| `--min-user-agent` | — | Filter DNS answers by minimum kaspad semver (e.g. `1.1.0`) |
| `--datadir` | `data` | Persistent storage directory |
| `--api-prefix` | `/api` | URL prefix for HTTP endpoints (`""` serves at root) |
| `--stats-interval` | `1m` | Periodic in-process stats dump cadence; `0s` disables |
| `--log-level` | `warn,...=info` | `env_logger` filter string |

Run `simply-kaspa-dnsseeder --help` for the full list and current defaults.

## TLS / HTTPS

The HTTP server can serve over TLS by passing both `--tls-cert` and `--tls-key`
(PEM files). When set, `--http-listen` accepts HTTPS instead of HTTP; supplying
only one of the two is a startup error.

```bash
simply-kaspa-dnsseeder \
  --dns-zone seed.mydomain.org \
  --dns-nameserver ns.mydomain.org \
  --tls-cert /etc/letsencrypt/live/seed.mydomain.org/fullchain.pem \
  --tls-key  /etc/letsencrypt/live/seed.mydomain.org/privkey.pem
```

Notes:

- The cert file may be a single certificate or a full chain (`fullchain.pem`).
- The key may be PKCS8 or PKCS1 PEM. Encrypted keys are not supported.
- Cert reload requires a process restart.
- For production, terminating TLS at a reverse proxy (nginx, Caddy, Traefik) is
  also a perfectly good option and avoids shipping the key alongside the binary.

## License

MIT. See [LICENSE](LICENSE).
