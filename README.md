# simply-kaspa-dnsseeder

A DNS seeder for the [Kaspa](https://kaspa.org) network, written in Rust.

It continuously crawls reachable Kaspa nodes, stores the good ones, and answers `A` / `AAAA` queries on a domain you control so fresh nodes can bootstrap into the network without hard-coded peer lists.

A Rust reimplementation of the venerable Go [`dnsseeder`](https://github.com/kaspanet/dnsseeder), with a small HTTP API on the side for ad-hoc peer submissions and introspection.

## Build

Requires Rust 1.93+ (see `rust-toolchain.toml`).

```bash
cargo build --release
```

The binary lands at `target/release/simply-kaspa-dnsseeder`.

## Run

You need two pieces of DNS infrastructure in place before starting:

1. A zone `NS` record that delegates a subdomain (e.g. `n-testnet-10.mydomain.org`) to the host running this seeder.
2. A glue/`A` record for the nameserver name (e.g. `ns-testnet-10.mydomain.org`) pointing at the seeder's public IP.

### Minimum configuration: public seeder with API key

```bash
simply-kaspa-dnsseeder \
  --network-id kaspa-testnet-10 \
  --dns-zone n-testnet-10.mydomain.org \
  --dns-nameserver ns-testnet-10.mydomain.org \
  --dns-listen 0.0.0.0:53 \
  --http-listen 0.0.0.0:8080 \
  --api-key "$(openssl rand -hex 32)"
```

That's the whole minimum: network, zone, nameserver FQDN, where to listen for DNS and HTTP, and an API key to protect the write side of the HTTP endpoint.

- DNS server activates only when both `--dns-zone` and `--dns-nameserver` are set.
- Binding port 53 typically needs `sudo` or `CAP_NET_BIND_SERVICE`.
- With `--api-key` set, `POST /peers` requires the key (`Authorization: Bearer <key>` or `X-Api-Key: <key>`) and `GET /peers` only includes the raw `ip` field when the request is authenticated.

### Mainnet example

```bash
simply-kaspa-dnsseeder \
  --network-id kaspa-mainnet \
  --dns-zone seed.mydomain.org \
  --dns-nameserver ns.mydomain.org \
  --strict-port \
  --api-key "$(cat /etc/seeder/api.key)"
```

`--strict-port` is recommended on mainnet to filter out nodes listening on non-default ports.

### Useful tuning flags

| Flag | Default | Purpose |
| --- | --- | --- |
| `--threads` | `8` | Concurrent probe workers |
| `--probe-tick` | `10s` | How often the crawler scans for eligible peers |
| `--stale-good` | `15m` | Re-probe interval for known-good peers (and DNS freshness window) |
| `--stale-bad` | `2h` | Re-probe interval for peers that have never succeeded |
| `--dead-after` | `7d` | Peers not seen for this long are pruned |
| `--min-protocol-version` | _unset_ | Filter DNS answers by minimum protocol version |
| `--min-user-agent` | _unset_ | Filter DNS answers by minimum kaspad semver (e.g. `1.1.0`) |
| `--datadir` | `data` | Persistent storage directory |

Run `simply-kaspa-dnsseeder --help` for the full list.

## License

MIT. See [LICENSE](LICENSE).
