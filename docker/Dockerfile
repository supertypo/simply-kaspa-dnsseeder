##
# builder image
##
ARG RUST_VERSION
FROM rust:${RUST_VERSION}-alpine AS builder

ARG REPO_DIR=.

RUN apk --no-cache add \
  musl-dev \
  g++ \
  linux-headers \
  git

WORKDIR /work

ENV RUSTFLAGS="-C target-feature=-crt-static -C link-args=-s" \
  CARGO_REGISTRIES_CRATES_IO_PROTOCOL="sparse"

# Cache dependencies
COPY "$REPO_DIR"/Cargo.toml "$REPO_DIR"/Cargo.lock "$REPO_DIR"/rust-toolchain.toml ./
COPY "$REPO_DIR"/cli/Cargo.toml        ./cli/
COPY "$REPO_DIR"/common/Cargo.toml     ./common/
COPY "$REPO_DIR"/store/Cargo.toml      ./store/
COPY "$REPO_DIR"/crawler/Cargo.toml    ./crawler/
COPY "$REPO_DIR"/dns/Cargo.toml        ./dns/
COPY "$REPO_DIR"/web/Cargo.toml        ./web/
COPY "$REPO_DIR"/dnsseeder/Cargo.toml  ./dnsseeder/
RUN mkdir -p cli/src common/src store/src crawler/src dns/src web/src dnsseeder/src && \
    echo "fn main() {}" > cli/build.rs && \
    echo "pub fn placeholder() {}" > cli/src/lib.rs && \
    echo "pub fn placeholder() {}" > common/src/lib.rs && \
    echo "pub fn placeholder() {}" > store/src/lib.rs && \
    echo "pub fn placeholder() {}" > crawler/src/lib.rs && \
    echo "pub fn placeholder() {}" > dns/src/lib.rs && \
    echo "pub fn placeholder() {}" > web/src/lib.rs && \
    echo "fn main() {}" > dnsseeder/src/main.rs

RUN cargo fetch
RUN cargo build --release

COPY "$REPO_DIR" .
RUN find . -type f \( -name build.rs -or -name lib.rs -or -name main.rs \) -exec touch {} +
RUN cargo build --release


##
# runtime image
##
FROM alpine

RUN apk --no-cache add \
  libstdc++

COPY --from=builder /work/target/release/simply-kaspa-dnsseeder /usr/local/bin/

USER 13337:13337

ARG VERSION
ENV VERSION=$VERSION

ENTRYPOINT ["simply-kaspa-dnsseeder"]
