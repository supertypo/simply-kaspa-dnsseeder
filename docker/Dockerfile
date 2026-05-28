##
# builder image
##
FROM rust:1.93.1-alpine AS builder

ARG REPO_DIR=.

RUN apk --no-cache add \
  musl-dev \
  g++ \
  clang18-dev \
  linux-headers \
  openssl-dev \
  postgresql-dev \
  curl

WORKDIR /work

ENV RUSTFLAGS="-C target-feature=-crt-static -C link-args=-s" \
  CARGO_REGISTRIES_CRATES_IO_PROTOCOL="sparse"

# Cache dependencies
COPY "$REPO_DIR"/Cargo.toml "$REPO_DIR"/Cargo.lock ./
COPY "$REPO_DIR"/cli/Cargo.toml ./cli/
COPY "$REPO_DIR"/kaspad/Cargo.toml ./kaspad/
COPY "$REPO_DIR"/socket-server/Cargo.toml ./socket-server/
RUN mkdir cli/src kaspad/src socket-server/src && \
    echo "fn main() {}" > cli/build.rs && \
    echo "pub fn placeholder() {}" > cli/src/lib.rs && \
    echo "pub fn placeholder() {}" > kaspad/src/lib.rs && \
    echo "pub fn placeholder() {}" > socket-server/src/lib.rs && \
    echo "fn main() {}" > socket-server/build.rs && \
    echo "fn main() {}" > socket-server/src/main.rs

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
  libstdc++ \
  postgresql-client

COPY --from=builder /work/target/release/simply-kaspa-socket-server /usr/local/bin/

USER 13337:13337

ARG VERSION
ENV VERSION=$VERSION

ENTRYPOINT ["simply-kaspa-socket-server"]
