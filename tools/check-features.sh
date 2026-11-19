#!/bin/sh
set -eu
cd "$(dirname "$0")/.."

cargo check --no-default-features --lib
cargo check --no-default-features --features redis-storage --lib
cargo check --no-default-features --features metrics-http1 --lib
cargo check --no-default-features --features quic-transport --lib
cargo check --no-default-features --features quic-transport,redis-storage --lib
cargo check --no-default-features --features server-example,redis-storage --example backend
cargo check --no-default-features --features multi-transport-example,redis-storage --example multi_transport

if cargo tree --no-default-features -e features | grep -q 'json-rpc-2-runtime feature "http1"'; then
    echo "HTTP/1 runtime feature unexpectedly enabled without default features" >&2
    exit 1
fi

echo "feature checks passed: cooperative memory/Redis, opt-in HTTP/1, and both hosts compile"
