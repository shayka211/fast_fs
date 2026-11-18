#!/usr/bin/env bash
# Build the ucx/ submodule into a local prefix (.ucx/<flavor>).
# Usage: scripts/build-ucx.sh [release|debug]   (default: release)
# Run once (or after bumping the submodule). Cargo never rebuilds UCX itself.
set -euo pipefail

FLAVOR="${1:-release}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/ucx"
PREFIX="$ROOT/.ucx/$FLAVOR"
BUILD="$ROOT/.ucx/build-$FLAVOR"

case "$FLAVOR" in
  release) FLAGS=(--enable-optimizations --disable-logging --disable-debug
                  --disable-assertions --disable-params-check) ;;
  debug)   FLAGS=(--enable-debug --enable-assertions) ;;
  *) echo "unknown flavor: $FLAVOR (release|debug)" >&2; exit 1 ;;
esac

[ -f "$SRC/configure.ac" ] || git -C "$ROOT" submodule update --init ucx
[ -x "$SRC/configure" ] || (cd "$SRC" && ./autogen.sh)

mkdir -p "$BUILD"
cd "$BUILD"
"$SRC/configure" --prefix="$PREFIX" --without-java --without-go \
  --disable-doxygen-doc --without-cuda --without-rocm "${FLAGS[@]}"
make -j"$(nproc)"
make install

echo
echo "UCX ($FLAVOR) installed to $PREFIX"
"$PREFIX/bin/ucx_info" -v | head -3
