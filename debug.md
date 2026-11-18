# Levels: error/warn/info (default)/debug/trace — trace logs every message sent and received
RUST_LOG=debug ./target/release/pingpong client
RUST_LOG=info,ucx=trace ./target/release/pingpong server     # per-message, UCX layer only

# Formats and per-process files (separate logs when running many nodes)
FASTFS_LOG_FORMAT=pretty ...      # multi-line, shows file:line
FASTFS_LOG_DIR=/tmp/logs ...      # also writes /tmp/logs/<process>.log as JSON (use jq)

# UCX's own internal logs
UCX_LOG_LEVEL=debug ...           # works with the release build
UCX_PROTO_INFO=y ...              # which transport/protocol UCX picks per message size
scripts/build-ucx.sh debug        # build once, for UCX_LOG_LEVEL=trace and UCX's internal checks
LD_LIBRARY_PATH=$PWD/.ucx/debug/lib UCX_LOG_LEVEL=trace ./target/release/pingpong ...
