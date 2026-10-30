#!/usr/bin/env bash

set -Eeuo pipefail

TOOLS_DIR=${TOOLS_DIR:-/home/panlei/benchmark-tools}
RESULT_ROOT=${RESULT_ROOT:-/home/panlei/redis-dragonfly-results/formal-20260824}
SERVER_HOST=${SERVER_HOST:-10.47.76.7}
PORT=${PORT:-6380}

RB="$TOOLS_DIR/redis-benchmark"
CLI="$TOOLS_DIR/redis-cli"
MEMTIER="$TOOLS_DIR/memtier_benchmark"
COLLECTORS=()

ulimit -n 65535

stop_collectors() {
  if ((${#COLLECTORS[@]})); then
    kill "${COLLECTORS[@]}" 2>/dev/null || true
    wait "${COLLECTORS[@]}" 2>/dev/null || true
  fi
  COLLECTORS=()
}

trap stop_collectors EXIT INT TERM

start_collectors() {
  local out=$1
  mpstat -P ALL 1 >"$out/client-mpstat.txt" &
  COLLECTORS+=("$!")
  vmstat -w 1 >"$out/client-vmstat.txt" &
  COLLECTORS+=("$!")
  sar -n DEV,EDEV,TCP,ETCP 1 >"$out/client-sar-network.txt" &
  COLLECTORS+=("$!")
}

memtier_string() {
  local rel=$1 clients=$2 pipeline=$3 seconds=$4 keys=$5
  local out="$RESULT_ROOT/$rel" threads per_thread
  mkdir -p "$out"
  if [[ $clients == 1000 ]]; then
    threads=8
    per_thread=125
  elif [[ $clients == 2000 ]]; then
    threads=8
    per_thread=250
  else
    printf 'unsupported client count: %s\n' "$clients" >&2
    return 1
  fi
  start_collectors "$out"
  "$MEMTIER" \
    --server="$SERVER_HOST" --port="$PORT" --protocol=redis \
    --threads="$threads" --clients="$per_thread" \
    --test-time="$seconds" --run-count=1 --pipeline="$pipeline" \
    --ratio=1:9 --key-minimum=1 --key-maximum="$keys" \
    --key-pattern=R:R --data-size=128 --random-data \
    --print-percentiles=50,95,99,99.9 --hide-histogram \
    --out-file="$out/memtier.txt" --json-out-file="$out/memtier.json" \
    >"$out/memtier.stdout" 2>&1
  stop_collectors
}

memtier_transaction() {
  local rel=$1 seconds=$2
  local out="$RESULT_ROOT/$rel"
  mkdir -p "$out"
  start_collectors "$out"
  "$MEMTIER" \
    --server="$SERVER_HOST" --port="$PORT" --protocol=redis \
    --threads=8 --clients=125 --test-time="$seconds" --run-count=1 \
    --pipeline=1 --transaction --data-size=128 --random-data \
    --key-minimum=1 --key-maximum=1000000 \
    --command='MULTI' \
    --command='SET tx:__key__ __data__' --command-key-pattern=R \
    --command='GET tx:__key__' --command-key-pattern=R \
    --command='EXEC' \
    --print-percentiles=50,95,99,99.9 --hide-histogram \
    --out-file="$out/memtier.txt" --json-out-file="$out/memtier.json" \
    >"$out/memtier.stdout" 2>&1
  stop_collectors
}

preload_string() {
  local rel=$1 keys=$2
  local out="$RESULT_ROOT/$rel" requests_per_client=$((keys / 100))
  mkdir -p "$out"
  "$MEMTIER" \
    --server="$SERVER_HOST" --port="$PORT" --protocol=redis \
    --threads=4 --clients=25 --requests="$requests_per_client" \
    --ratio=1:0 --pipeline=16 \
    --key-minimum=1 --key-maximum="$keys" --key-pattern=P:P \
    --data-size=128 --random-data --hide-histogram \
    --out-file="$out/preload.txt" --json-out-file="$out/preload.json" \
    >"$out/preload.stdout" 2>&1
}

correctness() {
  local rel=$1 out="$RESULT_ROOT/$1"
  mkdir -p "$out"
  {
    "$CLI" -h "$SERVER_HOST" -p "$PORT" ping
    "$CLI" -h "$SERVER_HOST" -p "$PORT" set check:string value
    "$CLI" -h "$SERVER_HOST" -p "$PORT" get check:string
    "$CLI" -h "$SERVER_HOST" -p "$PORT" hset check:hash field value
    "$CLI" -h "$SERVER_HOST" -p "$PORT" hget check:hash field
    "$CLI" -h "$SERVER_HOST" -p "$PORT" lpush check:list value
    "$CLI" -h "$SERVER_HOST" -p "$PORT" lpop check:list
    "$CLI" -h "$SERVER_HOST" -p "$PORT" sadd check:set member
    "$CLI" -h "$SERVER_HOST" -p "$PORT" sismember check:set member
    "$CLI" -h "$SERVER_HOST" -p "$PORT" zadd check:zset 1 member
    "$CLI" -h "$SERVER_HOST" -p "$PORT" zrange check:zset 0 0
    printf 'MULTI\nSET check:tx value\nGET check:tx\nEXEC\n' | \
      "$CLI" -h "$SERVER_HOST" -p "$PORT" --no-raw
  } >"$out/client-correctness.txt"
}

rb_preload() {
  local rel=$1 keyspace=$2 requests=$3
  shift 3
  local out="$RESULT_ROOT/$rel"
  mkdir -p "$out"
  "$RB" -h "$SERVER_HOST" -p "$PORT" --csv \
    -n "$requests" -c 100 --threads 4 -P 64 -r "$keyspace" \
    "$@" >"$out/redis-benchmark.csv" 2>"$out/redis-benchmark.stderr"
}

rb_case() {
  local rel=$1 pipeline=$2 keyspace=$3 requests=$4
  shift 4
  local out="$RESULT_ROOT/$rel"
  mkdir -p "$out"
  start_collectors "$out"
  "$RB" -h "$SERVER_HOST" -p "$PORT" --csv \
    -n "$requests" -c 1000 --threads 8 -P "$pipeline" -r "$keyspace" \
    "$@" >"$out/redis-benchmark.csv" 2>"$out/redis-benchmark.stderr"
  stop_collectors
}

rb_builtin() {
  local rel=$1 pipeline=$2 test=$3 requests=$4 keys=$5
  local out="$RESULT_ROOT/$rel"
  mkdir -p "$out"
  start_collectors "$out"
  "$RB" -h "$SERVER_HOST" -p "$PORT" --csv \
    -n "$requests" -c 1000 --threads 8 -P "$pipeline" \
    -r "$keys" -d 128 -t "$test" \
    >"$out/redis-benchmark.csv" 2>"$out/redis-benchmark.stderr"
  stop_collectors
}

write_environment() {
  mkdir -p "$RESULT_ROOT"
  {
    date --iso-8601=seconds
    hostname
    uname -a
    lscpu
    lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE
    free -h
    swapon --show
    ip -4 -br addr
    ip route
    ulimit -n
    "$RB" --version
    "$CLI" --version
    "$MEMTIER" --version || true
    sha256sum "$RB" "$CLI" "$MEMTIER"
  } >"$RESULT_ROOT/client-environment.txt" 2>&1
}

case ${1:-} in
  memtier-string) shift; memtier_string "$@" ;;
  memtier-transaction) shift; memtier_transaction "$@" ;;
  preload-string) shift; preload_string "$@" ;;
  correctness) shift; correctness "$@" ;;
  rb-preload) shift; rb_preload "$@" ;;
  rb-case) shift; rb_case "$@" ;;
  rb-builtin) shift; rb_builtin "$@" ;;
  environment) write_environment ;;
  *) printf 'unknown action: %s\n' "${1:-}" >&2; exit 2 ;;
esac
