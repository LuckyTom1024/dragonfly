#!/usr/bin/env bash

set -Eeuo pipefail

REDIS_DIR=${REDIS_DIR:-/home/redis/redis}
DRAGONFLY_DIR=${DRAGONFLY_DIR:-/home/Dragonfly/dragonfly}
MEMTIER=${MEMTIER:-/tmp/memtier_benchmark/memtier_benchmark}
RESULT_ROOT=${RESULT_ROOT:-/tmp/dragonfly-bench/formal}
PORT=${PORT:-6380}
MAXMEMORY=${MAXMEMORY:-8G}
TEST_TIME=${TEST_TIME:-60}
WARMUP_TIME=${WARMUP_TIME:-30}
STRING_KEYS=${STRING_KEYS:-1000000}
STRUCTURE_KEYS=${STRUCTURE_KEYS:-100000}
STRUCTURE_PRELOAD_REQUESTS=${STRUCTURE_PRELOAD_REQUESTS:-1000000}
RB_REQUESTS=${RB_REQUESTS:-10000000}
CLIENTS_1000_THREADS=${CLIENTS_1000_THREADS:-8}
CLIENTS_1000_PER_THREAD=${CLIENTS_1000_PER_THREAD:-125}
CLIENTS_2000_THREADS=${CLIENTS_2000_THREADS:-8}
CLIENTS_2000_PER_THREAD=${CLIENTS_2000_PER_THREAD:-250}
DRAGONFLY_RUNTIME=${DRAGONFLY_RUNTIME:-/home/cdm/cdm-build-tools/cdm-tools-x86_64-linux/gcc-14.2.0/lib64}

RB="$REDIS_DIR/src/redis-benchmark"
CLI="$REDIS_DIR/src/redis-cli"
REDIS_SERVER="$REDIS_DIR/src/redis-server"
DRAGONFLY_SERVER="$DRAGONFLY_DIR/build-opt/dragonfly"

SERVER_PID=
COLLECTOR_PIDS=()

log() {
  printf '[%(%FT%T%z)T] %s\n' -1 "$*"
}

server_cpus() {
  case "$1" in
    8) printf '%s\n' '0,2,4,6,8,10,12,14' ;;
    16) printf '%s\n' '0-15' ;;
    *) log "unsupported server thread count: $1"; return 1 ;;
  esac
}

stop_collectors() {
  if ((${#COLLECTOR_PIDS[@]})); then
    kill "${COLLECTOR_PIDS[@]}" 2>/dev/null || true
    wait "${COLLECTOR_PIDS[@]}" 2>/dev/null || true
  fi
  COLLECTOR_PIDS=()
}

stop_server() {
  stop_collectors
  if [[ -n ${SERVER_PID:-} ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -INT "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  SERVER_PID=
}

trap stop_server EXIT INT TERM

wait_ready() {
  for _ in $(seq 1 300); do
    if "$CLI" -h 127.0.0.1 -p "$PORT" ping >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      log 'server exited before becoming ready'
      return 1
    fi
    sleep 0.1
  done
  log 'server readiness timeout'
  return 1
}

start_server() {
  local product=$1
  local threads=$2
  local out=$3
  local cpus
  cpus=$(server_cpus "$threads")
  mkdir -p "$out"

  if ss -ltn | rg -q ":${PORT} "; then
    log "port $PORT is already in use"
    return 1
  fi

  if [[ $product == redis ]]; then
    numactl --physcpubind="$cpus" --localalloc \
      "$REDIS_SERVER" \
      --bind 127.0.0.1 \
      --port "$PORT" \
      --protected-mode no \
      --save '' \
      --appendonly no \
      --maxmemory "$MAXMEMORY" \
      --maxmemory-policy noeviction \
      --io-threads "$threads" \
      --daemonize no >"$out/server.log" 2>&1 &
  else
    numactl --physcpubind="$cpus" --localalloc \
      env LD_LIBRARY_PATH="$DRAGONFLY_RUNTIME" \
      "$DRAGONFLY_SERVER" \
      --bind=127.0.0.1 \
      --port="$PORT" \
      --proactor_threads="$threads" \
      --num_shards="$threads" \
      --maxmemory="$MAXMEMORY" \
      --cache_mode=false \
      --dbfilename= \
      --alsologtostderr >"$out/server.log" 2>&1 &
  fi
  SERVER_PID=$!
  wait_ready

  taskset -pc "$SERVER_PID" >"$out/server-affinity.txt"
  "$CLI" -h 127.0.0.1 -p "$PORT" info all >"$out/info-start.txt"
  log "started $product threads=$threads pid=$SERVER_PID cpus=$cpus"
}

start_collectors() {
  local out=$1
  pidstat -h -u -r -w -t -p "$SERVER_PID" 1 >"$out/pidstat.txt" &
  COLLECTOR_PIDS+=("$!")
  mpstat -P ALL 1 >"$out/mpstat.txt" &
  COLLECTOR_PIDS+=("$!")
  vmstat -w 1 >"$out/vmstat.txt" &
  COLLECTOR_PIDS+=("$!")
  sar -n DEV,EDEV,TCP,ETCP 1 >"$out/sar-network.txt" &
  COLLECTOR_PIDS+=("$!")
}

save_case_state() {
  local out=$1
  "$CLI" -h 127.0.0.1 -p "$PORT" info all >"$out/info-after.txt"
  sed -n '1,80p' "/proc/$SERVER_PID/status" >"$out/proc-status.txt"
  sed -n '1,120p' "/proc/$SERVER_PID/smaps_rollup" >"$out/smaps-rollup.txt"
}

preload_strings() {
  local out=$1
  local requests_per_client=$((STRING_KEYS / 100))
  "$MEMTIER" \
    --server=127.0.0.1 --port="$PORT" --protocol=redis \
    --threads=4 --clients=25 --requests="$requests_per_client" \
    --ratio=1:0 --pipeline=16 \
    --key-minimum=1 --key-maximum="$STRING_KEYS" --key-pattern=P:P \
    --data-size=128 --random-data --hide-histogram \
    --out-file="$out/preload.txt" --json-out-file="$out/preload.json" \
    >"$out/preload.stdout" 2>&1

  local dbsize
  dbsize=$("$CLI" -h 127.0.0.1 -p "$PORT" dbsize)
  printf '%s\n' "$dbsize" >"$out/dbsize-after-preload.txt"
  if [[ $dbsize != "$STRING_KEYS" ]]; then
    log "preload failed: expected $STRING_KEYS keys, got $dbsize"
    return 1
  fi
}

run_memtier_string() {
  local out=$1
  local clients=$2
  local pipeline=$3
  local seconds=$4
  local threads per_thread
  if [[ $clients == 1000 ]]; then
    threads=$CLIENTS_1000_THREADS
    per_thread=$CLIENTS_1000_PER_THREAD
  else
    threads=$CLIENTS_2000_THREADS
    per_thread=$CLIENTS_2000_PER_THREAD
  fi
  mkdir -p "$out"
  "$CLI" -h 127.0.0.1 -p "$PORT" info all >"$out/info-before.txt"
  start_collectors "$out"
  "$MEMTIER" \
    --server=127.0.0.1 --port="$PORT" --protocol=redis \
    --threads="$threads" --clients="$per_thread" \
    --test-time="$seconds" --run-count=1 --pipeline="$pipeline" \
    --ratio=1:9 --key-minimum=1 --key-maximum="$STRING_KEYS" \
    --key-pattern=R:R --data-size=128 --random-data \
    --print-percentiles=50,95,99,99.9 --hide-histogram \
    --out-file="$out/memtier.txt" --json-out-file="$out/memtier.json" \
    >"$out/memtier.stdout" 2>&1
  stop_collectors
  save_case_state "$out"
}

run_memtier_transaction() {
  local out=$1
  mkdir -p "$out"
  "$CLI" -h 127.0.0.1 -p "$PORT" info all >"$out/info-before.txt"
  start_collectors "$out"
  "$MEMTIER" \
    --server=127.0.0.1 --port="$PORT" --protocol=redis \
    --threads=8 --clients=125 --test-time="$TEST_TIME" --run-count=1 \
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
  save_case_state "$out"
}

correctness_check() {
  local out=$1
  {
    "$CLI" -h 127.0.0.1 -p "$PORT" ping
    "$CLI" -h 127.0.0.1 -p "$PORT" set check:string value
    "$CLI" -h 127.0.0.1 -p "$PORT" get check:string
    "$CLI" -h 127.0.0.1 -p "$PORT" hset check:hash field value
    "$CLI" -h 127.0.0.1 -p "$PORT" hget check:hash field
    "$CLI" -h 127.0.0.1 -p "$PORT" lpush check:list value
    "$CLI" -h 127.0.0.1 -p "$PORT" lpop check:list
    "$CLI" -h 127.0.0.1 -p "$PORT" sadd check:set member
    "$CLI" -h 127.0.0.1 -p "$PORT" sismember check:set member
    "$CLI" -h 127.0.0.1 -p "$PORT" zadd check:zset 1 member
    "$CLI" -h 127.0.0.1 -p "$PORT" zrange check:zset 0 0
    printf 'MULTI\nSET check:tx value\nGET check:tx\nEXEC\n' | \
      "$CLI" -h 127.0.0.1 -p "$PORT" --no-raw
  } >"$out/correctness.txt"
  "$CLI" -h 127.0.0.1 -p "$PORT" flushall >/dev/null
}

run_core_config() {
  local product=$1
  local threads=$2
  local config_dir="$RESULT_ROOT/core/${product}-t${threads}"
  start_server "$product" "$threads" "$config_dir"
  correctness_check "$config_dir"
  preload_strings "$config_dir"

  log "warmup $product t$threads ${WARMUP_TIME}s"
  run_memtier_string "$config_dir/warmup-c1000-p1" 1000 1 "$WARMUP_TIME"

  local clients pipeline case_dir
  for clients in 1000; do
    for pipeline in 1 16 64; do
      case_dir="$config_dir/string-c${clients}-p${pipeline}-60s"
      log "formal start $product t$threads c$clients p$pipeline ${TEST_TIME}s"
      run_memtier_string "$case_dir" "$clients" "$pipeline" "$TEST_TIME"
      rg '^Totals' "$case_dir/memtier.txt" || true
    done
  done

  case_dir="$config_dir/string-c2000-p1-60s"
  log "formal start $product t$threads c2000 p1 ${TEST_TIME}s"
  run_memtier_string "$case_dir" 2000 1 "$TEST_TIME"
  rg '^Totals' "$case_dir/memtier.txt" || true

  case_dir="$config_dir/tx1-c1000-p1-60s"
  log "transaction start $product t$threads c1000 p1 ${TEST_TIME}s"
  run_memtier_transaction "$case_dir"
  rg '^Totals' "$case_dir/memtier.txt" || true

  "$CLI" -h 127.0.0.1 -p "$PORT" info all >"$config_dir/info-end.txt"
  stop_server
}

run_rb_preload() {
  local out=$1
  local keyspace=$2
  shift 2
  mkdir -p "$(dirname "$out")"
  "$RB" -h 127.0.0.1 -p "$PORT" --csv \
    -n "$STRUCTURE_PRELOAD_REQUESTS" -c 100 --threads 4 -P 64 -r "$keyspace" \
    "$@" >"$out" 2>"${out%.csv}.stderr"
}

preload_structures() {
  local out=$1
  local value=$2
  local idx member score
  mkdir -p "$out/preload"
  preload_strings "$out"

  for idx in $(seq 0 9); do
    printf -v member 'field%06d' "$idx"
    run_rb_preload "$out/preload/hash-$idx.csv" "$STRUCTURE_KEYS" \
      hset 'h:__rand_int__' "$member" "$value"
  done

  run_rb_preload "$out/preload/list.csv" "$STRUCTURE_KEYS" \
    lpush 'l:__rand_int__' "$value"

  for idx in $(seq 0 9); do
    printf -v member 'member%06d' "$idx"
    run_rb_preload "$out/preload/set-$idx.csv" "$STRUCTURE_KEYS" \
      sadd 's:__rand_int__' "$member"
    score=$idx
    run_rb_preload "$out/preload/zset-$idx.csv" "$STRUCTURE_KEYS" \
      zadd 'z:__rand_int__' "$score" "$member"
  done

  "$CLI" -h 127.0.0.1 -p "$PORT" dbsize >"$out/dbsize-after-structure-preload.txt"
  {
    "$CLI" -h 127.0.0.1 -p "$PORT" hlen h:000000000001
    "$CLI" -h 127.0.0.1 -p "$PORT" llen l:000000000001
    "$CLI" -h 127.0.0.1 -p "$PORT" scard s:000000000001
    "$CLI" -h 127.0.0.1 -p "$PORT" zcard z:000000000001
  } >"$out/structure-sample-cardinality.txt"
}

run_rb_case() {
  local out=$1
  local pipeline=$2
  local keyspace=$3
  shift 3
  mkdir -p "$out"
  "$CLI" -h 127.0.0.1 -p "$PORT" info all >"$out/info-before.txt"
  start_collectors "$out"
  "$RB" -h 127.0.0.1 -p "$PORT" --csv \
    -n "$RB_REQUESTS" -c 1000 --threads 8 -P "$pipeline" -r "$keyspace" \
    "$@" >"$out/redis-benchmark.csv" 2>"$out/redis-benchmark.stderr"
  stop_collectors
  save_case_state "$out"
  if ! tail -n 1 "$out/redis-benchmark.csv" | rg -q '^[^,]+,'; then
    log "redis-benchmark did not produce a CSV result: $out"
    return 1
  fi
}

run_rb_builtin_case() {
  local out=$1
  local pipeline=$2
  local test=$3
  mkdir -p "$out"
  "$CLI" -h 127.0.0.1 -p "$PORT" info all >"$out/info-before.txt"
  start_collectors "$out"
  "$RB" -h 127.0.0.1 -p "$PORT" --csv \
    -n "$RB_REQUESTS" -c 1000 --threads 8 -P "$pipeline" \
    -r "$STRING_KEYS" -d 128 -t "$test" \
    >"$out/redis-benchmark.csv" 2>"$out/redis-benchmark.stderr"
  stop_collectors
  save_case_state "$out"
  if ! tail -n 1 "$out/redis-benchmark.csv" | rg -q '^[^,]+,'; then
    log "redis-benchmark did not produce a CSV result: $out"
    return 1
  fi
}

run_structure_config() {
  local product=$1
  local threads=$2
  local config_dir="$RESULT_ROOT/structures/${product}-t${threads}"
  local value pipeline case_dir
  printf -v value '%0128d' 0

  start_server "$product" "$threads" "$config_dir"
  correctness_check "$config_dir"
  preload_structures "$config_dir" "$value"

  for pipeline in 1 16 64; do
    log "rb start $product t$threads SET p$pipeline requests=$RB_REQUESTS"
    case_dir="$config_dir/string-set-p${pipeline}-10m"
    run_rb_builtin_case "$case_dir" "$pipeline" set
    tail -n 1 "$case_dir/redis-benchmark.csv"

    log "rb start $product t$threads GET p$pipeline requests=$RB_REQUESTS"
    case_dir="$config_dir/string-get-p${pipeline}-10m"
    run_rb_builtin_case "$case_dir" "$pipeline" get
    tail -n 1 "$case_dir/redis-benchmark.csv"

    log "rb start $product t$threads HGET p$pipeline requests=$RB_REQUESTS"
    case_dir="$config_dir/hash-hget-p${pipeline}-10m"
    run_rb_case "$case_dir" "$pipeline" "$STRUCTURE_KEYS" \
      hget 'h:__rand_int__' field000000
    tail -n 1 "$case_dir/redis-benchmark.csv"

    log "rb start $product t$threads HSET p$pipeline requests=$RB_REQUESTS"
    case_dir="$config_dir/hash-hset-p${pipeline}-10m"
    run_rb_case "$case_dir" "$pipeline" "$STRUCTURE_KEYS" \
      hset 'h:__rand_int__' field000000 "$value"
    tail -n 1 "$case_dir/redis-benchmark.csv"

    log "rb start $product t$threads LINDEX p$pipeline requests=$RB_REQUESTS"
    case_dir="$config_dir/list-lindex-p${pipeline}-10m"
    run_rb_case "$case_dir" "$pipeline" "$STRUCTURE_KEYS" \
      lindex 'l:__rand_int__' 0
    tail -n 1 "$case_dir/redis-benchmark.csv"

    log "rb start $product t$threads LPUSH p$pipeline requests=$RB_REQUESTS"
    case_dir="$config_dir/list-lpush-p${pipeline}-10m"
    run_rb_case "$case_dir" "$pipeline" "$STRUCTURE_KEYS" \
      lpush 'l:__rand_int__' "$value"
    tail -n 1 "$case_dir/redis-benchmark.csv"

    log "rb start $product t$threads LPOP p$pipeline requests=$RB_REQUESTS"
    case_dir="$config_dir/list-lpop-p${pipeline}-10m"
    run_rb_case "$case_dir" "$pipeline" "$STRUCTURE_KEYS" \
      lpop 'l:__rand_int__'
    tail -n 1 "$case_dir/redis-benchmark.csv"

    log "rb start $product t$threads SISMEMBER p$pipeline requests=$RB_REQUESTS"
    case_dir="$config_dir/set-sismember-p${pipeline}-10m"
    run_rb_case "$case_dir" "$pipeline" "$STRUCTURE_KEYS" \
      sismember 's:__rand_int__' member000000
    tail -n 1 "$case_dir/redis-benchmark.csv"

    log "rb start $product t$threads SADD p$pipeline requests=$RB_REQUESTS"
    case_dir="$config_dir/set-sadd-p${pipeline}-10m"
    run_rb_case "$case_dir" "$pipeline" "$STRUCTURE_KEYS" \
      sadd 's:__rand_int__' member000000
    tail -n 1 "$case_dir/redis-benchmark.csv"

    log "rb start $product t$threads ZRANGE p$pipeline requests=$RB_REQUESTS"
    case_dir="$config_dir/zset-zrange-p${pipeline}-10m"
    run_rb_case "$case_dir" "$pipeline" "$STRUCTURE_KEYS" \
      zrange 'z:__rand_int__' 0 9
    tail -n 1 "$case_dir/redis-benchmark.csv"

    log "rb start $product t$threads ZADD p$pipeline requests=$RB_REQUESTS"
    case_dir="$config_dir/zset-zadd-p${pipeline}-10m"
    run_rb_case "$case_dir" "$pipeline" "$STRUCTURE_KEYS" \
      zadd 'z:__rand_int__' 0 member000000
    tail -n 1 "$case_dir/redis-benchmark.csv"
  done

  "$CLI" -h 127.0.0.1 -p "$PORT" info all >"$config_dir/info-end.txt"
  stop_server
}

write_environment() {
  local out="$RESULT_ROOT/environment.txt"
  mkdir -p "$RESULT_ROOT"
  {
    date --iso-8601=seconds
    uname -a
    lscpu
    lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE
    numactl --hardware
    free -h
    swapon --show
    sysctl vm.overcommit_memory vm.swappiness kernel.perf_event_paranoid
    "$REDIS_SERVER" --version
    "$RB" --version
    "$MEMTIER" --version || true
    git -C "$DRAGONFLY_DIR" rev-parse HEAD
    git -C "$DRAGONFLY_DIR" describe --tags --always --dirty
    git -C "$DRAGONFLY_DIR" submodule status
    git -C "$REDIS_DIR" rev-parse HEAD
    git -C "$REDIS_DIR" describe --tags --always --dirty
    sha256sum "$DRAGONFLY_SERVER" "$REDIS_SERVER" "$RB" "$MEMTIER"
  } >"$out" 2>&1
  "$MEMTIER" --help >"$RESULT_ROOT/memtier-help.txt" || true
}

main() {
  local phase=${1:-core}
  case "$phase" in
    core)
      if find "$RESULT_ROOT" -mindepth 1 -print -quit 2>/dev/null | rg -q .; then
        log "result directory is not empty: $RESULT_ROOT"
        return 1
      fi
      write_environment
      run_core_config redis 8
      run_core_config dragonfly 8
      run_core_config redis 16
      run_core_config dragonfly 16
      log "core matrix complete: $RESULT_ROOT"
      ;;
    structures)
      if find "$RESULT_ROOT/structures" -mindepth 1 -print -quit 2>/dev/null | rg -q .; then
        log "structure result directory is not empty: $RESULT_ROOT/structures"
        return 1
      fi
      run_structure_config redis 8
      run_structure_config dragonfly 8
      run_structure_config redis 16
      run_structure_config dragonfly 16
      log "structure matrix complete: $RESULT_ROOT"
      ;;
    *)
      log "unsupported phase: $phase"
      return 1
      ;;
  esac
}

main "$@"
