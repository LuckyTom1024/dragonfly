#!/usr/bin/env bash

set -Eeuo pipefail

REDIS_DIR=${REDIS_DIR:-/home/redis/redis}
DRAGONFLY_DIR=${DRAGONFLY_DIR:-/home/Dragonfly/dragonfly}
LOCAL_RESULT_ROOT=${LOCAL_RESULT_ROOT:-/tmp/dragonfly-bench-remote/formal-20260824}
REMOTE_SSH=${REMOTE_SSH:-10.47.76.3}
REMOTE_RESULT_ROOT=${REMOTE_RESULT_ROOT:-/home/panlei/redis-dragonfly-results/formal-20260824}
REMOTE_WORKER=${REMOTE_WORKER:-/home/panlei/benchmark-tools/redis_dragonfly_remote_client_worker.sh}
SERVER_HOST=${SERVER_HOST:-10.47.76.7}
PORT=${PORT:-6380}
MAXMEMORY=${MAXMEMORY:-8G}
TEST_TIME=${TEST_TIME:-60}
WARMUP_TIME=${WARMUP_TIME:-30}
STRING_KEYS=${STRING_KEYS:-1000000}
STRUCTURE_KEYS=${STRUCTURE_KEYS:-100000}
STRUCTURE_PRELOAD_REQUESTS=${STRUCTURE_PRELOAD_REQUESTS:-1000000}
RB_REQUESTS=${RB_REQUESTS:-10000000}
CONFIGS=${CONFIGS:-'redis:8 dragonfly:8 redis:12 redis:16 dragonfly:16'}
DRAGONFLY_RUNTIME=${DRAGONFLY_RUNTIME:-/home/cdm/cdm-build-tools/cdm-tools-x86_64-linux/gcc-14.2.0/lib64}

CLI="$REDIS_DIR/src/redis-cli"
REDIS_SERVER="$REDIS_DIR/src/redis-server"
DRAGONFLY_SERVER="$DRAGONFLY_DIR/build-opt/dragonfly"
WORKER_SOURCE="$DRAGONFLY_DIR/tools/benchmark/redis_dragonfly_remote_client_worker.sh"

SERVER_PID=
COLLECTORS=()

ulimit -n 65535

log() {
  printf '[%(%FT%T%z)T] %s\n' -1 "$*"
}

server_cpus() {
  case "$1" in
    8) printf '%s\n' '0,2,4,6,8,10,12,14' ;;
    12) printf '%s\n' '0-4,6,8-12,14' ;;
    16) printf '%s\n' '0-15' ;;
    *) log "unsupported server thread count: $1"; return 1 ;;
  esac
}

remote_worker() {
  local cmd
  printf -v cmd '%q ' env \
    "RESULT_ROOT=$REMOTE_RESULT_ROOT" \
    "SERVER_HOST=$SERVER_HOST" \
    "PORT=$PORT" \
    "$REMOTE_WORKER" "$@"
  ssh "$REMOTE_SSH" "$cmd"
}

stop_collectors() {
  if ((${#COLLECTORS[@]})); then
    kill "${COLLECTORS[@]}" 2>/dev/null || true
    wait "${COLLECTORS[@]}" 2>/dev/null || true
  fi
  COLLECTORS=()
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
      if ssh "$REMOTE_SSH" "/home/panlei/benchmark-tools/redis-cli -h $SERVER_HOST -p $PORT ping" \
        >/dev/null 2>&1; then
        return 0
      fi
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
  local product=$1 threads=$2 out=$3 cpus
  cpus=$(server_cpus "$threads")
  mkdir -p "$out"
  if ss -ltn | rg -q ":${PORT} "; then
    log "port $PORT is already in use"
    return 1
  fi

  if [[ $product == redis ]]; then
    numactl --physcpubind="$cpus" --localalloc \
      "$REDIS_SERVER" \
      --bind 0.0.0.0 --port "$PORT" --protected-mode no \
      --save '' --appendonly no --maxmemory "$MAXMEMORY" \
      --maxmemory-policy noeviction --io-threads "$threads" --daemonize no \
      >"$out/server.log" 2>&1 &
  else
    numactl --physcpubind="$cpus" --localalloc \
      env LD_LIBRARY_PATH="$DRAGONFLY_RUNTIME" \
      "$DRAGONFLY_SERVER" \
      --bind=0.0.0.0 --port="$PORT" \
      --proactor_threads="$threads" --num_shards="$threads" \
      --maxmemory="$MAXMEMORY" --cache_mode=false --dbfilename= --alsologtostderr \
      >"$out/server.log" 2>&1 &
  fi
  SERVER_PID=$!
  wait_ready
  taskset -pc "$SERVER_PID" >"$out/server-affinity.txt"
  "$CLI" -h 127.0.0.1 -p "$PORT" info all >"$out/info-start.txt"
  log "started $product threads=$threads pid=$SERVER_PID cpus=$cpus"
}

start_collectors() {
  local out=$1
  pidstat -h -u -r -w -t -p "$SERVER_PID" 1 >"$out/server-pidstat.txt" &
  COLLECTORS+=("$!")
  mpstat -P ALL 1 >"$out/server-mpstat.txt" &
  COLLECTORS+=("$!")
  vmstat -w 1 >"$out/server-vmstat.txt" &
  COLLECTORS+=("$!")
  sar -n DEV,EDEV,TCP,ETCP 1 >"$out/server-sar-network.txt" &
  COLLECTORS+=("$!")
}

save_case_state() {
  local out=$1
  "$CLI" -h 127.0.0.1 -p "$PORT" info all >"$out/info-after.txt"
  sed -n '1,80p' "/proc/$SERVER_PID/status" >"$out/server-proc-status.txt"
  sed -n '1,120p' "/proc/$SERVER_PID/smaps_rollup" >"$out/server-smaps-rollup.txt"
}

run_remote_case() {
  local rel=$1 action=$2
  shift 2
  local out="$LOCAL_RESULT_ROOT/$rel"
  mkdir -p "$out"
  "$CLI" -h 127.0.0.1 -p "$PORT" info all >"$out/info-before.txt"
  start_collectors "$out"
  remote_worker "$action" "$rel" "$@"
  stop_collectors
  save_case_state "$out"
}

sync_remote_results() {
  mkdir -p "$LOCAL_RESULT_ROOT"
  rsync -a "$REMOTE_SSH:$REMOTE_RESULT_ROOT/" "$LOCAL_RESULT_ROOT/"
}

correctness_check() {
  local rel=$1
  remote_worker correctness "$rel"
  "$CLI" -h 127.0.0.1 -p "$PORT" flushall >/dev/null
}

preload_strings() {
  local rel=$1
  remote_worker preload-string "$rel" "$STRING_KEYS"
  local dbsize
  dbsize=$("$CLI" -h 127.0.0.1 -p "$PORT" dbsize)
  printf '%s\n' "$dbsize" >"$LOCAL_RESULT_ROOT/$rel/dbsize-after-preload.txt"
  if [[ $dbsize != "$STRING_KEYS" ]]; then
    log "preload failed: expected $STRING_KEYS keys, got $dbsize"
    return 1
  fi
}

run_core_config() {
  local product=$1 threads=$2 rel="core/${product}-t${threads}"
  local clients pipeline case_rel
  start_server "$product" "$threads" "$LOCAL_RESULT_ROOT/$rel"
  correctness_check "$rel"
  preload_strings "$rel"

  log "remote warmup $product t$threads ${WARMUP_TIME}s"
  run_remote_case "$rel/warmup-c1000-p1" memtier-string 1000 1 "$WARMUP_TIME" "$STRING_KEYS"

  for clients in 1000; do
    for pipeline in 1 16 64; do
      case_rel="$rel/string-c${clients}-p${pipeline}-60s"
      log "remote formal start $product t$threads c$clients p$pipeline ${TEST_TIME}s"
      run_remote_case "$case_rel" memtier-string "$clients" "$pipeline" "$TEST_TIME" "$STRING_KEYS"
      remote_worker environment >/dev/null 2>&1 || true
    done
  done

  case_rel="$rel/string-c2000-p1-60s"
  log "remote formal start $product t$threads c2000 p1 ${TEST_TIME}s"
  run_remote_case "$case_rel" memtier-string 2000 1 "$TEST_TIME" "$STRING_KEYS"

  case_rel="$rel/tx1-c1000-p1-60s"
  log "remote transaction start $product t$threads c1000 p1 ${TEST_TIME}s"
  run_remote_case "$case_rel" memtier-transaction "$TEST_TIME"

  "$CLI" -h 127.0.0.1 -p "$PORT" info all >"$LOCAL_RESULT_ROOT/$rel/info-end.txt"
  stop_server
  sync_remote_results
}

remote_rb_preload() {
  local rel=$1 keyspace=$2
  shift 2
  remote_worker rb-preload "$rel" "$keyspace" "$STRUCTURE_PRELOAD_REQUESTS" "$@"
}

preload_structures() {
  local rel=$1 value=$2 idx member score
  preload_strings "$rel"
  for idx in $(seq 0 9); do
    printf -v member 'field%06d' "$idx"
    remote_rb_preload "$rel/preload/hash-$idx" "$STRUCTURE_KEYS" \
      hset 'h:__rand_int__' "$member" "$value"
  done
  remote_rb_preload "$rel/preload/list" "$STRUCTURE_KEYS" \
    lpush 'l:__rand_int__' "$value"
  for idx in $(seq 0 9); do
    printf -v member 'member%06d' "$idx"
    remote_rb_preload "$rel/preload/set-$idx" "$STRUCTURE_KEYS" \
      sadd 's:__rand_int__' "$member"
    score=$idx
    remote_rb_preload "$rel/preload/zset-$idx" "$STRUCTURE_KEYS" \
      zadd 'z:__rand_int__' "$score" "$member"
  done
  "$CLI" -h 127.0.0.1 -p "$PORT" dbsize >"$LOCAL_RESULT_ROOT/$rel/dbsize-after-structure-preload.txt"
}

run_rb_case() {
  local rel=$1 pipeline=$2 keyspace=$3
  shift 3
  run_remote_case "$rel" rb-case "$pipeline" "$keyspace" "$RB_REQUESTS" "$@"
}

run_rb_builtin() {
  local rel=$1 pipeline=$2 test=$3
  run_remote_case "$rel" rb-builtin "$pipeline" "$test" "$RB_REQUESTS" "$STRING_KEYS"
}

run_structure_config() {
  local product=$1 threads=$2 rel="structures/${product}-t${threads}"
  local value pipeline
  printf -v value '%0128d' 0
  start_server "$product" "$threads" "$LOCAL_RESULT_ROOT/$rel"
  correctness_check "$rel"
  preload_structures "$rel" "$value"

  for pipeline in 1 16 64; do
    log "remote rb $product t$threads SET p$pipeline requests=$RB_REQUESTS"
    run_rb_builtin "$rel/string-set-p${pipeline}-10m" "$pipeline" set
    log "remote rb $product t$threads GET p$pipeline requests=$RB_REQUESTS"
    run_rb_builtin "$rel/string-get-p${pipeline}-10m" "$pipeline" get
    log "remote rb $product t$threads HGET p$pipeline requests=$RB_REQUESTS"
    run_rb_case "$rel/hash-hget-p${pipeline}-10m" "$pipeline" "$STRUCTURE_KEYS" \
      hget 'h:__rand_int__' field000000
    log "remote rb $product t$threads HSET p$pipeline requests=$RB_REQUESTS"
    run_rb_case "$rel/hash-hset-p${pipeline}-10m" "$pipeline" "$STRUCTURE_KEYS" \
      hset 'h:__rand_int__' field000000 "$value"
    log "remote rb $product t$threads LINDEX p$pipeline requests=$RB_REQUESTS"
    run_rb_case "$rel/list-lindex-p${pipeline}-10m" "$pipeline" "$STRUCTURE_KEYS" \
      lindex 'l:__rand_int__' 0
    log "remote rb $product t$threads LPUSH p$pipeline requests=$RB_REQUESTS"
    run_rb_case "$rel/list-lpush-p${pipeline}-10m" "$pipeline" "$STRUCTURE_KEYS" \
      lpush 'l:__rand_int__' "$value"
    log "remote rb $product t$threads LPOP p$pipeline requests=$RB_REQUESTS"
    run_rb_case "$rel/list-lpop-p${pipeline}-10m" "$pipeline" "$STRUCTURE_KEYS" \
      lpop 'l:__rand_int__'
    log "remote rb $product t$threads SISMEMBER p$pipeline requests=$RB_REQUESTS"
    run_rb_case "$rel/set-sismember-p${pipeline}-10m" "$pipeline" "$STRUCTURE_KEYS" \
      sismember 's:__rand_int__' member000000
    log "remote rb $product t$threads SADD p$pipeline requests=$RB_REQUESTS"
    run_rb_case "$rel/set-sadd-p${pipeline}-10m" "$pipeline" "$STRUCTURE_KEYS" \
      sadd 's:__rand_int__' member000000
    log "remote rb $product t$threads ZRANGE p$pipeline requests=$RB_REQUESTS"
    run_rb_case "$rel/zset-zrange-p${pipeline}-10m" "$pipeline" "$STRUCTURE_KEYS" \
      zrange 'z:__rand_int__' 0 9
    log "remote rb $product t$threads ZADD p$pipeline requests=$RB_REQUESTS"
    run_rb_case "$rel/zset-zadd-p${pipeline}-10m" "$pipeline" "$STRUCTURE_KEYS" \
      zadd 'z:__rand_int__' 0 member000000
  done
  "$CLI" -h 127.0.0.1 -p "$PORT" info all >"$LOCAL_RESULT_ROOT/$rel/info-end.txt"
  stop_server
  sync_remote_results
}

write_environment() {
  mkdir -p "$LOCAL_RESULT_ROOT"
  {
    date --iso-8601=seconds
    hostname
    uname -a
    lscpu
    lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE
    numactl --hardware
    free -h
    swapon --show
    ip -4 -br addr
    ip route
    "$REDIS_SERVER" --version
    git -C "$DRAGONFLY_DIR" rev-parse HEAD
    git -C "$DRAGONFLY_DIR" describe --tags --always --dirty
    git -C "$DRAGONFLY_DIR" submodule status
    git -C "$REDIS_DIR" rev-parse HEAD
    git -C "$REDIS_DIR" describe --tags --always --dirty
    sha256sum "$DRAGONFLY_SERVER" "$REDIS_SERVER"
    sha256sum "$DRAGONFLY_DIR/docs/redis-vs-dragonfly-standalone-benchmark-report.md"
  } >"$LOCAL_RESULT_ROOT/server-environment.txt" 2>&1
}

setup_remote() {
  ssh "$REMOTE_SSH" "rm -rf '$REMOTE_RESULT_ROOT'; mkdir -p '$REMOTE_RESULT_ROOT'; swapoff -a"
  scp -q "$WORKER_SOURCE" "$REMOTE_SSH:$REMOTE_WORKER"
  ssh "$REMOTE_SSH" "chmod +x '$REMOTE_WORKER'"
  remote_worker environment
}

main() {
  local phase=${1:-core} spec product threads
  case "$phase" in
    core)
      if find "$LOCAL_RESULT_ROOT" -mindepth 1 -print -quit 2>/dev/null | rg -q .; then
        log "local result directory is not empty: $LOCAL_RESULT_ROOT"
        return 1
      fi
      setup_remote
      write_environment
      ;;
    structures)
      if find "$LOCAL_RESULT_ROOT/structures" -mindepth 1 -print -quit 2>/dev/null | rg -q .; then
        log "structure result directory is not empty: $LOCAL_RESULT_ROOT/structures"
        return 1
      fi
      ;;
    *) log "unsupported phase: $phase"; return 1 ;;
  esac

  for spec in $CONFIGS; do
    product=${spec%:*}
    threads=${spec#*:}
    if [[ $phase == core ]]; then
      run_core_config "$product" "$threads"
    else
      run_structure_config "$product" "$threads"
    fi
  done
  sync_remote_results
  log "remote $phase matrix complete: $LOCAL_RESULT_ROOT"
}

main "$@"
