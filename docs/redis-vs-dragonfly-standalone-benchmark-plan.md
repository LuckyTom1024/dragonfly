# Redis vs Dragonfly 单机性能测试与架构分析方案

> 文档状态：可执行测试方案；本轮正式结果见 `redis-vs-dragonfly-standalone-benchmark-report.md`
> 基线日期：2026-08-21
> 服务端范围：单机、单进程、单实例、standalone 模式
> 主要工具：`redis-benchmark(/home/redis/redis)`、`memtier_benchmark(/tmp/memtier_benchmark/)`，事务专项辅以自定义 RESP 驱动器

## 1. 结论先行：本方案解决什么问题

本方案用于回答以下选型问题：

1. 在相同硬件、数据和协议条件下，Redis 与 Dragonfly 的最大吞吐、延迟和资源成本分别是多少？
2. Redis I/O 多线程能在多大程度上缓解网络读写和协议解析开销，何时由主线程命令执行形成上限？
3. Dragonfly 从 8 个物理核对应线程扩展到 16 个 SMT 逻辑 CPU 对应线程时，吞吐增长和效率如何？
4. 五类基础数据结构、pipeline、混合读写、事务、热点和多 key 操作下，两者差距是否一致？
5. 在当前机器上，8 线程和 16 线程中哪一档更适合吞吐、尾延迟和 CPU 效率目标？

最终结论不能只引用峰值 QPS。每个核心结论至少同时给出：

- 吞吐：operations/s；事务场景另报 transactions/s；
- 延迟：平均值、P50、P95、P99、P99.9 和最大值；
- 服务端成本：平均 CPU cores、QPS/CPU-core、RSS、每逻辑元素内存；
- 扩展性：16 线程相对 8 线程的 speedup 和 scaling efficiency；
- 稳定性：多轮中位数、离散程度、错误数和尾延迟抖动；
- 等吞吐比较：在相同 offered load 下比较延迟，而不只比较各自极限点。

## 2. 范围与关键修正

### 2.1 本轮包含

- Redis 单实例 standalone；
- Dragonfly 单实例 standalone；
- 关闭复制、Cluster、Sentinel、TLS、认证和主动持久化的纯内存主测试；
- TCP 协议测试，默认 RESP2；
- Redis I/O 线程数与 Dragonfly proactor/shard 线程数扩展；
- String、Hash、List、Set、Sorted Set；
- 单命令、pipeline、混合读写、`MULTI/EXEC`、热点和均匀 key 分布；
- 低并发延迟、饱和吞吐、等吞吐延迟和 CPU 效率。

### 2.2 本轮明确不包含

- Redis Cluster、Dragonfly Cluster、分片集群或代理；
- 主从复制、Sentinel、多可用区和故障切换；
- 跨节点或跨 slot 事务；
- TLS、ACL、压缩、模块、Search 和 tiered storage；
- RDB/AOF/快照性能；
- 多 Redis 实例对单 Dragonfly 实例。

持久化可作为后续独立报告，不能与本报告的纯内存结果混合。

### 2.3 “跨槽事务”改为“跨内部分片事务”

standalone 模式没有 Redis Cluster slot 路由语义，因此 `key1`、`key2` 是否属于不同 slot 对 Redis
standalone 没有意义。本方案不再使用“跨槽事务”这一名称。

事务专项改为：

- 单 key 事务：同一 key 上连续读写；
- 多 key、Dragonfly 单内部 shard 事务：预生成经验证落在同一 Dragonfly shard 的 key；
- 多 key、Dragonfly 多内部 shard 事务：预生成覆盖 2/4/8 个 Dragonfly shard 的 key；
- 热点冲突事务：多个 client 竞争相同 key 集合。

Redis 接收完全相同的命令和 key 序列。这里测量的是 Dragonfly 单机内部协调成本，不暗示 Redis
standalone 存在 slot 调度。不得通过启用 Cluster 或 `--lock_on_hashtags` 来构造主结果。

## 3. 当前源码和机器基线

### 3.1 当前目录实查结果

| 项目                 | 当前值                                       | 测试前处理                                |
| -------------------- | -------------------------------------------- | ----------------------------------------- |
| Dragonfly 源码       | `/home/Dragonfly/dragonfly`                | 固定 commit 后构建 release                |
| Dragonfly commit     | `0ab6e227bb1286a9bda7b0438c9ea3abaf633e60` | 当前为`main`，不是稳定 tag              |
| Dragonfly describe   | `v1.40.0-74-g0ab6e227`                     | 报告必须保留 describe 和完整 SHA          |
| Dragonfly Helio      | `3d8b4ece14ed62cb516e36afbc8947c0cc392826` | 已初始化并用于 release 构建               |
| Dragonfly 二进制     | `/home/Dragonfly/dragonfly/build-opt/dragonfly` | release 构建；运行需 GCC 14 runtime      |
| Redis 源码           | `/home/redis/redis`                        | 固定 commit 后重建 release                |
| Redis commit         | `de3ea25a9f486a44467cad0628bdb2d0a36aabf0` | 当前分支为`unstable`，不是稳定 tag      |
| Redis server version | `8.9.241`, jemalloc 5.3.0                  | 版本号与 git describe 都要记录            |
| redis-benchmark      | `8.9.241`                                  | 固定使用同一个客户端二进制压两种服务      |
| memtier_benchmark    | `/tmp/memtier_benchmark/memtier_benchmark` | `v=255.255.255 sha=272eeb64:1`            |

当前两个目录都不是“官方最新稳定 tag 对稳定 tag”。正式报告必须二选一，并写清选择：

1. **当前源码对比**：固定上述两个 SHA，报告标题写“current source snapshot”；或
2. **稳定版对比**：分别 checkout 测试开始日最新稳定 tag，重新构建并记录 SHA。

不能把当前结果标注成“稳定版对比”。也不能在一个产品上使用源码快照、另一个使用稳定 release。

### 3.2 当前服务器拓扑实查结果

| 项目                    | 当前值                                        | 影响                                          |
| ----------------------- | --------------------------------------------- | --------------------------------------------- |
| CPU                     | Intel Xeon Cascade Lake，16 logical CPUs      | 不能直接写成 16 个物理核                      |
| 物理拓扑                | 2 sockets × 4 cores/socket × 2 threads/core | 共 8 物理核，SMT 开启                         |
| NUMA                    | 1 个可见 NUMA node                            | 虚拟机拓扑与 socket 信息不完全一致，需披露    |
| 内存                    | 31 GiB                                        | 10M 数据集必须先做容量试装                    |
| 当前可用内存            | 约 21 GiB                                     | 正式测试前需清理背景负载并重新记录            |
| swap                    | 当前为 0 B（本轮已执行 `swapoff -a`）        | 每轮仍需用 `vmstat` 验证零 swap I/O          |
| Kernel                  | Linux 5.15.0-124-generic                      | Dragonfly 可使用 io_uring，仍须从启动日志确认 |
| 虚拟化                  | KVM                                           | 必须采集 steal time，宿主机争用会污染结果     |
| THP                     | `madvise`                                   | 保持不变并记录，不在产品之间切换              |
| `perf_event_paranoid` | 4                                             | 普通用户不能完整采集 perf；需授权或 root      |
| sysstat                 | 12.5.2                                       | 已用于 `pidstat`；仍需归档完整采集输出         |
| NIC                     | 虚拟网卡，速率未知                            | 远程压测前实测带宽，而不是假定 10/25 GbE      |

线程曲线应按以下方式解释：

- Redis 测试 8、12、16 三档 I/O threads；Dragonfly 测试 8、16 两档服务端线程数；
- 8 线程分别绑定到 8 个不同物理核，作为物理核基线；
- Redis 12 线程在两个 socket 各增加 2 个 SMT siblings，保持跨 socket 对称；
- 16 线程使用全部 16 个 SMT 逻辑 CPU；
- 8 到 12/16 的增益主要是 SMT 增益，不能宣传为物理核线性扩展，且可能很小甚至为负；
- 若 load generator 与服务端同机，16 个逻辑 CPU 无法同时完整分配给两者，该结果只能标记为
  “同机资源竞争”，不能作为主结论。

## 4. 线程模型与待验证假设

### 4.1 当前 Redis 源码的线程模型

当前 Redis 源码中：

- `io-threads 1` 为默认单线程客户端 I/O；
- `io-threads N` 中，主线程计入 N，实际额外创建 N-1 个 I/O 线程；
- I/O 线程负责 socket read/write、输入读取和 RESP 协议解析；
- 完整命令被移交主线程执行，数据结构访问仍由主线程串行完成；
- `io-threads-do-reads` 在当前源码中已废弃，不应再写入配置；
- lazy free、AOF 等后台线程不等于命令执行线程，不应计入 `io-threads` 扩展。

待验证假设：

- 小 value、简单 GET/SET 在网络或协议解析占比较高时，增加 I/O threads 可提升吞吐；
- 命令执行、复杂数据结构或热点成为瓶颈后，主线程接近 100% 单核，增加 I/O threads 收益递减；
- pipeline 增大后，每命令网络开销下降，Redis I/O 多线程的相对收益可能缩小；
- I/O threads 过多会增加主线程交接、调度和 cache 开销。

### 4.2 Dragonfly 的线程模型

Dragonfly 是 shared-nothing 多线程架构：

- client connection 由 fiber 处理，并在生命周期内绑定到一个 proactor thread；
- 内存数据库切分为 N 个 shard，每个 shard 由单一线程拥有；
- 单 key 命令路由到拥有该 key 的 shard 执行；
- 多 key/多命令事务由 connection fiber 协调相关 shard；
- 默认 `num_shards=0` 时会自动取 `proactor_threads` 数量；
- 本测试显式设置 `--proactor_threads=N --num_shards=N`，避免隐式默认变化；
- `--conn_io_threads` 保持默认 0，不把连接线程隔离实验混入主矩阵；
- `multi_exec_squash` 保持默认开启，并记录该值。

待验证假设：

- 均匀分布的独立 key 可随 shard/proactor 数量扩展；
- 单热点 key、少数 container key 或高度冲突事务无法获得相同扩展；
- 多 shard 事务会产生调度、锁和跨线程 hop 成本；
- pipeline/MULTI command squashing 可摊薄 hop，但对延迟和公平性需要单独解释；
- 线程数超过 8 个物理核后，SMT 和调度开销使效率下降。

### 4.3 本方案所依据的当前源码位置

| 结论                                                | 源码或文档                           |
| --------------------------------------------------- | ------------------------------------ |
| Redis I/O threads 读取、解析并把完整命令交回主线程  | `/home/redis/redis/src/iothread.c` |
| Redis`io-threads-do-reads` 已废弃                 | `/home/redis/redis/src/config.c`   |
| Redis 当前 threaded I/O 配置说明                    | `/home/redis/redis/redis.conf`     |
| Dragonfly shared-nothing/thread-per-shard           | `docs/df-share-nothing.md`         |
| Dragonfly 多 shard 与 MULTI/EXEC 调度               | `docs/transaction.md`              |
| Dragonfly`num_shards=0` 自动取 proactor pool size | `src/server/main_service.cc`       |

正式报告引用架构差异时应关联线程 CPU、transaction width 或源码事实，不能仅凭 benchmark 数值反推实现。

## 5. 公平性原则

### 5.1 一次只运行一个服务端

Redis 和 Dragonfly 不得同时驻留数据并压测。每个 case 的流程为：停止全部目标服务、确认端口和
进程清空、启动一个服务、装载数据、预热、测量、采集、停止服务。

推荐以交错顺序抵消机器随时间漂移：

```text
repeat 1: Redis -> Dragonfly
repeat 2: Dragonfly -> Redis
repeat 3: Redis -> Dragonfly
repeat 4: Dragonfly -> Redis
repeat 5: 随机顺序
```

### 5.2 主结果使用独立 load generator

“单机”在本方案中指单机单实例服务端，不表示压力客户端必须与服务端同机。主结果应由独立压测机
通过 TCP 访问服务端，两台机器在同一交换网络，且压测机满足：

- CPU 使用率低于 70%，无单 load thread 长时间打满；
- 将 load threads 增加 25% 后，服务端 QPS 变化小于 3%；
- NIC 吞吐低于实测可用带宽的 80%，无 drop/retransmit；
- 同一个客户端二进制、线程数、client 数和随机种子用于两个产品。

如果只能同机执行，则单独建立 `local-contention` profile：

- 使用 Unix socket 的结果只能测协议/执行上限，不能与 TCP 主结果混用；
- 使用 loopback TCP 时采集服务端和客户端各自 CPU；
- 按 `lscpu -e=CPU,CORE,SOCKET,NODE` 选择不重叠物理核，避免把 SMT sibling 分给相互竞争的两侧；
- 因当前只有 8 个物理核，同机模式无法验证“服务端使用 16 logical CPUs”的无干扰扩展。

### 5.3 固定所有非目标变量

主矩阵固定：

- 相同 server host、端口类型、RESP2、MTU、TCP 参数和 CPU affinity；
- 相同 key/value bytes，而不仅是相同字符数；
- 相同数据逻辑状态、命中率、返回元素数和响应字节数；
- 相同 client 总数。注意 memtier 的总连接数是 `threads × clients-per-thread`；
- 相同 pipeline depth；
- 相同 load-generator threads，不能随服务端线程数改变客户端能力；
- 相同测试时长、随机种子、重复次数和执行顺序策略；
- 相同无持久化、no-eviction 语义；
- 不在一个产品上使用 Unix socket、另一个使用 TCP；
- 不在 Dragonfly 使用特殊 hashtag/round-robin sharding 参数来美化均匀度。

## 6. 构建、版本冻结与产物记录

### 6.1 Dragonfly release 构建

```bash
cd /home/Dragonfly/dragonfly
git submodule update --init --recursive
./helio/blaze.sh -release \
  -DWITH_AWS=OFF \
  -DWITH_GCP=OFF \
  -DWITH_TIERING=OFF \
  -DWITH_SEARCH=OFF
cd build-opt
ninja dragonfly
./dragonfly --version
```

本轮实际构建因系统 `/usr/local` 暴露旧 GmSSL/OpenSSL 1.1.0d，额外使用 OpenSSL 3 overlay 和 GCC 14
runtime，完整关键参数为：

```text
-release -DWITH_AWS=OFF -DWITH_GCP=OFF -DWITH_TIERING=OFF -DWITH_SEARCH=OFF -DDF_USE_SSL=OFF
-DOPENSSL_INCLUDE_DIR=/tmp/openssl3
-DOPENSSL_SSL_LIBRARY=/usr/lib/x86_64-linux-gnu/libssl.so.3
-DOPENSSL_CRYPTO_LIBRARY=/usr/lib/x86_64-linux-gnu/libcrypto.so.3
-DCMAKE_CXX_FLAGS=-I/tmp/openssl3
```

启动 Dragonfly 时设置：

```bash
export LD_LIBRARY_PATH=/home/cdm/cdm-build-tools/cdm-tools-x86_64-linux/gcc-14.2.0/lib64
```

### 6.2 Redis release 构建

正式构建前保存当前源码状态。不要在有未提交用户改动的目录执行清理。

```bash
cd /home/redis/redis
git status --short
make -j"$(nproc)" BUILD_TLS=no
./src/redis-server --version
./src/redis-benchmark --version
```

如需 `make distclean`，必须先确认工作树没有需要保留的生成文件或改动。两者均使用同一代编译器和
等价 release 优化；不得拿 Dragonfly release 与 Redis debug/sanitizer build 对比。

### 6.3 每次测试归档

```bash
git -C /home/Dragonfly/dragonfly status --short
git -C /home/Dragonfly/dragonfly rev-parse HEAD
git -C /home/Dragonfly/dragonfly describe --tags --always --dirty
git -C /home/Dragonfly/dragonfly submodule status

git -C /home/redis/redis status --short
git -C /home/redis/redis rev-parse HEAD
git -C /home/redis/redis describe --tags --always --dirty

sha256sum /home/Dragonfly/dragonfly/build-opt/dragonfly
sha256sum /home/redis/redis/src/redis-server
sha256sum /home/redis/redis/src/redis-benchmark
ldd /home/Dragonfly/dragonfly/build-opt/dragonfly
ldd /home/redis/redis/src/redis-server
```

同时归档：完整构建命令、CMake cache、编译器/链接器版本、启动命令、展开后的配置文件、stdout、
stderr、二进制大小和 `file` 输出。

## 7. 主机准备和前置验收

### 7.1 系统快照

```bash
date --iso-8601=seconds
uname -a
lscpu
lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE,MAXMHZ,MINMHZ
numactl --hardware
free -h
swapon --show
cat /sys/devices/system/cpu/smt/active
cat /sys/kernel/mm/transparent_hugepage/enabled
sysctl vm.swappiness kernel.perf_event_paranoid kernel.nmi_watchdog
sysctl net.core.somaxconn net.ipv4.tcp_max_syn_backlog
ip -s link
```

另外记录：服务器型号、BIOS、CPU governor、turbo 状态、容器/cgroup 限制、中断亲和性、网卡型号、
驱动、固件、队列数、MTU、实测单向/双向带宽和 RTT。

### 7.2 正式测试条件

- 停止无关 workload、定时任务和监控抓取；
- 禁止 swap，或至少保证 `vmstat 1` 中 `si/so` 全程为 0；
- CPU governor 固定为 performance；若虚拟机无权设置，明确记录；
- 保持 THP 策略对两个产品完全一致；
- `ulimit -n` 高于最大 client 数加内部 fd 余量，建议至少 65535；
- 检查 `dmesg` 无 OOM、NIC reset、io_uring error 和 soft lockup；
- 连续采集 `mpstat` 的 steal，任何正式轮次平均 steal > 1% 应作废重跑；
- 远程 TCP 测试先用 `iperf3` 验证网络不会先于服务端饱和。

不要为了测试临时改变一批内核参数却不记录。所有变更须在 `environment.txt` 中保存变更前、变更后
和值恢复情况。

## 8. 服务端配置

### 8.1 Redis 配置模板

每个 `N` 生成独立配置，例如 `redis-N.conf`：

```conf
bind 0.0.0.0
port 6379
protected-mode no
daemonize no
databases 1

save ""
appendonly no

maxmemory 24gb
maxmemory-policy noeviction

io-threads N
```

注意：

- `24gb` 只是基于 31 GiB 机器的起始建议，最终值由容量试装决定；
- 不写 `io-threads-do-reads`，当前 Redis 已废弃该配置；
- `io-threads=N` 是包含主线程在内的客户端 I/O 线程总量；
- 使用远程客户端时必须通过防火墙限制来源，不能把无认证实例暴露到不可信网络；
- 启动后用 `CONFIG GET`、`INFO server` 和 `INFO persistence` 验证实际值。

启动模板：

```bash
N=8
if ((N == 8)); then
  SERVER_CPUS=0,2,4,6,8,10,12,14
elif ((N == 16)); then
  SERVER_CPUS=0-15
else
  echo "本方案只允许 N=8 或 N=16" >&2
  exit 1
fi
numactl --physcpubind="$SERVER_CPUS" --localalloc \
  /home/redis/redis/src/redis-server /path/to/redis-N.conf
```

### 8.2 Dragonfly 启动模板

```bash
N=8
if ((N == 8)); then
  SERVER_CPUS=0,2,4,6,8,10,12,14
elif ((N == 16)); then
  SERVER_CPUS=0-15
else
  echo "本方案只允许 N=8 或 N=16" >&2
  exit 1
fi
numactl --physcpubind="$SERVER_CPUS" --localalloc \
  /home/Dragonfly/dragonfly/build-opt/dragonfly \
  --bind=0.0.0.0 \
  --port=6379 \
  --proactor_threads="$N" \
  --num_shards="$N" \
  --maxmemory=24G \
  --cache_mode=false \
  --alsologtostderr
```

固定默认值并在启动日志中验证：

- standalone：不传 `--cluster_mode`；
- `conn_io_threads=0`；
- `multi_exec_squash=true`；
- `cache_mode=false`，达到内存限制时不做缓存淘汰；
- 不配置 snapshot cron，不主动调用 `SAVE`；
- 从日志确认 proactor 类型、实际 thread 数、shard 数和 maxmemory。

### 8.3 线程矩阵

| 产品      | 必测线程数 | 解释                                       |
| --------- | ---------- | ------------------------------------------ |
| Redis     | 8, 12, 16  | `io-threads`，命令执行主线程仍是关键瓶颈 |
| Dragonfly | 8, 16      | `proactor_threads=num_shards`            |

16 logical CPU 机器上不预先断言 16 优于 8。最终建议必须来自实测，并分别报告：最高 QPS 配置、
最低 P99 配置和最高 QPS/CPU-core 配置。

CPU 绑定按当前 `lscpu -e` 拓扑设置：

| 服务端线程数 | CPU 列表 | 含义 |
|---:|---|---|
| 8 | `0,2,4,6,8,10,12,14` | 每个物理核选择一个 logical CPU |
| 12 | `0-4,6,8-12,14` | 每个 socket 各 6 个 logical CPU，对称增加 SMT siblings |
| 16 | `0-15` | 使用每个物理核的两个 SMT siblings |

如果硬件、虚拟机拓扑或 CPU hotplug 发生变化，必须重新由 `CORE` 列生成列表，不能照抄上述编号。

## 9. 数据集设计

### 9.1 容量不能只写“1000 万 key”

五种结构没有统一的 key/value 概念。Hash/List/Set/ZSet 还存在顶层 key、field/member/element，编码也会
随元素数变化。因此统一为 **1000 万逻辑元素、128-byte payload**，并为每类结构记录顶层 key 数、
每 key 元素数、编码和实际内存。

正式装载 10M 前先装载 1M 逻辑元素，测量内存并外推：

```text
projected_peak = memory_after_1M × 10 × 1.20
```

只有 projected peak 小于主机 RAM 的 70%，且装载后 `MemAvailable` 留有至少 20% 时才执行 10M。
如果任一产品不满足，两个产品都降低到相同逻辑元素数，并在标题标注实际规模。

### 9.2 规范数据模型

| 结构       | 顶层 key × 每 key 元素 |       payload | 读操作                  | 写操作                                 |
| ---------- | ----------------------: | ------------: | ----------------------- | -------------------------------------- |
| String     |                10M × 1 |   value 128 B | GET hit/miss            | SET update；另测 SET insert            |
| Hash       |         1M × 10 fields |   value 128 B | HGET hit/miss           | HSET update；另测 field insert         |
| List       |       1M × 10 elements | element 128 B | LINDEX；LPOP 单独测     | LPUSH 单独测；稳态用 LPUSH+LPOP        |
| Set        |        1M × 10 members |  member 128 B | SISMEMBER hit/miss      | SADD insert；重复 SADD 作为 no-op 单列 |
| Sorted Set |        1M × 10 members |  member 128 B | ZRANGE 0 9；补充 ZSCORE | ZADD update；另测 insert               |

key、field、member 中的前缀和数字宽度固定。payload 使用固定 seed 生成的二进制安全 128 bytes，避免
两个工具生成不同内容。归档数据生成 seed 和前 100 条样本的 SHA256。

### 9.3 防止“假写入”和“读空数据”

- `SET` 覆盖与首次插入的成本不同，分别报告；
- `HSET` 更新已有 field 与新增 field 分开；
- `SADD` 已存在 member 返回 0，不是有效写入吞吐，必须检查返回值；
- `ZADD` 更新 score 与新增 member 分开；
- `LPOP` 会耗尽 list，不能对一个有限预装数据集无限跑 60 秒；
- `LPUSH` 会持续增大内存，不能让两产品在不同内存水位上结束；
- 破坏性 List 测试按固定 operation 数运行并在每轮前重装，稳态吞吐另用原子
  `LPUSH key value` + `LPOP key` 成对 workload；
- 所有 read case 开始和结束都抽样验证命中率、元素数和响应长度。

### 9.4 key 分布

每个核心 workload 至少包含：

1. **Uniform**：10M 逻辑元素均匀随机，验证多核扩展；
2. **Hotset**：1% key 承担 90% 请求，模拟业务热点；
3. **Single hot key/container**：诊断序列化上限，不计入综合分数；
4. **Miss**：GET/HGET/SISMEMBER 各 50% miss 专项，不能与 100% hit 合并。

如使用 Zipf/Gaussian，记录工具版本、参数和实际 top-1/top-10 key 请求占比，不能只写“Zipf”。

## 10. 分层测试矩阵

即使只测试 Redis 8/12/16、Dragonfly 8/16，完整笛卡尔积仍会产生数百轮。本方案分四层执行，
避免重复组合掩盖核心结论。

### 10.1 P0：正确性和压力端能力验收

每个服务端配置先执行：

- `PING`、五类结构读写和两类 `MULTI/EXEC` 的返回值校验；
- 1 client、pipeline 1 的 5 分钟稳定性；
- load threads 增加 25% 的饱和验证；
- 检查 error、timeout、connection reset、miss、eviction 全为预期；
- 检查服务端线程数、CPU affinity、数据量和 maxmemory；
- 运行空连接测试，确认 2000 clients 可建立且 fd/端口范围足够。

任何正确性失败都必须先停止性能测试。

### 10.2 P1：延迟与 client 饱和曲线

固定服务端 `N=8` 和 `N=16`，String GET、SET update、90:10 GET:SET，pipeline=1，client 数：

```text
1, 8, 32, 128, 512, 1000, 2000
```

目的：

- 1/8 clients 给出低排队延迟；
- 32~512 找到吞吐拐点；
- 1000/2000 满足高并发目标，但不用于声称“单请求低延迟”；
- 若 1000 到 2000 clients QPS 不升而 P99 大增，报告应将 1000 视为更合理工作点。

### 10.3 P2：线程扩展主矩阵

| 维度                     | 值                                 |
| ------------------------ | ---------------------------------- |
| Redis threads            | 8, 12, 16                           |
| Dragonfly threads/shards | 8, 16                               |
| Workload                 | GET hit、SET update、GET:SET=90:10 |
| Clients                  | 1000；2000 作为确认点              |
| Pipeline                 | 1, 16；64 作为确认点               |
| Distribution             | Uniform；single-hot-key 作为诊断点 |
| Repeat                   | 至少 5 轮                          |

P2 是判断 Redis I/O threads 与 Dragonfly 命令执行多线程差距的主数据。Redis 8/12/16 和 Dragonfly
8/16 都完整跑 1000 clients；2000 clients 和 pipeline 64 也分别在所有必测线程数上确认。

### 10.4 P3：五种数据结构矩阵

P3 在 Redis 8/12/16 和 Dragonfly 8/16 配置上执行：

| 维度         | 值                                                                 |
| ------------ | ------------------------------------------------------------------ |
| Commands     | GET/SET、HGET/HSET、LINDEX/LPOP/LPUSH、SISMEMBER/SADD、ZRANGE/ZADD |
| Clients      | 1000, 2000                                                         |
| Pipeline     | 1, 16, 64                                                          |
| Read/write   | 100:0、90:10、50:50；不适用的结构使用成对稳态定义                  |
| Distribution | Uniform，另对热点做 pipeline=1                                     |
| Repeat       | 至少 5 轮                                                          |

对 ZRANGE 固定返回 10 个 member，并同时记录 response bytes/s。不能用返回 1 个元素的 Dragonfly
结果与返回 10 个元素的 Redis 结果比较。

### 10.5 P4：等吞吐延迟与极限压力

先取得两个产品同一 workload 的饱和 QPS，再以较低者的 25%、50%、75%、90% 作为共同 offered
load，运行 rate-limited 测试。输出 P50/P95/P99/P99.9 延迟曲线。

饱和点定义为：继续增加 clients/load threads 后 QPS 增长小于 3%，或 P99 比前一点增加超过 2 倍。
最大 QPS 与可接受尾延迟下的 QPS 必须分开报告。

## 11. benchmark 工具职责和命令模板

### 11.1 redis-benchmark：命令微基准

固定使用 Redis 源码目录中的客户端压两个产品：

```bash
RB=/home/redis/redis/src/redis-benchmark
HOST=server.example
PORT=6379
CLIENT_THREADS=8
CLIENTS=1000
PIPELINE=1
KEYS=10000000
HASH_KEYS=1000000
REQUESTS=100000000
```

String 内置测试：

```bash
"$RB" -h "$HOST" -p "$PORT" \
  --threads "$CLIENT_THREADS" -c "$CLIENTS" \
  -P "$PIPELINE" -n "$REQUESTS" -r "$KEYS" -d 128 \
  -t set,get --csv
```

任意命令示例：

```bash
VALUE=$(printf '%0128d' 0)
test "$(printf %s "$VALUE" | wc -c)" -eq 128

"$RB" -h "$HOST" -p "$PORT" --threads "$CLIENT_THREADS" \
  -c "$CLIENTS" -P "$PIPELINE" -n "$REQUESTS" -r "$HASH_KEYS" \
  HGET 'hash:__rand_int__' field000001 --csv

"$RB" -h "$HOST" -p "$PORT" --threads "$CLIENT_THREADS" \
  -c "$CLIENTS" -P "$PIPELINE" -n "$REQUESTS" -r "$HASH_KEYS" \
  HSET 'hash:__rand_int__' field000001 "$VALUE" --csv
```

注意：示例中的 shell 字符串必须在实际脚本中校验为精确 128 bytes。`redis-benchmark -d 128` 只应
用于其内置 SET/GET 数据生成，任意命令要显式传 payload。

`redis-benchmark` 以 request 数控制正式轮次，不提供严格的“先预热 30 秒再只统计后 60 秒”单次
窗口。正确做法是：

1. 单独运行 30 秒预热，丢弃该进程全部结果；
2. 用短校准轮估算 QPS；
3. 令正式 `-n` 足够使该轮持续至少 60 秒；
4. 保存 `--csv` 输出，其中当前版本包含 avg/min/P50/P95/P99/max；
5. 不使用第一个预热进程的数据，也不截取瞬时峰值。

不要用 `redis-benchmark MULTI SET ... EXEC` 伪装事务。命令行参数会形成一个 RESP command，不能
自动形成跨请求、有连接状态的正确事务序列。

### 11.2 memtier_benchmark：并发、pipeline 和混合 workload

安装后先归档帮助和版本：

```bash
memtier_benchmark --version
memtier_benchmark --help > memtier-help.txt
```

1000 clients 示例：10 load threads × 每线程 100 clients。

```bash
memtier_benchmark \
  --server="$HOST" --port="$PORT" --protocol=redis \
  --threads=10 --clients=100 \
  --pipeline="$PIPELINE" \
  --test-time=60 --run-count=1 \
  --ratio=1:9 \
  --key-minimum=0 --key-maximum=9999999 \
  --key-pattern=R:R \
  --data-size=128 --random-data \
  --json-out-file="result.json" \
  --out-file="result.txt"
```

2000 clients 使用 `--threads=10 --clients=200`，而不是把 `--clients` 错写成总连接数。

任意命令能力随 memtier 版本变化。固定版本后，使用该版本帮助确认以下选项，再生成五种结构 workload：

```text
--command="HGET hash:__key__ field000001"
--command-ratio=9
--command-key-pattern=R

--command="HSET hash:__key__ field000001 __data__"
--command-ratio=1
--command-key-pattern=R
```

每个 `--command-ratio` 和 `--command-key-pattern` 属于它前面的 command。正式运行前抓取 100 个请求，
验证实际命令比例、key 范围、命中率和 payload 长度。若固定的 memtier release 不支持所需 arbitrary
command 行为，不得静默换成 GET/SET；保留 memtier 做并发/pipeline 主测试，并由事务/结构驱动器补齐。

### 11.3 两个工具不能互相替代

| 工具              | 主职责                                              | 限制                                                 |
| ----------------- | --------------------------------------------------- | ---------------------------------------------------- |
| redis-benchmark   | 单命令、内置 GET/SET、任意命令微基准                | 混合比例和有状态事务能力有限                         |
| memtier_benchmark | 高 client、time-based、pipeline、混合比例、JSON/HDR | client 数是每线程；版本间 arbitrary command 能力不同 |
| 自定义 RESP 驱动  | 精确事务序列、多 key shard width、批次延迟          | 必须先证明 load generator 自身不成为瓶颈             |

所有主报告必须同时包含 redis-benchmark 和 memtier 结果，不能把两者 QPS 直接相减，因为其请求生成、
计时和延迟定义不同。只在同工具、同版本、同参数、同 workload 内做产品比值。

## 12. MULTI/EXEC 专项

### 12.1 必测事务

**TX1：String 单 key，两条业务命令**

```text
MULTI
SET txn:<id> <128B>
GET txn:<id>
EXEC
```

**TX2：Hash 单 key，两条业务命令**

```text
MULTI
HSET txh:<id> field000001 <128B>
HGET txh:<id> field000001
EXEC
```

**TX3：四个不同 key，写读混合**

```text
MULTI
SET tx:<id>:0 <128B>
SET tx:<id>:1 <128B>
GET tx:<id>:2
GET tx:<id>:3
EXEC
```

**TX4：八个不同 key，纯写 batch**

```text
MULTI
SET tx:<id>:0 <128B>
SET tx:<id>:1 <128B>
...
SET tx:<id>:7 <128B>
EXEC
```

TX3/TX4 分别预生成 Dragonfly shard width=1、2、4、8 的 key 文件。不能仅凭 key 名猜测 shard；应使用
与当前 commit 一致的 hash 逻辑离线生成，并通过 Dragonfly `INFO` 中 `tx_with_freq` 或 metrics 的
`transaction_widths_total` 增量验证实际 width。

这里的 shard width 是一笔事务实际触达的内部分片数量，不是服务端线程配置；服务端始终只使用 8 或
16 线程。

**TX5：冲突事务**

TX1 的 100%、10%、1% keyspace 三档，用于测锁冲突和热点尾延迟。

### 12.2 pipeline 与事务不是同一概念

分别测试并命名：

1. `SET; GET`，每条等回复后再发下一条；
2. 非事务 pipeline：一次发送 N 组 `SET; GET`；
3. 单个 `MULTI ... EXEC`；
4. pipeline 中连续发送 N 个完整事务，每个事务边界不得交错；
5. 单个包含 2/4/8 条业务命令的大事务。

pipeline 提升来自减少 RTT/系统调用等待；`MULTI/EXEC` 还包含排队、原子执行和回复数组成本。不能将
两者合并为一个“batch QPS”。

### 12.3 事务驱动器验收

固定 memtier 版本若明确支持完整事务 rotation，可在小流量抓包验证后使用；否则编写异步 C++/Rust/Go
RESP 驱动器。驱动器必须：

- 每个 connection 严格发送 `MULTI`、业务命令、`EXEC`；
- 验证 `MULTI` 回复 OK、业务命令回复 QUEUED、EXEC 数组长度和内容；
- 事务 pipeline 时只允许完整事务连续排队，不能交错两个事务的子命令；
- 同时报 transactions/s、business commands/s 和 wire commands/s；
- 对 TX1，1 transaction = 4 wire commands = 2 business commands；
- 记录 transaction wall latency，而不只记录每条 QUEUED latency；
- 使用 HDR Histogram 输出 P50/P95/P99/P99.9/max；
- 先对 dummy/最快服务做 loadgen ceiling 测试，确保 CPU 和单线程不饱和；
- 固定随机 seed，并能重放同一 key 序列到两个产品。

Lua/EVAL batch 可作为独立场景，但不能代替 `MULTI/EXEC`，因为二者调度和执行语义不同。

## 13. 每轮标准流程

### 13.1 Case ID

每个 case 使用唯一 ID：

```text
<product>-<commit>-t<server_threads>-<tool>-<structure>-<operation>
-c<total_clients>-p<pipeline>-<distribution>-r<repeat>
```

示例：

```text
dragonfly-0ab6e227-t8-memtier-hash-hget90_hset10-c1000-p16-uniform-r3
```

### 13.2 执行步骤

1. 确认没有残留 server、benchmark 和采集进程；
2. 清理上一个实例，等待内存回收，记录 `MemAvailable`；
3. 以固定 CPU/NUMA 策略启动目标服务；
4. 验证版本、配置、线程、proactor、shard、持久化和 eviction；
5. `FLUSHALL` 后装载规范数据集；
6. 验证 DBSIZE、各结构元素数、抽样 payload、命中率和内存；
7. 建立全部连接，运行 30 秒预热，丢弃全部预热统计；
8. 等待 5 秒，使采集窗口边界清晰；
9. 启动系统采集；
10. 正式运行 60 秒；尾延迟关键 case 增加一轮 300 秒；
11. 停止采集，抓取 INFO/metrics、错误数、命令数和内存；
12. 校验 benchmark operations 与服务端 commandstats 数量；
13. 保存原始文件，只追加解析后的汇总，禁止覆盖；
14. 停止服务，检查日志和 `dmesg`；
15. 至少重复 5 次，按交错顺序切换产品。

装载阶段不能算入正式 CPU、QPS 和 latency。写入型 case 若改变数据状态，必须按规范恢复，不得直接用
上一轮结束状态开始下一轮。

## 14. 资源采集

### 14.1 推荐采集命令

需要安装 `sysstat`。以下命令与 benchmark 同时开始，输出到 case 目录：

```bash
pidstat -h -u -r -w -t -p "$SERVER_PID" 1 > pidstat.txt &
mpstat -P ALL 1 > mpstat.txt &
sar -n DEV,EDEV,TCP,ETCP 1 > sar-net.txt &
vmstat -w 1 > vmstat.txt &

perf stat -p "$SERVER_PID" \
  -e task-clock,cycles,instructions,cache-references,cache-misses,\
context-switches,cpu-migrations,page-faults \
  -o perf-stat.txt -- sleep 60 &
```

当前 `kernel.perf_event_paranoid=4`，`perf` 需要 root 或经批准的临时权限调整。若无法采集，报告必须写
“缺失”，不能填 0。

开始和结束各抓取：

```bash
cat /proc/$SERVER_PID/status
cat /proc/$SERVER_PID/smaps_rollup
cat /proc/$SERVER_PID/io
ss -tinp
ip -s link
```

服务端指标：

- Redis：`INFO all`、`INFO commandstats`、`INFO latencystats`、`INFO memory`；
- Dragonfly：`INFO all` 和 `/metrics`；重点保存 transaction width、squash width、hop、queue；
- 两者：进程总 CPU、每线程 CPU、RSS/PSS、minor/major faults、context switch、网络 bytes/packets；
- 主机：每 logical CPU user/system/softirq/iowait/steal、run queue、swap、NIC drop/retransmit。

### 14.2 CPU 口径

Linux 进程 `%CPU=800%` 表示平均使用 8 个 logical CPU。统一换算：

```text
cpu_cores_used = process_cpu_percent / 100
host_cpu_fraction = process_cpu_percent / 1600
qps_per_cpu_core = operations_per_second / cpu_cores_used
```

同时绘制每线程 CPU。Redis 如果主线程接近 100% 而 I/O threads 尚未满，说明命令执行形成瓶颈；
Dragonfly 如果多个 shard threads 均衡接近满载，才说明 key 分布成功利用多核。

## 15. 有效性门槛和作废规则

正式 case 必须全部满足：

- benchmark error、timeout、unexpected reply 为 0；
- 服务端无 crash、OOM、eviction、snapshot、AOF、replication 活动；
- hit workload 实际命中率 ≥ 99.9%，miss workload 与目标误差 ≤ 0.5%；
- 数据集开始和结束的逻辑元素数符合 workload 定义；
- 正式窗口至少 60 秒，且不包含连接建立和预热；
- load generator 未饱和；
- 网络无 drop，retransmit 无异常，带宽未达到链路瓶颈；
- 无 swap in/out，无 major fault 风暴；
- KVM steal 平均 ≤ 1%，单秒尖峰需披露；
- 无其他进程持续占用任一目标物理核超过 2%；
- 5 轮 QPS 的变异系数 CV ≤ 5%。

若 CV > 5%，先排查热降频、steal、loadgen、后台任务和数据状态，再扩展到 10 轮。不得删除不利样本；
只有明确记录技术原因的 invalid run 可以作废。

## 16. 统计和派生指标

每个 case 以 5 轮中位数为主值，同时报告 min/max、IQR 和 CV。提升比例统一为：

```text
throughput_gain_pct = (Dragonfly_QPS / Redis_QPS - 1) × 100%
latency_change_pct = (Dragonfly_P99 / Redis_P99 - 1) × 100%
speedup_16_vs_8 = QPS_16 / QPS_8
scaling_efficiency_16_vs_8 = (QPS_16 / QPS_8) / (16 / 8)
redis_speedup_12_vs_8 = Redis_QPS_12 / Redis_QPS_8
cpu_efficiency_gain =
  (Dragonfly_QPS_per_core / Redis_QPS_per_core - 1) × 100%
```

当 Redis 8/12/16 和 Dragonfly 8/16 中的最佳线程数不同时，可以比较各自最优配置，但必须同时给出
8 对 8、16 对 16，以及 Redis 8/12/16 内部曲线。不得只挑 Dragonfly 最优对 Redis 非最优，或反之。

统计 transactions/s 时不要直接与 command QPS 放在同一列。建议同时输出：

| Case | tx/s | business ops/s | wire cmd/s | P99 tx latency | CPU cores |
| ---- | ---: | -------------: | ---------: | -------------: | --------: |

## 17. 最终图表和报告模板

### 17.1 必须输出的图

1. Redis 8/12/16、Dragonfly 8/16 线程 QPS 对比，按 GET/SET/mixed 分面；
2. 16 相对 8 线程的 speedup 和 scaling efficiency；
3. server CPU cores vs threads；
4. QPS/CPU-core vs threads；
5. P99 latency vs throughput 的等负载曲线；
6. pipeline 1/16/64 的 QPS 和 P99；
7. 五种数据结构的吞吐与 P99；
8. uniform、hotset、single-hot-key 的退化幅度；
9. transaction shard width 1/2/4/8 的 tx/s 和 P99；
10. RSS 和 bytes/logical-element；
11. Redis 主线程/I/O threads 与 Dragonfly 各 worker 的 CPU 热图；
12. 长跑 300 秒的每秒 QPS、P99、CPU 和 RSS 时间线。

### 17.2 基础命令表

| Command/语义      | Redis QPS | Dragonfly QPS | 吞吐差异 | Redis P99 | Dragonfly P99 | CPU-core 差异 |
| ----------------- | --------: | ------------: | -------: | --------: | ------------: | ------------: |
| GET hit           |           |               |          |           |               |               |
| SET update        |           |               |          |           |               |               |
| HGET hit          |           |               |          |           |               |               |
| HSET update       |           |               |          |           |               |               |
| LINDEX            |           |               |          |           |               |               |
| LPUSH+LPOP steady |           |               |          |           |               |               |
| SISMEMBER hit     |           |               |          |           |               |               |
| SADD insert       |           |               |          |           |               |               |
| ZRANGE 0 9        |           |               |          |           |               |               |
| ZADD update       |           |               |          |           |               |               |

### 17.3 线程扩展表

| Product   | Threads | QPS | Speedup vs 8 | Scaling efficiency | CPU cores | QPS/core | P99 |
| --------- | ------: | --: | -----------: | -----------------: | --------: | -------: | --: |
| Redis     |       8 |     |         1.00 |               1.00 |           |          |     |
| Redis     |      16 |     |              |                    |           |          |     |
| Dragonfly |       8 |     |         1.00 |               1.00 |           |          |     |
| Dragonfly |      16 |     |              |                    |           |          |     |

### 17.4 事务表

| Transaction      | Dragonfly shard width | Redis tx/s | Dragonfly tx/s | 差异 | Redis P99 | Dragonfly P99 |
| ---------------- | --------------------: | ---------: | -------------: | ---: | --------: | ------------: |
| TX1 String       |                     1 |            |                |      |           |               |
| TX2 Hash         |                     1 |            |                |      |           |               |
| TX3 multi-key    |                     1 |            |                |      |           |               |
| TX3 multi-key    |                     2 |            |                |      |           |               |
| TX4 multi-key    |                     4 |            |                |      |           |               |
| TX4 multi-key    |                     8 |            |                |      |           |               |
| TX5 hot/conflict |                actual |            |                |      |           |               |

### 17.5 最佳线程配置建议必须分三类

最终不能只写一个“最佳线程数”，而应分别给出：

- **峰值吞吐优先**：最大 QPS 且 P99 未越过业务 SLO；
- **尾延迟优先**：在目标 QPS 下 P99/P99.9 最低；
- **成本效率优先**：QPS/CPU-core 最高，且有足够 headroom。

对当前 8 physical core / 16 logical CPU 机器，应明确 8→12→16 是 SMT 扩展段。如果 12/16 的 QPS
不高于 8、P99 或 CPU 效率更差，建议保留逻辑 CPU 给 kernel、NIC 中断和旁路任务。

## 18. 结果解释边界

- Redis I/O thread 增益不等于 Redis 已实现数据结构并行执行；必须结合主线程 CPU 证明；
- Dragonfly uniform key 的扩展不能外推到单热点 key；必须同时展示热点退化；
- pipeline=64 的高 QPS 不能代表单请求延迟；
- 1000/2000 clients 的尾延迟包含排队效应，不能替代低并发延迟；
- ZRANGE/LPOP 等响应大小或数据状态不同的结果不可比较；
- 纯内存无持久化结果不能外推到 AOF `everysec`；
- loopback 结果不能外推到真实网络；
- 当前 KVM 虚拟机结果应说明宿主争用和虚拟 NIC 的不确定性；
- 当前源码 snapshot 结果不能被表述为稳定版本结论；
- 一次 benchmark 是工具与服务端组合的结果，不是服务端的绝对常数。

## 19. 建议执行顺序和时间预算

1. 冻结版本、初始化 Helio、构建 release、安装固定 memtier/sysstat；
2. 完成 P0，预计半天；
3. 容量试装和五种结构数据生成，预计半天到一天；
4. P1 client 曲线，预计半天；
5. P2 的 Redis 8/12/16、Dragonfly 8/16 线程对比，预计半天；
6. P3 五结构和 pipeline，预计 1~2 天；
7. 事务、热点和等吞吐延迟，预计一天；
8. 失败重跑、300 秒长跑和统计复核，预计半天到一天；
9. 生成图表与《Redis vs Dragonfly 单机性能测试与架构分析报告》；
10. 由第二人按原始数据抽查至少 10% case。

完整高可信方案预计需要 3~5 天机器时间，而不是一次 `redis-benchmark -t set,get`。若时间受限，优先级
依次为 P0、P2、P1、P3、事务和热点；不得通过减少重复轮次到 1 来制造“完整”假象。

## 20. 测试完成检查清单

### 版本和环境

- [ ] 两个产品均为明确的稳定 tag 或明确的 current-source SHA；
- [ ] 两者均为 release build，编译器和优化参数已归档；
- [ ] Dragonfly Helio SHA、Redis jemalloc 版本已记录；
- [ ] OS/kernel/CPU/NUMA/SMT/内存/NIC/虚拟化已记录；
- [ ] swap、steal、governor、THP、CPU affinity 已验证；
- [ ] load generator ceiling 和网络 ceiling 已验证。

### 数据与正确性

- [ ] 10M 逻辑元素经过容量试装，或报告已标明缩小后的统一规模；
- [ ] 五种结构的顶层 key、元素数、payload 和编码已记录；
- [ ] hit/miss、响应大小和命令返回值已抽样校验；
- [ ] List/Set/ZSet 写入没有退化为耗尽、no-op 或无限增长；
- [ ] 无 eviction、持久化、复制和 Cluster 活动。

### 测试执行

- [ ] redis-benchmark 与 memtier_benchmark 均已使用；
- [ ] 预热与正式窗口完全分离；
- [ ] 每个正式 case 至少 5 轮且产品顺序交错；
- [ ] low concurrency、1000/2000 clients、pipeline 1/16/64 均覆盖；
- [ ] Redis 8/12/16 I/O threads 已覆盖；
- [ ] Dragonfly 8/16 threads/shards 已覆盖；
- [ ] uniform、hotset、single-hot-key 均覆盖；
- [ ] 事务按 transaction 边界计时，并验证 Dragonfly shard width；
- [ ] 所有 invalid run 都有原因，没有选择性删除样本。

### 报告

- [ ] 同时报告 QPS、P95/P99/P99.9、CPU cores、QPS/core、RSS 和错误数；
- [ ] 同时有极限吞吐比较和等吞吐延迟比较；
- [ ] 区分 operations/s、transactions/s、business ops/s 和 wire commands/s；
- [ ] 区分 8 物理核段与 8→12→16 SMT 段；
- [ ] 最佳配置按吞吐、尾延迟、成本效率分别建议；
- [ ] 架构结论均有线程 CPU、指标或曲线证据；
- [ ] 原始数据、命令、配置、日志和解析脚本可复现。
