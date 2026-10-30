# Redis vs Dragonfly 双机性能测试与架构分析报告

> 测试日期：2026-08-24
> 服务端：`10.47.76.7`，单机、单进程、standalone、纯内存、TCP/RESP2
> 压测端：`10.47.76.3`，与服务端物理隔离
> 服务端线程：Redis 测试 8、12、16；Dragonfly 测试 8、16
> 测试方案：`docs/redis-vs-dragonfly-standalone-benchmark-plan.md`
> 本报告是新的双机测试报告；原同机报告未修改

## 1. 结论摘要

本轮将 `redis-benchmark` 和 `memtier_benchmark` 从服务端移到独立机器后，从零重新完成了 8/16
线程正式矩阵，并在相同环境追加 Redis 12 I/O threads 全矩阵。最重要的结论如下。

1. **原同机测试确实明显压制了 Dragonfly 16 的扩展能力。**在 1000 clients 混合读写中，
   Dragonfly 16 相对 Dragonfly 8：pipeline=1 为 -11.0%，pipeline=16 为 +23.2%，pipeline=64 为
   +33.7%。旧同机结果中 Dragonfly 16 的普遍大幅回退不能继续作为结论。
2. **Dragonfly 的多线程命令执行优势需要足够的并行工作量。**8 线程下，pipeline=1 吞吐仅比
   Redis 高 1.4%，P99 反而高 32.8%；pipeline=16/64 吞吐分别高 30.9%/80.6%。16 线程下，
   pipeline=16/64 吞吐分别高 140.1%/190.0%，且 P99 分别低 40.1%/58.8%。
3. **Redis 12 和 16 均没有优于 Redis 8。**Redis 12 在 pipeline=1/16/64 相对 Redis 8 分别回退
   4.3%/26.7%/8.5%；Redis 16 分别回退 8.1%/32.8%/16.7%。memtier 中 12 线程结果位于 8 与 16
   之间，说明 Redis 回退不能只归因于同机 load generator；主线程命令执行上限、I/O 线程交接和
   SMT sibling 竞争仍是主要因素。
4. **pipeline=1 不是 Dragonfly 的优势区。**远程 RTT、每命令协议与 shard 路由成本无法被批量摊薄，
   且压测端 8 个 memtier worker 已使用约 6.9--7.9 cores。此时两者吞吐接近，Redis 的精简串行路径
   在部分命令和 16 线程配置下略快；不能把这组结果解释为 Dragonfly 服务端的最大单请求能力。
5. **五种数据结构呈现相同趋势。**pipeline=1 时，Dragonfly 8 对 Redis 8 的 11 个命令几何平均优势
   仅 4.2%，主要由 LPUSH/LPOP 拉高；pipeline=16/64 的几何平均优势扩大为 100.0%/161.3%。
   Dragonfly 16 在 pipeline=16/64 相对自身 8 线程的几何平均增益为 25.4%/15.8%。
6. **单 key 小事务仍是 Redis 更强。**按 `EXEC` 完成数计算，Redis 8 为 114,761 tx/s，Dragonfly 8
   为 102,059 tx/s，Dragonfly 低 11.1%；Dragonfly 的 EXEC P99 为 74.751 ms，明显高于 Redis 的
   7.231 ms。拆分客户端后吞吐差距较旧同机测试明显缩小，但尾延迟问题仍存在。
7. **当前 16-logical-CPU 机器的建议配置：**低 pipeline、低尾延迟业务优先选 8 线程；Dragonfly
   在 pipeline>=16 且追求吞吐时可选 16 线程；Redis 建议 8 I/O threads，12/16 均无收益。

本轮只有一个 formal repeat，结果适合用于修正同机测试结论和指导下一轮验证，但还不能替代 3--5 轮
中位数、等 offered-load 延迟和 300 秒稳定性测试。

## 2. 测试门槛与数据完整性

| 检查项                     | 本轮结果                                                                |
| -------------------------- | ----------------------------------------------------------------------- |
| 服务部署                   | 仅服务端`10.47.76.7` 启动一个目标实例                                 |
| 压测部署                   | 工具仅运行在独立客户端`10.47.76.3`                                    |
| 服务端线程档位             | Redis 8/12/16；Dragonfly 8/16                                            |
| memtier 正式 case          | 25 个：20 个 String + 5 个事务                                          |
| memtier 正式时长           | 25/25 均满足；实际 60.013--60.040 秒                                    |
| redis-benchmark 正式 case  | 165 个                                                                  |
| redis-benchmark 请求数     | 每 case 精确 10,000,000，总计 16.5 亿                                   |
| memtier String 操作数      | 1,141,449,355                                                           |
| memtier 事务 wire commands | 124,439,594                                                             |
| 正式操作总计               | 2,915,888,949（不含预载、预热）                                         |
| Connection Errors          | 0                                                                       |
| hit workload Misses/sec    | 0                                                                       |
| 事务 Aborts/sec            | 0                                                                       |
| redis-benchmark CSV        | 165/165 有效，均有吞吐与延迟字段                                        |
| Dragonfly benchmark stderr | 66 个 case 仅有`Could not fetch server CONFIG` 兼容性警告，无请求失败 |
| 原始文件                   | 2,974 个，未压缩约 56 MiB                                               |
| swap                       | 两端均关闭，采集期间未发现 swap I/O                                     |

memtier 以“至少 60 秒”满足硬门槛；redis-benchmark 以“至少 1000 万请求”满足硬门槛。30 秒预热和
数据预载均单独执行，不计入正式结果。

## 3. 双机环境与网络验收

### 3.1 拓扑

```text
10.47.76.3（压测客户端）
  redis-benchmark / memtier_benchmark
              |
              | TCP/RESP2，10.47.76.0/24
              v
10.47.76.7:6380（服务端）
  每次只启动 Redis 或 Dragonfly 其中一个实例
```

| 项目       | 服务端`10.47.76.7`              | 客户端`10.47.76.3`              |
| ---------- | --------------------------------- | --------------------------------- |
| CPU        | Intel Xeon Cascade Lake           | Intel Xeon Cascade Lake           |
| CPU 拓扑   | 8 physical / 16 logical，SMT=2    | 8 physical / 16 logical，SMT=2    |
| 虚拟化     | KVM                               | KVM                               |
| NUMA       | 1 个可见 node，CPU 0-15           | 1 个可见 node，CPU 0-15           |
| 内存       | 31 GiB，测试前约 20 GiB available | 31 GiB，测试前约 28 GiB available |
| Kernel     | 5.15.0-124-generic                | 5.15.0-187-generic                |
| 网卡地址   | eth0`10.47.76.7/24`             | eth0`10.47.76.3/24`             |
| 文件描述符 | 服务和脚本使用 65,535             | 压测脚本使用 65,535               |
| swap       | 0 B                               | 0 B                               |

两端 CPU 型号和拓扑一致，但 kernel patch level 不同；由于两产品始终在同一服务端上交替运行，这不影响
产品间服务端公平性。8 线程绑到 `0,2,4,6,8,10,12,14`，即每个物理核一个 logical CPU；12 线程
绑到 `0-4,6,8-12,14`，两个 socket 各 6 个 logical CPU；16 线程使用 `0-15`。因此 8→12→16
主要测量逐步启用 SMT sibling 的效果，不是物理核增加。

### 3.2 网络基线

| 检查项                    |                          结果 |
| ------------------------- | ----------------------------: |
| ICMP RTT average          |                   约 0.293 ms |
| 客户端 → 服务端 iperf3   |                   18.7 Gbit/s |
| 服务端 → 客户端 iperf3   |                   14.9 Gbit/s |
| 正式 memtier 最大双向流量 | 456,324 KiB/s，约 3.74 Gbit/s |
| 最大流量占较低方向基线    |                      约 25.1% |

因此主结果没有触及链路带宽上限。iperf 输出包含 TCP retransmission，说明这是虚拟化网络的实际可用
基线，不应把它当作无损物理 NIC；但正式负载与链路上限仍有足够余量。

## 4. 软件版本、构建与启动参数

本轮比较的是固定 current-source snapshots，不是“最新稳定版对最新稳定版”。报告不能将其标成稳定版
结果。

| 项目                    | 版本/标识                                                            |
| ----------------------- | -------------------------------------------------------------------- |
| Redis source            | `/home/redis/redis`                                                |
| Redis commit            | `de3ea25a9f486a44467cad0628bdb2d0a36aabf0`                         |
| Redis describe          | `8.10-m04-int-36-gde3ea25a9`                                       |
| Redis server            | 8.9.241，jemalloc 5.3.0                                              |
| Redis server SHA256     | `69813424e60e5eb15187cbed9a5f592faa1dd7188bcbc1a6a0f4d39adb1666fb` |
| Dragonfly source        | `/home/Dragonfly/dragonfly`                                        |
| Dragonfly commit        | `0ab6e227bb1286a9bda7b0438c9ea3abaf633e60`                         |
| Dragonfly describe      | `v1.40.0-74-g0ab6e227`                                             |
| Helio commit            | `3d8b4ece14ed62cb516e36afbc8947c0cc392826`                         |
| Dragonfly binary SHA256 | `6866da7b26cabea9b19f9ffe06f24688afcc0b4a33e91a5d06740194dde159d3` |

Dragonfly 使用 release build，关闭 AWS、GCP、Tiering、Search 和 SSL；运行时加载 GCC 14 runtime。
Redis 使用当前源码 release binary，关闭主动持久化。

服务启动的关键差异只有线程模型参数：

```text
Redis 8/12/16:
--bind 0.0.0.0 --port 6380 --protected-mode no
--save '' --appendonly no --maxmemory 8G --maxmemory-policy noeviction
--io-threads 8|12|16

Dragonfly 8/16:
--bind=0.0.0.0 --port=6380 --maxmemory=8G --cache_mode=false --dbfilename=
--proactor_threads=8|16 --num_shards=8|16
```

### 4.1 远端压测工具

安装目录为 `10.47.76.3:/home/panlei/benchmark-tools/`。

| 工具              | 版本               | SHA256                                                               |
| ----------------- | ------------------ | -------------------------------------------------------------------- |
| redis-benchmark   | 8.9.241            | `942f5612b7fbf8046d367c8f9332c209134a762e7d56a3129a23b438eab6b5b5` |
| redis-cli         | 8.9.241            | `e85027af29db4236d0123667a9224099231235b9e84dd2a0ae85a48e82dd4fe3` |
| memtier_benchmark | `sha=272eeb64:1` | `7892e138b5537bc4fd493ccb544a47c329370d603b24ff8d633d0b14988049cb` |

同一组客户端二进制和参数用于两个服务，避免工具版本偏差。

## 5. 测试方法

### 5.1 memtier String 混合读写

- 预载 1,000,000 keys，value 为 128 bytes；
- uniform random key，SET:GET=1:9，命中率 100%；
- 8 个 memtier worker；1000 clients 时每 worker 125 个连接，2000 时 250 个；
- pipeline=1/16/64；
- 每个服务配置先做独立 30 秒预热，再执行每个 60 秒正式 case；
- 记录 Totals ops/s、Avg、P50/P95/P99/P99.9、错误、客户端 CPU；
- 服务端同步记录 INFO、pidstat、mpstat、vmstat、sar、smaps_rollup。

### 5.2 redis-benchmark 五类结构

- 1000 clients、8 个 benchmark worker、每 case 10,000,000 requests；
- pipeline=1/16/64；
- String 使用 1,000,000 keys；Hash/List/Set/ZSet 使用 100,000 container keys；
- Hash、Set、ZSet 每个 container 预载 10 个 field/member；List 平均预载约 10 个元素；
- `HSET`、`SADD-existing`、`ZADD-update` 是更新已有 field/member，不测新增元素分配；
- `ZRANGE` 固定返回 0--9 共 10 个元素；
- 每档 pipeline 先 LPUSH 1000 万次再 LPOP 1000 万次，避免空 List 造成无效比较。

### 5.3 事务

事务 rotation 为：

```text
MULTI
SET tx:<key> <128-byte value>
GET tx:<key>
EXEC
```

每笔事务包含 4 条 wire commands、2 条业务命令；1000 clients、pipeline=1、60 秒。事务吞吐使用
`Execs Ops/sec`，尾延迟使用 EXEC 命令的回复延迟。它比 Totals 的四命令平均延迟更接近事务完成点，
但仍不是客户端从 MULTI 发出到 EXEC 完整回复的独立 wall-clock transaction latency。

## 6. memtier String 混合读写结果

每一行是独立 60 秒正式窗口。`Server cores` 是 pidstat 的进程 CPU/100，`Client cores` 是 memtier
自身进程统计。

| Product   | Threads | Clients | Pipeline |     Ops/s | Avg ms |    P50 |     P95 |     P99 |   P99.9 | Server cores | Client cores |
| --------- | ------: | ------: | -------: | --------: | -----: | -----: | ------: | ------: | ------: | -----------: | -----------: |
| Redis     |       8 |    1000 |        1 |   446,485 |  2.237 |  1.967 |   4.255 |   7.327 |  14.719 |        6.499 |        7.687 |
| Dragonfly |       8 |    1000 |        1 |   452,817 |  2.205 |  1.807 |   4.863 |   9.727 |  14.143 |        7.194 |        7.506 |
| Redis     |      12 |    1000 |        1 |   427,077 |  2.338 |  2.015 |   4.383 |   9.983 |  16.639 |        7.517 |        7.367 |
| Redis     |      16 |    1000 |        1 |   410,249 |  2.434 |  2.079 |   4.319 |  13.119 |  18.047 |        8.256 |        7.174 |
| Dragonfly |      16 |    1000 |        1 |   402,788 |  2.480 |  1.927 |   4.767 |  17.151 |  20.479 |       12.319 |        6.918 |
| Redis     |       8 |    1000 |       16 | 1,302,525 | 12.267 | 11.903 |  16.895 |  24.447 |  31.231 |        7.188 |        4.254 |
| Dragonfly |       8 |    1000 |       16 | 1,704,566 |  9.371 |  8.319 |  18.943 |  26.879 |  40.447 |        7.637 |        5.350 |
| Redis     |      12 |    1000 |       16 |   954,295 | 16.750 | 14.655 |  29.439 |  45.055 |  78.847 |       10.289 |        3.336 |
| Redis     |      16 |    1000 |       16 |   874,670 | 18.276 | 15.871 |  30.463 |  45.311 |  70.143 |       14.423 |        3.080 |
| Dragonfly |      16 |    1000 |       16 | 2,099,832 |  7.604 |  6.047 |  17.919 |  27.135 |  44.543 |       13.985 |        6.463 |
| Redis     |       8 |    1000 |       64 | 1,135,858 | 56.299 | 48.639 |  97.791 | 104.959 | 132.095 |        7.748 |        2.416 |
| Dragonfly |       8 |    1000 |       64 | 2,051,479 | 31.161 | 29.311 |  55.039 |  73.215 | 103.935 |        7.690 |        4.259 |
| Redis     |      12 |    1000 |       64 | 1,039,280 | 61.525 | 52.735 | 106.495 | 118.271 | 172.031 |       11.598 |        2.321 |
| Redis     |      16 |    1000 |       64 |   945,804 | 67.609 | 56.575 | 114.687 | 148.479 | 167.935 |       15.253 |        2.115 |
| Dragonfly |      16 |    1000 |       64 | 2,742,417 | 23.304 | 20.863 |  44.543 |  61.183 |  89.087 |       14.269 |        5.936 |
| Redis     |       8 |    2000 |        1 |   440,640 |  4.535 |  3.999 |   8.255 |  13.055 |  31.103 |        6.237 |        7.914 |
| Dragonfly |       8 |    2000 |        1 |   412,933 |  4.839 |  4.031 |  10.559 |  18.175 |  34.815 |        7.359 |        7.871 |
| Redis     |      12 |    2000 |        1 |   401,218 |  4.980 |  4.479 |   8.703 |  13.887 |  32.511 |        7.276 |        7.752 |
| Redis     |      16 |    2000 |        1 |   401,358 |  4.979 |  4.351 |   9.343 |  16.383 |  34.815 |        7.994 |        7.558 |
| Dragonfly |      16 |    2000 |        1 |   371,623 |  5.377 |  4.287 |  15.615 |  21.887 |  33.535 |       11.752 |        7.101 |

### 6.1 同线程数产品对比

| Threads | Pipeline | Redis ops/s | Dragonfly ops/s | Dragonfly QPS 差异 | Redis P99 | Dragonfly P99 | Dragonfly P99 差异 |
| ------: | -------: | ----------: | --------------: | -----------------: | --------: | ------------: | -----------------: |
|       8 |        1 |     446,485 |         452,817 |              +1.4% |     7.327 |         9.727 |             +32.8% |
|       8 |       16 |   1,302,525 |       1,704,566 |             +30.9% |    24.447 |        26.879 |              +9.9% |
|       8 |       64 |   1,135,858 |       2,051,479 |             +80.6% |   104.959 |        73.215 |             -30.2% |
|      16 |        1 |     410,249 |         402,788 |              -1.8% |    13.119 |        17.151 |             +30.7% |
|      16 |       16 |     874,670 |       2,099,832 |            +140.1% |    45.311 |        27.135 |             -40.1% |
|      16 |       64 |     945,804 |       2,742,417 |            +190.0% |   148.479 |        61.183 |             -58.8% |

在 pipeline=1，两者 QPS 差异只有约 ±2%，但 Dragonfly P99 高约 31%--33%。在 pipeline=16，
Dragonfly 16 已同时取得更高吞吐和更低 P99；pipeline=64 的优势进一步扩大。Redis 8 的最佳吞吐点
仍是 pipeline=16，增至 64 后吞吐下降 12.8%、P99 增加 329.3%。Dragonfly 16 从 pipeline=16 增至
64 时吞吐再增加 30.6%，但 P99 增加 125.5%，应由业务 SLO 决定是否值得。

### 6.2 8→12→16 线程扩展

| Product   | Pipeline | 8 threads ops/s | 12 threads ops/s | 16 threads ops/s | 12 vs 8 | 16 vs 8 |
| --------- | -------: | --------------: | ---------------: | ---------------: | ------: | ------: |
| Redis     |        1 |         446,485 |          427,077 |          410,249 |   -4.3% |   -8.1% |
| Redis     |       16 |       1,302,525 |          954,295 |          874,670 |  -26.7% |  -32.8% |
| Redis     |       64 |       1,135,858 |        1,039,280 |          945,804 |   -8.5% |  -16.7% |
| Dragonfly |        1 |         452,817 |              N/A |          402,788 |     N/A |  -11.0% |
| Dragonfly |       16 |       1,704,566 |              N/A |        2,099,832 |     N/A |  +23.2% |
| Dragonfly |       64 |       2,051,479 |              N/A |        2,742,417 |     N/A |  +33.7% |

Redis 12 在三档 pipeline 都处于 Redis 8 与 Redis 16 之间，但没有形成优于 8 线程的新峰值。随着
I/O threads 从 8 增至 12、16，p16 的吞吐呈 1.303M → 0.954M → 0.875M 单调下降，说明额外 I/O
线程在当前 8 个物理核上主要增加 SMT、交接和调度成本。关键事实仍是：拆分客户端后 Dragonfly 在
p16/p64 获得明确 SMT 增益，而 Redis 没有。

### 6.3 1000→2000 clients

| Product   | Threads |                    QPS 变化 |                   P99 变化 |
| --------- | ------: | --------------------------: | -------------------------: |
| Redis     |       8 | 446,485 → 440,640（-1.3%） |  7.327 → 13.055（+78.2%） |
| Dragonfly |       8 | 452,817 → 412,933（-8.8%） |  9.727 → 18.175（+86.9%） |
| Redis     |      12 | 427,077 → 401,218（-6.1%） |  9.983 → 13.887（+39.1%） |
| Redis     |      16 | 410,249 → 401,358（-2.2%） | 13.119 → 16.383（+24.9%） |
| Dragonfly |      16 | 402,788 → 371,623（-7.7%） | 17.151 → 21.887（+27.6%） |

2000 clients 没有提升吞吐，只增加排队和尾延迟。当前矩阵中 1000 clients 已足以达到高并发工作点。

### 6.4 CPU 利用率与效率

| Product   | Threads | Pipeline | Server cores | Client cores | Kops/s/server-core |
| --------- | ------: | -------: | -----------: | -----------: | -----------------: |
| Redis     |       8 |        1 |        6.499 |        7.687 |               68.7 |
| Redis     |       8 |       16 |        7.188 |        4.254 |              181.2 |
| Redis     |       8 |       64 |        7.748 |        2.416 |              146.6 |
| Redis     |      12 |        1 |        7.517 |        7.367 |               56.8 |
| Redis     |      12 |       16 |       10.289 |        3.336 |               92.7 |
| Redis     |      12 |       64 |       11.598 |        2.321 |               89.6 |
| Redis     |      16 |        1 |        8.256 |        7.174 |               49.7 |
| Redis     |      16 |       16 |       14.423 |        3.080 |               60.6 |
| Redis     |      16 |       64 |       15.253 |        2.115 |               62.0 |
| Dragonfly |       8 |        1 |        7.194 |        7.506 |               62.9 |
| Dragonfly |       8 |       16 |        7.637 |        5.350 |              223.2 |
| Dragonfly |       8 |       64 |        7.690 |        4.259 |              266.8 |
| Dragonfly |      16 |        1 |       12.319 |        6.918 |               32.7 |
| Dragonfly |      16 |       16 |       13.985 |        6.463 |              150.1 |
| Dragonfly |      16 |       64 |       14.269 |        5.936 |              192.2 |

pipeline=1 的 memtier client 已接近 8 个 worker cores 满载，因此这组 QPS 可能同时受客户端生成能力和
远程逐请求往返限制。pipeline 增大后客户端每 operation 的系统调用成本降低，服务端差异才更清楚。

线程级 pidstat 进一步显示：

| Product   | Threads |    Pipeline |           主执行/Proactor 平均 CPU |   I/O thread 平均 CPU |
| --------- | ------: | ----------: | ---------------------------------: | --------------------: |
| Redis     |       8 | 1 / 16 / 64 |              87.8% / 96.6% / 96.9% | 80.3% / 88.9% / 96.8% |
| Redis     |      12 | 1 / 16 / 64 |              90.8% / 90.4% / 96.8% | 59.0% / 83.8% / 96.6% |
| Redis     |      16 | 1 / 16 / 64 |              91.2% / 95.0% / 95.7% | 48.1% / 88.3% / 95.3% |
| Dragonfly |       8 | 1 / 16 / 64 | 89.9% / 95.5% / 96.1% per proactor |                   N/A |
| Dragonfly |      16 | 1 / 16 / 64 | 77.0% / 87.4% / 89.2% per proactor |                   N/A |

Redis 主线程在 p16/p64 已稳定接近一个逻辑 CPU 满载；额外 I/O threads 能并行 socket read/write 和
解析，但不能让数据结构命令并行执行。Dragonfly 的 proactor/shard 线程同时承担 I/O 和命令执行，
pipeline 足够大时能把工作分散到更多执行线程。

## 7. redis-benchmark 五种数据结构结果

表中格式为 `QPS / P99(ms)`；`Dfly Δ` 为同线程数下 Dragonfly 相对 Redis 的吞吐差异。

| Pipeline | Command       | Redis 8 QPS / P99 | Dragonfly 8 QPS / P99 | Dfly 8 Δ | Redis 16 QPS / P99 | Dragonfly 16 QPS / P99 | Dfly 16 Δ |
| -------: | ------------- | ----------------: | --------------------: | --------: | -----------------: | ---------------------: | ---------: |
|        1 | SET           |    395116 / 5.327 |        395023 / 7.327 |     -0.0% |    362963 / 10.167 |        353170 / 11.199 |      -2.7% |
|        1 | GET           |    376265 / 5.591 |        365912 / 6.359 |     -2.8% |    349920 / 10.967 |        332447 / 11.967 |      -5.0% |
|        1 | HGET          |    362687 / 5.303 |        365925 / 6.463 |     +0.9% |     350385 / 5.039 |        332469 / 13.407 |      -5.1% |
|        1 | HSET-update   |    391144 / 5.383 |        395179 / 7.527 |     +1.0% |     344234 / 5.727 |        359350 / 13.199 |      +4.4% |
|        1 | LINDEX        |    369208 / 5.575 |        369413 / 6.855 |     +0.1% |     353282 / 8.647 |        340971 / 14.023 |      -3.5% |
|        1 | LPUSH         |    305083 / 5.551 |        373106 / 9.199 |    +22.3% |     293893 / 5.631 |        341146 / 15.255 |     +16.1% |
|        1 | LPOP          |    285519 / 6.127 |        362884 / 7.695 |    +27.1% |     266468 / 6.295 |        332480 / 10.903 |     +24.8% |
|        1 | SISMEMBER     |    318939 / 4.783 |        316496 / 6.887 |     -0.8% |     309176 / 9.743 |        288967 / 12.047 |      -6.5% |
|        1 | SADD-existing |    352808 / 5.239 |        353058 / 7.023 |     +0.1% |     335267 / 9.527 |        306758 / 13.767 |      -8.5% |
|        1 | ZRANGE-0-9    |    356455 / 5.087 |        362739 / 7.623 |     +1.8% |     344329 / 5.775 |        337975 / 13.047 |      -1.8% |
|        1 | ZADD-update   |    316426 / 4.663 |        318969 / 5.511 |     +0.8% |     311604 / 8.535 |        291011 / 12.399 |      -6.6% |
|       16 | SET           |   973710 / 24.623 |      1477978 / 30.239 |    +51.8% |    887311 / 25.919 |       1656452 / 25.023 |     +86.7% |
|       16 | GET           |  1140901 / 19.519 |      1663063 / 26.575 |    +45.8% |   1023856 / 21.615 |       1727414 / 37.695 |     +68.7% |
|       16 | HGET          |   643791 / 30.511 |      1376273 / 46.143 |   +113.8% |    578503 / 35.903 |       1808645 / 24.815 |    +212.6% |
|       16 | HSET-update   |   554447 / 39.231 |      1247194 / 47.999 |   +124.9% |    518457 / 39.871 |       1730104 / 24.719 |    +233.7% |
|       16 | LINDEX        |   831601 / 24.703 |      1596934 / 25.167 |    +92.0% |    798594 / 29.439 |       1892148 / 20.783 |    +136.9% |
|       16 | LPUSH         |   395210 / 48.287 |      1079098 / 49.087 |   +173.0% |    373037 / 55.359 |       1533272 / 32.959 |    +311.0% |
|       16 | LPOP          |   341146 / 63.743 |      1140251 / 46.527 |   +234.2% |    341169 / 62.367 |       1662510 / 29.231 |    +387.3% |
|       16 | SISMEMBER     |  1078865 / 21.631 |      1812251 / 24.511 |    +68.0% |   1023646 / 22.639 |       2091175 / 17.839 |    +104.3% |
|       16 | SADD-existing |  1050530 / 19.535 |      1659751 / 23.727 |    +58.0% |   1023437 / 22.367 |       2209456 / 23.791 |    +115.9% |
|       16 | ZRANGE-0-9    |   570353 / 40.447 |      1208898 / 34.335 |   +112.0% |    587096 / 39.039 |       1590078 / 29.791 |    +170.8% |
|       16 | ZADD-update   |   886918 / 26.623 |      1734305 / 27.247 |    +95.5% |    907276 / 24.543 |       1985703 / 16.911 |    +118.9% |
|       64 | SET           |   812480 / 86.463 |      1766472 / 81.663 |   +117.4% |   686955 / 122.303 |       1759015 / 69.567 |    +156.1% |
|       64 | GET           |  1075963 / 74.111 |      1983340 / 69.695 |    +84.3% |   1048328 / 91.263 |       2303086 / 50.463 |    +119.7% |
|       64 | HGET          |  585926 / 107.519 |      1726818 / 86.399 |   +194.7% |   531321 / 128.959 |       2194908 / 67.583 |    +313.1% |
|       64 | HSET-update   |  545703 / 132.735 |      1772421 / 84.863 |   +224.8% |   474383 / 169.855 |       1755310 / 91.775 |    +270.0% |
|       64 | LINDEX        |   737518 / 99.071 |      1889288 / 90.815 |   +156.2% |   686672 / 121.791 |       2290426 / 48.767 |    +233.6% |
|       64 | LPUSH         |  376011 / 178.687 |     1418238 / 178.431 |   +277.2% |   343713 / 229.375 |       1850139 / 67.519 |    +438.3% |
|       64 | LPOP          |  332292 / 270.591 |      1420454 / 96.127 |   +327.5% |   318989 / 286.719 |       2073398 / 78.015 |    +550.0% |
|       64 | SISMEMBER     |   970685 / 71.167 |      2065262 / 79.935 |   +112.8% |   812678 / 111.231 |       2028398 / 40.255 |    +149.6% |
|       64 | SADD-existing |   947688 / 73.087 |      1887505 / 93.887 |    +99.2% |    829669 / 93.823 |       1927154 / 77.951 |    +132.3% |
|       64 | ZRANGE-0-9    |  545881 / 115.007 |     1243163 / 124.799 |   +127.7% |   504515 / 129.791 |       1877934 / 75.007 |    +272.2% |
|       64 | ZADD-update   |   812678 / 82.879 |      2057613 / 81.919 |   +153.2% |    737409 / 95.871 |       2027575 / 27.071 |    +175.0% |

### 7.1 Redis 12 I/O threads 完整结果

每行同样为 1000 clients、8 个 benchmark workers、精确 10,000,000 requests。`vs Redis 8/16`
为 Redis 12 相对既有 Redis 档位的吞吐变化。

| Pipeline | Command       | Redis 12 QPS / P99 | vs Redis 8 | vs Redis 16 |
| -------: | ------------- | -----------------: | ---------: | ----------: |
|        1 | SET           |     391298 / 6.439 |      -1.0% |       +7.8% |
|        1 | GET           |     369522 / 8.767 |      -1.8% |       +5.6% |
|        1 | HGET          |     359790 / 4.791 |      -0.8% |       +2.7% |
|        1 | HSET-update   |     344507 / 4.759 |     -11.9% |       +0.1% |
|        1 | LINDEX        |     369508 / 4.895 |      +0.1% |       +4.6% |
|        1 | LPUSH         |     298312 / 5.447 |      -2.2% |       +1.5% |
|        1 | LPOP          |     245290 / 6.975 |     -14.1% |       -7.9% |
|        1 | SISMEMBER     |     311575 / 5.975 |      -2.3% |       +0.8% |
|        1 | SADD-existing |     341262 / 5.159 |      -3.3% |       +1.8% |
|        1 | ZRANGE-0-9    |     327439 / 5.271 |      -8.1% |       -4.9% |
|        1 | ZADD-update   |     316466 / 6.231 |      +0.0% |       +1.6% |
|       16 | SET           |    814863 / 26.191 |     -16.3% |       -8.2% |
|       16 | GET           |   1024066 / 21.071 |     -10.2% |       +0.0% |
|       16 | HGET          |    595806 / 32.287 |      -7.5% |       +3.0% |
|       16 | HSET-update   |    486831 / 40.959 |     -12.2% |       -6.1% |
|       16 | LINDEX        |    739372 / 30.575 |     -11.1% |       -7.4% |
|       16 | LPUSH         |    356366 / 53.983 |      -9.8% |       -4.5% |
|       16 | LPOP          |   271525 / 128.959 |     -20.4% |      -20.4% |
|       16 | SISMEMBER     |    767813 / 51.807 |     -28.8% |      -25.0% |
|       16 | SADD-existing |    867754 / 29.119 |     -17.4% |      -15.2% |
|       16 | ZRANGE-0-9    |    475217 / 48.159 |     -16.7% |      -19.1% |
|       16 | ZADD-update   |    867905 / 24.671 |      -2.1% |       -4.3% |
|       64 | SET           |   686483 / 111.615 |     -15.5% |       -0.1% |
|       64 | GET           |   995025 / 103.103 |      -7.5% |       -5.1% |
|       64 | HGET          |   410914 / 343.039 |     -29.9% |      -22.7% |
|       64 | HSET-update   |   437905 / 211.711 |     -19.8% |       -7.7% |
|       64 | LINDEX        |   699056 / 100.991 |      -5.2% |       +1.8% |
|       64 | LPUSH         |   343702 / 216.703 |      -8.6% |       -0.0% |
|       64 | LPOP          |   295360 / 282.879 |     -11.1% |       -7.4% |
|       64 | SISMEMBER     |    884408 / 81.855 |      -8.9% |       +8.8% |
|       64 | SADD-existing |    846884 / 81.983 |     -10.6% |       +2.1% |
|       64 | ZRANGE-0-9    |   498182 / 165.247 |      -8.7% |       -1.3% |
|       64 | ZADD-update   |    737572 / 89.087 |      -9.2% |       +0.0% |

Redis 12 只在少数单命令上因单轮波动略高于 Redis 8；按 11 个命令的吞吐比几何平均，p1/p16/p64
分别低 4.2%/14.2%/12.6%。尤其 HGET pipeline=64 的 P99 达到 343.039 ms，应在重复测试中确认其
稳定性；单轮异常尾延迟不能直接外推为长期表现。

### 7.2 跨命令汇总

为避免用简单总和让高 QPS 命令主导比较，产品差异和扩展率采用 11 个命令吞吐比的几何平均。

| Pipeline | Dragonfly 8 vs Redis 8 | Dragonfly 16 vs Redis 16 | Redis 12 vs 8 | Redis 16 vs 8 | Dragonfly 16 vs 8 |
| -------: | ---------------------: | -----------------------: | ------------: | ------------: | ----------------: |
|        1 |                  +4.2% |                    +0.0% |         -4.2% |         -5.3% |             -9.1% |
|       16 |                +100.0% |                  +162.5% |        -14.2% |         -4.4% |            +25.4% |
|       64 |                +161.3% |                  +235.0% |        -12.6% |         -9.7% |            +15.8% |

pipeline=1 下，除 LPUSH/LPOP 外的大多数命令差异在约 ±9% 内，Redis 的 P99 通常更低。pipeline=16
以后，Hash、List 和 ZSet 等命令能给 Dragonfly 的 shard 执行线程提供足够并行工作；Redis 则仍由
主执行线程串行处理命令。pipeline=64 的最高吞吐伴随 27--287 ms 的 P99，不能脱离延迟 SLO 选型。

### 7.3 结构测试 CPU 概览

下面是每组 11 个命令的算术平均，仅用于观察资源趋势，不应把不同命令的 QPS 平均值当作业务容量。

| Config       | Pipeline |  平均 QPS | 平均 P99 ms | 平均 server cores |
| ------------ | -------: | --------: | ----------: | ----------------: |
| Redis 8      |        1 |   348,150 |       5.330 |             5.071 |
| Dragonfly 8  |        1 |   361,700 |       7.134 |             6.941 |
| Redis 12     |        1 |   334,088 |       5.883 |             5.611 |
| Redis 16     |        1 |   329,229 |       7.823 |             6.167 |
| Dragonfly 16 |        1 |   328,795 |      12.838 |            11.134 |
| Redis 8      |       16 |   769,770 |      32.623 |             3.043 |
| Dragonfly 8  |       16 | 1,454,181 |      34.687 |             6.566 |
| Redis 12     |       16 |   660,683 |      44.344 |             3.501 |
| Redis 16     |       16 |   732,944 |      34.460 |             3.919 |
| Dragonfly 16 |       16 | 1,807,905 |      25.778 |            11.539 |
| Redis 8      |       64 |   703,893 |     117.392 |             6.939 |
| Dragonfly 8  |       64 | 1,748,234 |      97.139 |             6.346 |
| Redis 12     |       64 |   621,408 |     162.565 |            10.132 |
| Redis 16     |       64 |   634,058 |     143.726 |            12.991 |
| Dragonfly 16 |       64 | 2,007,940 |      63.088 |             9.788 |

固定请求数使高吞吐 case 仅持续数秒，pidstat 样本较少，因此结构测试 CPU 平均值的精度低于 60 秒
memtier case；CPU 结论应以第 6.4 节的 60 秒数据为主。

## 8. MULTI/EXEC 事务结果

| Product   | Threads | Wire commands/s | EXEC tx/s | EXEC Avg ms |   P50 |    P95 |    P99 |   P99.9 | Server cores | Client cores |
| --------- | ------: | --------------: | --------: | ----------: | ----: | -----: | -----: | ------: | -----------: | -----------: |
| Redis     |       8 |         459,070 |   114,761 |       2.176 | 1.863 |  4.095 |  7.231 |  27.007 |        6.168 |        7.708 |
| Redis     |      12 |         416,339 |   104,079 |       2.399 | 2.111 |  4.047 |  8.767 |  28.415 |        6.666 |        7.448 |
| Redis     |      16 |         400,901 |   100,219 |       2.492 | 2.207 |  4.223 |  8.255 |  28.671 |        7.060 |        7.432 |
| Dragonfly |       8 |         408,258 |   102,059 |       4.739 | 2.271 | 11.391 | 74.751 | 158.719 |        7.029 |        7.485 |
| Dragonfly |      16 |         388,288 |    97,066 |       4.257 | 2.063 | 13.567 | 59.135 | 149.503 |       12.009 |        6.942 |

Redis 8 相对 Dragonfly 8 的事务吞吐高 12.4%，若以 Dragonfly 相对 Redis 表述则为 -11.1%。Redis
12 相对 Redis 8 低 9.3%、相对 Redis 16 高 3.9%；Redis 12/16 和 Dragonfly 16 均弱于各自 8 线程，
说明这种 pipeline=1 的短事务没有足够并行度覆盖 SMT、调度和事务协调开销。

Dragonfly 16 的 EXEC P99 比 8 线程改善 20.9%，但仍是 Redis 16 的约 7.2 倍。当前只完成单 key TX1；
未执行多 key、跨 Dragonfly 内部 shard、热点冲突事务，因此不能据此评价 Dragonfly 多 shard 事务模型。

## 9. 内存与网络资源

1,000,000 个 128-byte String key 完成预载和预热后的快照：

| Config       | INFO used_memory |  smaps Rss | 约 used_memory/key |
| ------------ | ---------------: | ---------: | -----------------: |
| Redis 8      |       206.07 MiB | 220.63 MiB |              216 B |
| Dragonfly 8  |       186.24 MiB | 218.35 MiB |              195 B |
| Redis 12     |       206.97 MiB | 221.89 MiB |              217 B |
| Redis 16     |       207.86 MiB | 223.48 MiB |              218 B |
| Dragonfly 16 |       186.40 MiB | 230.58 MiB |              195 B |

Dragonfly 的逻辑 used_memory 约比 Redis 低 9.6%，但进程 RSS 优势很小；16 线程 Dragonfly 的线程、
fiber 和 shard 固定开销使 RSS 高于 Redis 16。该结果只代表当前 100 万 String 数据集，不能外推到
Hash/List/Set/ZSet 或 1000 万 key。

正式 memtier 的最大协议流量出现在 Dragonfly 16/pipeline=64：总计 456,324 KiB/s，其中接收
329,143 KiB/s、发送 127,181 KiB/s。它远低于 iperf 基线，因此高 pipeline 结果不是 NIC 带宽封顶。

## 10. 同机报告与本轮双机结果的关系

原同机报告仍保留为历史基线，SHA256 在本轮测试前后均为
`61687811aad0a8c032e7f5c36b4e00c3f14313de17afcf16f0fbb6517ea671cf`。

| Config       | Pipeline | 原同机 ops/s | 新双机 ops/s | 绝对值变化 |
| ------------ | -------: | -----------: | -----------: | ---------: |
| Redis 8      |        1 |      735,465 |      446,485 |     -39.3% |
| Dragonfly 8  |        1 |      643,614 |      452,817 |     -29.6% |
| Redis 16     |        1 |      521,837 |      410,249 |     -21.4% |
| Dragonfly 16 |        1 |      321,845 |      402,788 |     +25.1% |
| Redis 8      |       16 |    1,478,586 |    1,302,525 |     -11.9% |
| Dragonfly 8  |       16 |    1,888,783 |    1,704,566 |      -9.8% |
| Redis 16     |       16 |    1,363,402 |      874,670 |     -35.8% |
| Dragonfly 16 |       16 |    1,788,357 |    2,099,832 |     +17.4% |
| Redis 8      |       64 |    1,193,696 |    1,135,858 |      -4.8% |
| Dragonfly 8  |       64 |    2,235,664 |    2,051,479 |      -8.2% |
| Redis 16     |       64 |      902,975 |      945,804 |      +4.7% |
| Dragonfly 16 |       64 |    2,140,436 |    2,742,417 |     +28.1% |

绝对 QPS 同时受 loopback 与 0.293 ms 远程 RTT 差异影响，不能简单把“新值减旧值”全部归因于 CPU
隔离。原同机报告没有 Redis 12 数据，因此 Redis 12 只做本轮 8→12→16 内部比较。可信的修正来自
新一轮内部对比：

- 原报告关于“同机环境无法公平验证 Dragonfly 16”的风险判断得到验证；
- “Dragonfly 16 普遍弱于 8”被修正为“p1 仍弱，但 p16/p64 分别强 23.2%/33.7%”；
- “Redis 16 弱于 8”在独立客户端下仍成立；新增 Redis 12 也弱于 8，进一步支持该结论；
- p1 绝对 QPS 因真实网络 RTT 下降，说明同机 loopback 数值不能代表生产 TCP 延迟；
- 大 pipeline 下 Dragonfly 的吞吐优势仍然存在，并在 16 线程时扩大。

## 11. 线程与 I/O 架构解释

### 11.1 Redis

本轮 Redis `INFO` 明确记录 `multiplexing_api:epoll`。`io-threads=N` 中主线程计入 N，额外线程负责
socket read/write 和协议解析；完整命令仍交回主线程执行。测试中 Redis 主线程在 pipeline=16/64
达到约 90%--97% CPU，证明命令执行串行路径已成为上限。把 I/O threads 从 8 增至 12、16 还会引入：

- 更多主线程与 I/O 线程之间的任务交接；
- 8 个物理核上的 SMT sibling 竞争；
- cache、调度和同步开销；
- pipeline 增大后网络摊销下降，额外 I/O 线程的边际收益进一步缩小。

Redis 12 的三档 memtier 吞吐全部低于 Redis 8，且 p16 每核效率只有 92.7 Kops/s/core，远低于
Redis 8 的 181.2 Kops/s/core。因此 Redis I/O 多线程不是命令执行多线程，不能期待 QPS 随
`io-threads` 线性增长。

### 11.2 Dragonfly

本轮 Dragonfly `INFO` 明确记录 `multiplexing_api:iouring`，启动日志记录
`IORing with 1024 entries`；因此本次实际使用 io_uring，不是 epoll fallback。Dragonfly 每个
proactor/shard 线程拥有自己的数据 shard，并可执行命令。均匀随机 key 和足够 pipeline 能让多个 shard
并行工作，这正是 p16/p64 的 16 线程扩展来源。

Dragonfly 也不是在所有负载下自动扩展：

- pipeline=1 时每次只提供较少可并行工作，还要支付 fiber、路由和 shard 调度固定成本；
- 单热点 key 或单 container key 不能均匀分布到所有 shard；
- MULTI/EXEC 需要事务协调，EXEC 尾延迟可能被跨线程调度和排队放大；
- 16 logical threads 共享 8 个物理核资源，低负载下 SMT 开销可能超过收益。

### 11.3 为什么 pipeline=1 时 Redis 可能更快

这不是“epoll 一定比 io_uring 快”，也不是“Dragonfly 多线程无效”。本轮证据更符合以下组合原因：

1. pipeline=1 每条命令都等待一次远程请求/回复，0.293 ms RTT 和客户端调度占比显著；
2. Redis 的单主线程命令路径短，简单 GET/SET 不需要跨 shard 路由；
3. Dragonfly 的 shared-nothing 路径要确定目标 shard，并通过 fiber/proactor 执行，固定成本难以摊薄；
4. memtier 的 8 个 worker 在 p1 已使用约 7 cores，客户端本身接近生成上限；
5. 请求增至 2000 clients 后吞吐不升、P99 明显恶化，说明增加排队不是解决办法。

所以 p1 应主要看延迟和等 offered-load 结果，而不是只比较饱和 QPS。进入 pipeline=16/64 后网络与路由
固定成本被批量摊薄，Dragonfly 的多线程命令执行优势才成为主导。

## 12. 16 核机器配置建议

| 业务目标               | Redis 建议        | Dragonfly 建议         | 原因                                                |
| ---------------------- | ----------------- | ---------------------- | --------------------------------------------------- |
| pipeline=1、低尾延迟   | 8 I/O threads     | 8 proactor/shards      | Redis 12/16 均回退；Dragonfly 16 也回退             |
| pipeline≈16、吞吐优先 | 8 I/O threads     | 16 proactor/shards     | Dragonfly 16 为 2.10M ops/s，P99 27.135 ms          |
| pipeline≈64、最高吞吐 | 8 I/O threads     | 16 proactor/shards     | Dragonfly 16 为 2.74M ops/s，但需接受 61.183 ms P99 |
| 单 key 短事务          | 8 I/O threads     | 先用 8，并专项验证长尾 | Redis 8 tx/s 更高且 EXEC P99 显著更低               |
| 2000+ 长连接           | 8，控制 in-flight | 8/16 取决于 pipeline   | 本轮 2000 clients 不增吞吐，只增加排队              |

这里的“16 核”实际是 8 physical cores / 16 SMT logical CPUs。如果生产机器是真正 16 个物理核，
Dragonfly 的扩展潜力和最佳线程数需要重新测量，不能直接套用本报告。

## 13. 架构优劣与选型边界

### Redis 更适合

- 简单单请求、低 pipeline、尾延迟敏感；
- 单 key 小事务和强依赖成熟事务语义的业务；
- 当前容量单实例一个主执行核已足够，希望行为简单可预测；
- 应用无法有效 batch，或请求高度集中在少量热点 key。

### Dragonfly 更适合

- 大量独立、均匀分布 key，可形成并行执行；
- pipeline 或客户端并发能持续提供足够 in-flight work；
- 单实例需要利用多个物理核/SMT 提升吞吐；
- Hash/List/Set/ZSet 等操作在高 pipeline 下需要显著高于 Redis 单执行线程的容量。

### 不能由本报告决定

- Cluster、代理、跨节点或跨 slot 性能；
- 主从复制、故障切换和一致性；
- AOF/RDB/快照开启后的性能与恢复时间；
- TLS、ACL、Lua、模块、Search 和 tiered storage；
- 单热点 key、多 shard 事务和复杂 WATCH 冲突；
- 真正 16 个物理核或更大 NUMA 机器的扩展曲线。

## 14. 局限与下一轮建议

1. 当前只有一个正式 repeat。核心矩阵至少再做 3--5 轮，交错产品顺序，报告 median、IQR 和 CV。
2. 增加固定 offered-load（例如 200k/400k/800k/1.2M ops/s），比较相同吞吐下的 P99/P99.9。
3. 为 pipeline=1 增加更多客户端 worker 数并做 25% 增量验收，确认 load-generator ceiling。
4. 对关键 p1/p16 case 做 300 秒长跑，观察 QPS、RSS、steal、retransmit 和尾延迟漂移。
5. 增加 uniform、10% hotset、single-hot-key 三种 key 分布，区分 shard 均匀扩展和热点上限。
6. 增加 Hash 事务、多 key 同 shard、2/4/8 shard 事务和热点冲突事务，并使用独立 HDR 驱动测量整笔
   事务 wall latency。
7. 如选型目标要求“官方稳定版”，应固定双方测试日稳定 tag 重新 release build，本轮 snapshot 只作为
   当前源码基线。

## 15. 原始数据、归档与复现

| 内容               | 位置                                                                            |
| ------------------ | ------------------------------------------------------------------------------- |
| 本机合并原始结果   | `/tmp/dragonfly-bench-remote/formal-20260824/`                                |
| 网络验收原始结果   | `/tmp/dragonfly-bench-remote/network/`                                        |
| 远端客户端原始结果 | `10.47.76.3:/home/panlei/redis-dragonfly-results/formal-20260824/`            |
| Redis 12 远端结果  | `10.47.76.3:/home/panlei/redis-dragonfly-results/redis12-20260824/`           |
| 压缩归档           | `/home/Dragonfly/benchmark-results/redis-vs-dragonfly-remote-20260824.tar.gz` |
| 归档 SHA256        | `6a614ae5b76795bdcec6bc6dec13d686a9837c4fc61cccc19bdef8a3a13df1ed`            |
| 服务端执行脚本     | `tools/benchmark/run_redis_dragonfly_remote.sh`                               |
| 远端 worker        | `tools/benchmark/redis_dragonfly_remote_client_worker.sh`                     |
| 原同机报告         | `docs/redis-vs-dragonfly-standalone-benchmark-report.md`                      |
| Pedis 构建日志     | `/tmp/pedis-build-20260906.log`                                               |

每个正式 case 保存客户端输出、客户端 mpstat/vmstat/sar、服务端 INFO before/after、pidstat、mpstat、
vmstat、sar、进程状态和内存快照。归档同时包含 formal 数据和 iperf 网络验收数据。

本报告没有修改原同机报告；原报告 SHA256 保持为
`61687811aad0a8c032e7f5c36b4e00c3f14313de17afcf16f0fbb6517ea671cf`。

## 16. Pedis 补充测试（2026-09-06）

### 16.1 测试目标与范围

本轮按与 Redis/Dragonfly 相同的单机服务、远程客户端方案，尝试增加
`/home/Pedis/pedis` 的对比。计划使用服务端 `10.47.76.7`，客户端
`10.47.76.3:/home/panlei`，并覆盖：

- redis-benchmark 五种基础数据结构（String、Hash、List、Set、Sorted Set）；
- memtier-benchmark 的 1000/2000 clients、pipeline 1/16/64；
- MULTI/EXEC 单节点事务；
- Pedis 可用的 8/12/16 线程档位（先以 `--smp` 参数确认实际支持范围）；
- 每个正式 case 至少 60 秒或 10,000,000 次请求，并保存客户端、服务端 CPU、内存、网络和错误统计。

### 16.2 版本与构建信息

| 项目 | 值 |
| --- | --- |
| 仓库 | `/home/Pedis/pedis` |
| Pedis HEAD | `64f928d Fix the typo in the redis.md` |
| Seastar 子模块 | `b79be026aaf3cc1f785ac376f24feee0c2ae4ace` |
| 操作系统 | Ubuntu 22.04 系列用户态 |
| Linux kernel | `5.15.0-124-generic` |
| C++ 编译器 | `g++-11 (11.4.0)` |
| Python | `3.10.12` |
| 配置命令 | `./configure.py --mode=release --with=scylla --disable-hwloc --compiler=/usr/bin/g++-11 --c-compiler=/usr/bin/gcc-11 --cflags='-I/tmp/pedis-antlr/include' --debuginfo=0` |
| ANTLR runtime | `/tmp/pedis-antlr/include/antlr3.hpp`（ANTLR 3.5.2） |

配置阶段可以完成，并生成 `build.ninja`；但链接目标
`build/release/scylla` 未能构建成功，因此没有可启动的 Pedis 服务，也没有任何
Pedis benchmark 数字写入结果表。

### 16.3 构建阻塞证据

执行：

```text
cd /home/Pedis/pedis
PATH=/tmp/pedis-bin:$PATH ninja -j16 build/release/scylla
```

错误均出现在生成的 CQL 源文件编译阶段（例如 `build/release/gen/cql3/CqlLexer.o`）。
并行构建的一次输出首先暴露了 GCC6 Concepts 兼容性错误：

```text
error: ‘concept’ does not name a type
error: ‘requires’ does not name a type
note: ‘concept’ only available with ‘-std=c++20’ or ‘-fconcepts’
```

错误来自 Pedis/Seastar 旧版 GCC6 Concepts 代码（包括 `concept bool` 和
`GCC6_CONCEPT` 宏）。尝试临时禁用宏后，源码中仍有未被宏包裹的 Concepts 声明，
导致更多类型缺失和模板实例化错误；该 workaround 已恢复，未保留为项目修改。
恢复源码后使用单线程重新构建，仍在同一目标上失败，典型错误包括：

```text
error: ‘atomic_cell_or_collection’ has not been declared
error: comparison of integer expressions of different signedness [-Werror=sign-compare]
error: ‘reinterpret_cast’ ... casts away qualifiers [-Werror]
```

这表明问题不只是一个可通过开关消除的 Concepts 检测，而是该 2019 年代码快照
与当前 GCC 11、系统 Boost/ANTLR 头文件组合存在多处 ABI/API 和 `-Werror` 不兼容。

该版本 configure 只提供 `--with=scylla` 和 `--with=iotune`，README 中的
`--with=pedis` 并不是当前源码的有效选项。仓库没有现成的
`build/release/pedis` 或 `build/release/scylla` 可执行文件。

### 16.4 Pedis 测试执行状态

| 测试项 | 状态 | 结果 |
| --- | --- | --- |
| 服务启动/端口探针 | 未执行 | 无可执行文件 |
| redis-benchmark String/Hash/List/Set/ZSet | 未执行 | 无服务端 |
| memtier pipeline=1/16/64 | 未执行 | 无服务端 |
| 1000/2000 clients | 未执行 | 无服务端 |
| MULTI/EXEC 事务 | 未执行 | 无服务端 |
| `--smp 8/12/16` 扩展测试 | 未执行 | 无服务端，不能确认参数支持 |
| CPU/内存/网络采样 | 未执行 | 无服务端进程 |

因此本报告中的 Redis 与 Dragonfly QPS、延迟、CPU 和线程扩展数据保持原样；
Pedis 行不能填入 0、空跑结果或错误响应速率，因为那会把“构建失败”误报成
“性能为零”。

### 16.5 对选型结论的影响

本报告现阶段仍是 Redis 与 Dragonfly 的双产品实测结论。Pedis 只能得出以下
工程结论：在当前仓库快照和现有编译环境下，无法建立可运行的 Redis 协议服务，
故不能进行公平的远程客户端性能比较。该结论不代表 Pedis 的运行时性能，也不
代表其在匹配的历史工具链上一定无法构建。

如需完成 Pedis 性能对比，后续必须先提供与该提交匹配的构建环境（通常是项目
时代的 GCC/Boost/Seastar 组合，或维护者提供的容器/发行包），并重新执行本节
16.1 的全部矩阵。只有在服务成功启动、命令兼容性探针通过且每个 case 达到
60 秒或 10,000,000 请求后，数据才可与本报告现有 Redis/Dragonfly 结果并列。

### 16.6 “直接下载最新版可执行文件”核查结果

针对直接使用预编译版本的建议，本轮通过 GitHub Releases/Tags、仓库分支和公开
容器仓库进行了核查：

| 来源 | 核查结果 |
| --- | --- |
| `SINHASantos/pedis` Releases | 0 个 Release，无二进制资产 |
| `SINHASantos/pedis` Tags | 0 个 Tag |
| 上游 `fastio/1store` Releases/Tags | 0 个 Release、0 个 Tag |
| Docker Hub `fastio/1store`、`fastio/pedis`、`sinhasantos/pedis` | 仓库不存在（HTTP 404） |
| GHCR 对应镜像名 | 无公开可拉取 manifest（匿名请求 HTTP 403） |
| 活跃 fork | 未发现带 Release/二进制资产的 fork；最新代码仍停留在 2019 年提交 |

因此不存在可验证来源的“Pedis 最新版可执行文件”可直接下载。不能把
`scylladb/scylla` 官方镜像或其他 Scylla 二进制当作 Pedis：它们不包含 Pedis
的 Redis 适配代码，替换后会改变测试对象。当前唯一可追溯的 Pedis 制品是源码
快照本身，所以本报告保留构建失败结论，而不虚构下载或压测结果。
