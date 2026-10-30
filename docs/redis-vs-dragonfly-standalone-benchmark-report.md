# Redis vs Dragonfly 单机性能测试与架构分析报告

> 测试日期：2026-08-21  
> 测试范围：单机、单进程、standalone、纯内存、TCP/RESP2  
> 服务端线程：只测试 8 和 16；没有测试其他服务端线程数  
> 原始数据：`/tmp/dragonfly-bench/formal/`  
> 测试方案：`docs/redis-vs-dragonfly-standalone-benchmark-plan.md`

## 1. 结论摘要

本轮已重新清空旧数据并从零执行。正式数据满足以下硬门槛：

- memtier 核心场景每个 case 独立运行至少 60 秒；实际窗口为 60.012--60.055 秒；
- redis-benchmark 五结构场景每个 case 固定执行 10,000,000 requests；
- 20 个 memtier 正式 case、132 个 redis-benchmark 正式 case；
- redis-benchmark 正式请求总量 13.2 亿；memtier String 约 10.31 亿 operations；事务约 1.15 亿
  wire commands；合计约 24.65 亿次正式操作；
- connection error、server error、timeout 和 hit workload miss 均为 0；
- 预热、预载不计入正式结果；
- 所有结果均同时保存服务端 INFO、pidstat、mpstat、vmstat 和网络采集。

本机只有 8 个物理核、16 个 SMT logical CPUs。8 线程配置是一线程一物理核；16 线程配置是 SMT
扩展段，不是物理核翻倍。压测客户端和服务端位于同一台机器，因此结果必须解释为
`same-host-contention`，尤其不能把 Dragonfly 16 的 pipeline=1 回退外推到独立压测机。

当前一轮正式结果给出的核心判断是：

1. **在 pipeline=1 的混合读写 memtier 场景，Redis 8 更高**：735,465 vs 643,614 ops/s；两侧
   memtier 8 个 worker 都接近满载，因此这组结果包含明显 load-generator ceiling。
2. **在 pipeline=16/64，Dragonfly 8 吞吐更高**：分别比 Redis 8 高 27.7% 和 87.3%；但
   pipeline=16 的 P99 为 24.191 ms，高于 Redis 8 的 16.063 ms。
3. **redis-benchmark 五结构 pipeline=1 下，Dragonfly 8 在 Hash、List、ZRANGE 上更强**；其中
   LPUSH/LPOP 分别比 Redis 8 高约 61.2%/89.5%。SET/GET 只领先约 3.2%/8.4%，Set/ZADD 接近。
4. **pipeline=16/64 时 Dragonfly 的多线程执行优势扩大**。pipeline=64 下，Dragonfly 8 对 Redis 8
   的吞吐提升从约 104.9%（GET）到 334.6%（LPOP），但大 pipeline 的 P99 普遍达到几十到上百 ms。
5. **Redis 16 在当前机器没有优于 Redis 8**。Redis 主执行线程在多数饱和场景约 93%--98% CPU，
   增加 I/O threads 没有改变命令执行串行上限，反而增加 SMT、交接和同机竞争成本。
6. **Dragonfly 16 只有在足够大的 pipeline 下才显现吞吐**。redis-benchmark pipeline=16/64 的多项
   命令超过 Dragonfly 8，但 pipeline=1 和 memtier 混合读写明显回退，尾延迟也更高。
7. **本轮 TX1 单 key 事务 Redis 更快**。Redis 8 约 167,229 transactions/s，Dragonfly 8 约
   90,825 transactions/s；Dragonfly 的 P99.9 出现明显长尾。

## 2. 结果可信度与边界

### 2.1 已满足

| 检查项 | 结果 |
| --- | --- |
| standalone、单实例 | 通过 |
| Redis/Dragonfly 只测服务端 8、16 线程 | 通过 |
| 两个 benchmark 工具均使用 | 通过 |
| memtier 正式窗口至少 60 秒 | 20/20 通过 |
| redis-benchmark 每 case 至少 1000 万请求 | 132/132 通过 |
| 1000/2000 clients | String 混合读写通过 |
| pipeline=1/16/64 | String 与五结构通过 |
| 五种基础数据结构 | 通过 |
| MULTI/EXEC 完整事务 rotation | 通过 |
| hit workload miss=0 | 通过 |
| connection/server errors=0 | 通过 |
| swap | 关闭，0 B |
| KVM steal | 核心 case 平均低于 1% |

### 2.2 仍需谨慎

- 当前只有一个 formal repeat，不是方案要求的 5 轮中位数；本报告不能给出 CV/IQR；
- load generator 与服务端同机。memtier pipeline=1 的 client CPU 达到约 7.5--7.8 cores，说明
  p1 吞吐可能由客户端和 loopback 共同限制；
- 16 线程服务端使用全部 logical CPUs，没有给 load generator 独立 CPU；
- 结构预载的 Hash/Set/ZSet 抽样为 10 个 field/member；List 采用均匀随机预载，抽样长度为 8--10，
  随后的 LPUSH/LPOP 成对执行保证正式 LPOP 不读空；
- SADD 正式场景是 existing-member no-op update，不代表新增 member 的内存分配成本；
- 当前比较的是两个 current-source snapshot，不是稳定 tag 对稳定 tag；
- 没有独立客户端机器，因此绝对 QPS 和 8→16 扩展结论不能外推到生产网络。

## 3. 测试对象

| 项目 | 版本/路径 |
| --- | --- |
| Dragonfly source | `/home/Dragonfly/dragonfly` |
| Dragonfly SHA | `0ab6e227bb1286a9bda7b0438c9ea3abaf633e60` |
| Dragonfly describe | `v1.40.0-74-g0ab6e227` |
| Dragonfly binary | `build-opt/dragonfly`，release，运行版本显示 `dev-0000000` |
| Helio | `3d8b4ece14ed62cb516e36afbc8947c0cc392826` |
| Redis source | `/home/redis/redis` |
| Redis SHA | `de3ea25a9f486a44467cad0628bdb2d0a36aabf0` |
| Redis | 8.9.241，jemalloc 5.3.0 |
| redis-benchmark | `/home/redis/redis/src/redis-benchmark`，8.9.241 |
| memtier | `/tmp/memtier_benchmark/memtier_benchmark`，SHA `272eeb64:1` |

Dragonfly release 构建关闭 AWS、GCP、Tiering、Search 和 SSL。由于本机 `/usr/local` 中存在旧 GmSSL，
构建使用 OpenSSL 3 overlay；运行时使用 GCC 14 libstdc++ runtime。完整参数见测试方案和
`/tmp/dragonfly-bench/formal/environment.txt`。

## 4. 硬件与服务端配置

| 项目 | 值 |
| --- | --- |
| CPU | Intel Xeon Cascade Lake，8 physical cores / 16 logical CPUs |
| 拓扑 | 2 sockets × 4 cores × SMT2 |
| NUMA | 1 visible node |
| 内存 | 31 GiB |
| Kernel | Linux 5.15.0-124-generic |
| 虚拟化 | KVM |
| THP | madvise |
| swap | 关闭 |
| 端口 | 127.0.0.1:6380，TCP/RESP2 |
| 持久化 | Redis save/AOF 关闭；Dragonfly snapshot 不启用 |
| maxmemory | 8G，noeviction/cache_mode=false |

| 服务端配置 | CPU affinity |
| --- | --- |
| Redis io-threads=8 | `0,2,4,6,8,10,12,14` |
| Dragonfly proactor_threads=num_shards=8 | `0,2,4,6,8,10,12,14` |
| Redis io-threads=16 | `0-15` |
| Dragonfly proactor_threads=num_shards=16 | `0-15` |

## 5. memtier String 混合读写结果

工作负载：1,000,000 keys、128-byte value、SET:GET=1:9、uniform random、1000/2000 clients。
每个配置先装载精确 100 万 key，再做 30 秒预热；表内每一行均为独立 60 秒正式窗口。

`server cores` 来自 pidstat 进程总 CPU；`client cores` 来自 memtier 自身统计。

| Product | Threads | Clients | Pipeline | Ops/s | Avg ms | P50 | P95 | P99 | P99.9 | Server cores | Client cores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Redis | 8 | 1000 | 1 | 735,465 | 1.359 | 1.239 | 2.127 | 3.279 | 17.023 | 6.918 | 7.833 |
| Dragonfly | 8 | 1000 | 1 | 643,614 | 1.552 | 1.383 | 2.447 | 5.823 | 15.807 | 7.607 | 7.575 |
| Redis | 16 | 1000 | 1 | 521,837 | 1.914 | 1.503 | 4.351 | 9.023 | 18.559 | 6.104 | 6.422 |
| Dragonfly | 16 | 1000 | 1 | 321,845 | 3.104 | 2.223 | 10.495 | 17.151 | 25.599 | 7.480 | 4.651 |
| Redis | 8 | 1000 | 16 | 1,478,586 | 10.809 | 10.431 | 13.823 | 16.063 | 23.807 | 3.203 | 3.307 |
| Dragonfly | 8 | 1000 | 16 | 1,888,783 | 8.459 | 7.551 | 17.151 | 24.191 | 34.303 | 7.780 | 4.636 |
| Redis | 16 | 1000 | 16 | 1,363,402 | 11.724 | 11.263 | 14.975 | 16.895 | 25.471 | 3.356 | 3.192 |
| Dragonfly | 16 | 1000 | 16 | 1,788,357 | 8.934 | 7.295 | 21.119 | 33.023 | 51.199 | 9.808 | 3.952 |
| Redis | 8 | 1000 | 64 | 1,193,696 | 53.573 | 51.455 | 64.511 | 103.423 | 119.807 | 7.164 | 1.920 |
| Dragonfly | 8 | 1000 | 64 | 2,235,664 | 28.604 | 26.879 | 52.991 | 70.655 | 97.279 | 7.786 | 3.623 |
| Redis | 16 | 1000 | 64 | 902,975 | 70.815 | 62.463 | 119.295 | 164.863 | 204.799 | 13.798 | 1.498 |
| Dragonfly | 16 | 1000 | 64 | 2,140,436 | 29.873 | 25.087 | 66.559 | 96.767 | 147.455 | 9.812 | 3.458 |
| Redis | 8 | 2000 | 1 | 617,392 | 3.237 | 2.751 | 5.311 | 14.015 | 29.823 | 6.420 | 7.868 |
| Dragonfly | 8 | 2000 | 1 | 533,976 | 3.742 | 3.199 | 6.815 | 14.847 | 30.975 | 7.478 | 7.608 |
| Redis | 16 | 2000 | 1 | 512,124 | 3.901 | 3.327 | 7.007 | 14.271 | 32.511 | 6.163 | 7.450 |
| Dragonfly | 16 | 2000 | 1 | 291,083 | 6.866 | 5.599 | 19.583 | 28.543 | 43.007 | 6.817 | 4.946 |

### 5.1 Pipeline 吞吐与延迟

8 线程同配置比较：

| Pipeline | Redis ops/s | Dragonfly ops/s | Dragonfly throughput difference | Redis P99 | Dragonfly P99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 735,465 | 643,614 | -12.5% | 3.279 | 5.823 |
| 16 | 1,478,586 | 1,888,783 | +27.7% | 16.063 | 24.191 |
| 64 | 1,193,696 | 2,235,664 | +87.3% | 103.423 | 70.655 |

Redis 8 的最佳吞吐点是 pipeline=16；继续增至 64 后吞吐下降 19.3%，P99 增至约 6.4 倍。
Dragonfly 8 在 pipeline=64 仍有吞吐增益，但相对 pipeline=16 只增加 18.4%，P99 增至约 2.9 倍。

### 5.2 1000 与 2000 clients

pipeline=1 从 1000 增加到 2000 clients 后，四个配置吞吐均没有提升：

- Redis 8：-16.1%，P99 3.279 → 14.015 ms；
- Dragonfly 8：-17.0%，P99 5.823 → 14.847 ms；
- Redis 16：-1.9%，P99 9.023 → 14.271 ms；
- Dragonfly 16：-9.6%，P99 17.151 → 28.543 ms。

因此当前机器建议把 1000 clients 作为高并发主工作点，2000 clients 只作为连接/排队压力确认点。

## 6. redis-benchmark 五种数据结构

每一行都是 1000 clients、8 个 benchmark worker、精确 10,000,000 requests。数值格式为
`QPS / P99(ms)`。Hash/Set/ZSet 预载约 100k container × 10 elements；String 为 100 万 key；value 为
128 bytes。SADD 是 existing-member no-op update，ZADD 是 score update。

| Pipeline | Command | Redis 8 QPS / P99 ms | Dragonfly 8 QPS / P99 ms | Redis 16 QPS / P99 ms | Dragonfly 16 QPS / P99 ms |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | SET | 634397 / 4.303 | 654879 / 3.055 | 578771 / 4.375 | 369344 / 11.687 |
| 1 | GET | 614628 / 3.927 | 665956 / 2.487 | 587302 / 3.311 | 352597 / 10.551 |
| 1 | HGET | 525928 / 3.807 | 633914 / 3.039 | 464188 / 5.671 | 356037 / 9.887 |
| 1 | HSET | 525928 / 3.767 | 634035 / 2.695 | 412116 / 5.919 | 372578 / 10.311 |
| 1 | LINDEX | 624454 / 3.231 | 634035 / 2.479 | 504796 / 6.439 | 369017 / 8.287 |
| 1 | LPUSH | 353882 / 4.983 | 570516 / 3.487 | 327622 / 5.359 | 349626 / 12.999 |
| 1 | LPOP | 314842 / 5.559 | 596481 / 2.791 | 296226 / 5.671 | 355872 / 9.687 |
| 1 | SISMEMBER | 614402 / 2.615 | 595380 / 2.455 | 578402 / 2.711 | 349736 / 4.655 |
| 1 | SADD-existing | 644413 / 3.103 | 623947 / 2.631 | 578737 / 2.943 | 359092 / 8.095 |
| 1 | ZRANGE-0-9 | 519130 / 3.935 | 604888 / 2.935 | 481232 / 5.263 | 321471 / 13.391 |
| 1 | ZADD-update | 614402 / 3.175 | 605400 / 2.303 | 570288 / 2.895 | 359054 / 4.567 |
| 16 | SET | 973710 / 22.607 | 1663340 / 23.839 | 928678 / 23.167 | 1898254 / 37.983 |
| 16 | GET | 1174812 / 17.567 | 1735508 / 22.063 | 1079331 / 19.759 | 1993223 / 35.935 |
| 16 | HGET | 665425 / 29.839 | 1478415 / 29.231 | 554539 / 38.047 | 1897893 / 34.207 |
| 16 | HSET | 587165 / 34.687 | 1477541 / 27.727 | 518591 / 38.079 | 1897533 / 37.183 |
| 16 | LINDEX | 814930 / 25.615 | 1662787 / 24.079 | 798786 / 25.679 | 1994416 / 34.719 |
| 16 | LPUSH | 399202 / 54.399 | 1287498 / 31.599 | 383818 / 53.247 | 1662234 / 37.823 |
| 16 | LPOP | 327193 / 64.191 | 1247661 / 31.983 | 332712 / 62.079 | 1596424 / 41.087 |
| 16 | SISMEMBER | 1079331 / 18.559 | 1901141 / 19.807 | 1050862 / 20.799 | 2097316 / 32.735 |
| 16 | SADD-existing | 1109385 / 19.439 | 1734305 / 24.991 | 1050972 / 21.039 | 1991239 / 35.455 |
| 16 | ZRANGE-0-9 | 604961 / 40.191 | 1247505 / 35.711 | 596196 / 38.591 | 1326612 / 53.663 |
| 16 | ZADD-update | 950751 / 23.631 | 1814553 / 24.047 | 907688 / 23.359 | 1894657 / 34.751 |
| 64 | SET | 905141 / 84.287 | 2188663 / 66.239 | 885269 / 86.207 | 2326664 / 85.567 |
| 64 | GET | 1076890 / 67.647 | 2206044 / 63.647 | 1021868 / 88.319 | 2335903 / 77.439 |
| 64 | HGET | 622937 / 115.967 | 1891790 / 81.535 | 538677 / 164.479 | 2205558 / 76.735 |
| 64 | HSET | 577267 / 127.807 | 1890002 / 78.207 | 504464 / 164.095 | 2329916 / 90.303 |
| 64 | LINDEX | 766342 / 99.903 | 2093364 / 74.815 | 751597 / 112.703 | 2646203 / 64.831 |
| 64 | LPUSH | 376138 / 219.135 | 1528351 / 107.967 | 376194 / 221.183 | 1986097 / 96.255 |
| 64 | LPOP | 326851 / 229.247 | 1420454 / 132.991 | 314080 / 242.815 | 2206531 / 76.223 |
| 64 | SISMEMBER | 971157 / 92.799 | 2333178 / 66.623 | 971628 / 99.199 | 2835271 / 70.335 |
| 64 | SADD-existing | 971534 / 86.335 | 2207506 / 68.351 | 971817 / 79.167 | 2827255 / 71.487 |
| 64 | ZRANGE-0-9 | 561545 / 147.583 | 1373249 / 101.183 | 553863 / 127.807 | 1586798 / 113.151 |
| 64 | ZADD-update | 847817 / 110.591 | 2339728 / 62.047 | 830082 / 91.903 | 2835271 / 69.567 |

### 6.1 Pipeline=1 的基础命令差异（8 线程）

| Command | Redis QPS | Dragonfly QPS | Dragonfly difference | Redis P99 | Dragonfly P99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| SET | 634,397 | 654,879 | +3.2% | 4.303 | 3.055 |
| GET | 614,628 | 665,956 | +8.4% | 3.927 | 2.487 |
| HGET | 525,928 | 633,914 | +20.5% | 3.807 | 3.039 |
| HSET update | 525,928 | 634,035 | +20.6% | 3.767 | 2.695 |
| LINDEX | 624,454 | 634,035 | +1.5% | 3.231 | 2.479 |
| LPUSH | 353,882 | 570,516 | +61.2% | 4.983 | 3.487 |
| LPOP | 314,842 | 596,481 | +89.5% | 5.559 | 2.791 |
| SISMEMBER | 614,402 | 595,380 | -3.1% | 2.615 | 2.455 |
| SADD existing | 644,413 | 623,947 | -3.2% | 3.103 | 2.631 |
| ZRANGE 0 9 | 519,130 | 604,888 | +16.5% | 3.935 | 2.935 |
| ZADD update | 614,402 | 605,400 | -1.5% | 3.175 | 2.303 |

### 6.2 Pipeline 扩大后的差异

pipeline=16 时，Dragonfly 8 相对 Redis 8 的吞吐提升范围约为 47.7%（GET）到 281.3%（LPOP）。
pipeline=64 时，提升范围约为 104.9%（GET）到 334.6%（LPOP）。

这些提升不能单独作为生产结论：pipeline=64 的单批次排队使 P99 普遍达到 60--230 ms。应根据业务
SLO 选择 pipeline，而不是只选择最高 QPS。

## 7. MULTI/EXEC 事务结果

TX1：`MULTI; SET tx:<key> <128B>; GET tx:<key>; EXEC`。每个 rotation 是一笔事务，包含 4 条 wire
commands 和 2 条业务命令。每行独立运行 60 秒、1000 clients、pipeline=1。

延迟列是 memtier `--transaction` 的 Totals 统计；本轮没有使用独立 HDR 驱动器重新测量 EXEC 到完整
事务回复的 wall latency，因此事务吞吐可信，延迟需要在事务专项驱动器中复核。

| Product | Threads | Wire commands/s | Transactions/s | Avg ms | P50 | P95 | P99 | P99.9 | Server cores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Redis | 8 | 668,914 | 167,229 | 1.493 | 1.343 | 2.207 | 3.999 | 22.271 | 6.577 |
| Redis | 16 | 567,780 | 141,945 | 1.758 | 1.575 | 2.879 | 5.791 | 26.111 | 6.225 |
| Dragonfly | 8 | 363,298 | 90,825 | 2.751 | 0.495 | 3.455 | 36.351 | 405.503 | 5.111 |
| Dragonfly | 16 | 311,754 | 77,938 | 3.204 | 0.615 | 7.231 | 37.887 | 321.535 | 7.292 |

该单 key 小事务更有利于 Redis 的串行执行路径。Dragonfly P50 很低，但长尾显著，导致平均值、P99 和
P99.9 较差。多 key、跨 Dragonfly 内部 shard 的 TX3/TX4 尚未执行，因此不能从 TX1 推断多 shard
事务的相对成本。

## 8. 线程模型与 CPU 证据

### 8.1 Redis I/O threads

Redis 8 的 60 秒 String 混合读写 pidstat：

| Pipeline | Main thread avg CPU | 每个 I/O thread avg CPU | I/O thread 数 |
| ---: | ---: | ---: | ---: |
| 1 | 93.0% | 85.5% | 7 |
| 16 | 98.3% | 31.7% | 7 |
| 64 | 98.4% | 88.3% | 7 |

Redis 16 的主线程在 pipeline=16 时仍约 98.4%。命令执行主线程接近单核上限后，增加 I/O threads 不能
让数据结构执行并行化。本机 Redis 16 在所有核心 memtier 吞吐点都没有超过 Redis 8。

### 8.2 Dragonfly proactors

| Threads | Pipeline | Proactor avg CPU/thread | Process CPU cores |
| ---: | ---: | ---: | ---: |
| 8 | 1 | 95.1% | 7.607 |
| 8 | 16 | 97.3% | 7.780 |
| 8 | 64 | 97.3% | 7.786 |
| 16 | 1 | 46.7% | 7.480 |
| 16 | 16 | 61.3% | 9.808 |
| 16 | 64 | 61.3% | 9.812 |

Dragonfly 8 的 proactors 基本全部饱和且负载均衡，证明 uniform key 成功利用 8 个物理核。Dragonfly 16
的每线程利用率较低；pipeline=16/64 能把进程 CPU 提升到约 9.8 cores，但同机 load generator 和 SMT
限制了完整 16 logical CPU 的利用。

## 9. 16 核逻辑 CPU 机器的配置建议

这里的“16 核”实际是 8 physical cores + SMT16，不是 16 physical cores。

- **低 pipeline、低尾延迟优先**：当前机器两产品都建议服务端 8 线程。Redis 8 的 memtier p1 QPS
  最高；Dragonfly 8 的 redis-benchmark p1 也明显优于 Dragonfly 16。
- **吞吐优先且业务允许 batching**：Dragonfly 8 + pipeline=16 是较稳妥起点；pipeline=64 可继续提高
  吞吐，但必须确认 P99 SLO。redis-benchmark 中 Dragonfly 16 在 p16/p64 的峰值更高，但 memtier
  混合读写没有复现相同扩展，暂不建议直接设为默认。
- **Redis 配置**：建议 `io-threads=8`。本机 `io-threads=16` 没有一个核心吞吐点超过 8，并占用更多
  SMT sibling。
- **CPU 效率优先**：应给 kernel、loopback/NIC 中断和业务旁路任务预留 SMT 资源；8 线程配置更易
  获得稳定尾延迟。

## 10. 架构优劣总结

### Redis

优点：

- pipeline=1 和单 key MULTI/EXEC 路径成熟，当前测试吞吐和尾延迟更稳；
- 简单命令的单请求延迟较可预测；
- I/O threads 能并行读写和解析，在网络处理较重时有价值。

限制：

- 命令执行仍由主线程串行完成；复杂结构或 pipeline 摊薄网络开销后，主线程成为上限；
- 增加到 16 I/O threads 不能带来命令执行多核扩展；
- 大 pipeline 下吞吐回退且尾延迟显著增加。

### Dragonfly

优点：

- uniform key 和 pipeline workload 能利用多个 proactor/shard；
- Hash、List、ZRANGE 等结构在 8 物理核上有明显优势；
- pipeline=16/64 下吞吐扩展显著，尤其 List 和 Set。

限制：

- 单 key 小事务在本轮明显落后 Redis，且 P99/P99.9 长尾较大；
- 16 SMT threads 在低 pipeline 下回退明显；
- 高吞吐依赖 batching，不能把 pipeline=64 的 QPS当作单请求性能；
- 多 shard 事务、热点和持久化场景仍需独立测试。

## 11. 原始数据与复现

| 内容 | 路径 |
| --- | --- |
| 环境、版本、二进制 SHA256 | `/tmp/dragonfly-bench/formal/environment.txt` |
| memtier 核心结果 | `/tmp/dragonfly-bench/formal/core/` |
| redis-benchmark 五结构结果 | `/tmp/dragonfly-bench/formal/structures/` |
| 原始数据压缩归档 | `/home/Dragonfly/benchmark-results/redis-vs-dragonfly-standalone-20260821.tar.gz` |
| 可复现执行脚本 | `tools/benchmark/run_redis_dragonfly_standalone.sh` |
| 测试方案 | `docs/redis-vs-dragonfly-standalone-benchmark-plan.md` |

每个 memtier case 目录包含 JSON、文本报告、stdout、INFO before/after、pidstat、mpstat、vmstat、sar 和
进程内存快照。每个 redis-benchmark case 包含 CSV、stderr、INFO before/after 和相同资源采集。
压缩归档 SHA256 为 `be73586cba9fcd34df45151068c7f0afcb77bd8a05ff5043011d9dcce2259fc6`。

## 12. 后续正式工作

当前数据已经满足用户指定的“至少 1000 万请求或至少 60 秒”，但要升级为可用于最终采购/替换决策的
高置信报告，还应继续：

1. 在独立 load generator 上复跑，消除同机 CPU 竞争；
2. 每个核心 case 至少 5 轮，报告中位数、IQR、CV；
3. 增加 uniform/hotset/single-hot-key 三种分布；
4. 补充 TX2 Hash、TX3/TX4 多 key 和 Dragonfly shard width 1/2/4/8；
5. 增加等 offered-load 的 P99/P99.9 比较，而不只比较各自饱和 QPS；
6. 对关键 case 增加 300 秒长跑，检查 RSS、吞吐和尾延迟漂移。
