# 持久化队列对照实验

这里的 SPSC 队列只用于实验。正式 `matchingengine` 继续使用
`src/matching_engine/persistence/queue.hpp` 中的互斥锁与条件变量队列。

从仓库根目录执行：

```sh
cmake -S src -B build/queue-study -DCMAKE_BUILD_TYPE=Release -DMARKETLAB_BUILD_TESTS=ON
cmake --build build/queue-study --target persistence_benchmarks persistence_tests -j 8

./build/queue-study/bin/persistence_queue_benchmark --check
./build/queue-study/bin/persistence_tests build/queue-study/checks
./build/queue-study/bin/persistence_queue_recovery_spsc_park build/queue-study/checks
./build/queue-study/bin/persistence_queue_recovery_spsc_yield build/queue-study/checks

python3 tests/recovery/bench/run.py --output build/queue-study/formal-results
```

目标均为 `EXCLUDE_FROM_ALL`，需要显式构建；不会把性能测试混入正常运行程序或 CTest 默认回归。
新的 CMake 块只在启用 `MARKETLAB_BUILD_TESTS` 时读取。

`run.py` 顺序执行各候选，轮换候选次序，默认每个配置 5 次。队列测试每次合计 200 万项；
真实引擎测试每次预置 10 万活跃订单，再执行合计 100 万条买卖交替的新单命令，
分别测 1、2、4、8 个 shard。全系统商品数量固定为 256。

```sh
# 更大的活跃订单集；这里选择 1、4 个 shard，各重复 3 次。
python3 tests/recovery/bench/run.py \
  --output build/queue-study/million-active-results \
  --active-orders 1000000 --commands 1000000 \
  --shards 1 4 --repeats 3 --skip-queue
```

程序保留真实日志编码、write、fsync、提交屏障和通知回调。计时不包含预置、快照和重启，
但每轮计时后都会创建快照、重新打开引擎，并检查恢复的盘口及提交边界。
没有 TCP、Gateway 或 Publisher，因此这里的 commands/s 不能直接当成对外系统 TPS。

延迟是每 127 条命令采样的 command 调用前到 Accepted 回调的时间，含 buffer 等待，
样本交替覆盖买卖请求。CPU 核数为进程 CPU 时间与墙钟时间之比。
只在独立进程中顺序跑这些测试，避免同时编译、运行其他 benchmark 或清理系统缓存。

输出包含 `queue.csv`、`engine.csv`、每轮日志、`metadata.json` 和 `summary.json`。
元数据记录构建参数、源文件摘要及负载配置；summary 使用各轮中位数，并保留吞吐最小值和最大值。
不要用固定吞吐门槛判断正确性，也不要把单次最好结果当作结论。

每个引擎实验只清理自己用 `mkdtemp` 创建的目录；不会触碰真实交易数据。
恢复正确性测试的输出目录保留供检查。测试运行需要相应输出目录有足够可用空间。

文件分工：

| 文件 | 用途 |
| --- | --- |
| `spsc_queue.hpp` | 实验有界单生产者 / 单消费者环，支持外部 close、排空及两种等待策略 |
| `queue_benchmark.cpp` | 队列顺序、完整性、并发关闭测试，以及纯传递吞吐测试 |
| `engine_benchmark.cpp` | 实际撮合、写盘、通知、最终盘口和恢复检查 |
| `CMakeLists.txt` | 在构建目录生成 include overlay，复用同一份正式引擎源码 |
| `run.py` | 顺序运行、轮换顺序、保存原始数据和统计 |

SPSC 不是通用 MPMC 队列，容量须为 2 的幂；实验未处理超过 `2^63` 项后的游标回绕。
`spsc_park` 的快路径用原子操作，空满时允许阻塞，不应把整个阻塞接口称为严格 lock-free。
`spsc_yield` 的两条日志队列空满时持续重试；低频快照队列仍使用休眠等待。
正确性测试和 ThreadSanitizer 能发现部分错误，但不等于形式化证明或生产认证。

设计和实测解释见
[`07_recovery_notifications_wal_and_queue_benchmarks.md`](../../../docs/task2_recovery/07_recovery_notifications_wal_and_queue_benchmarks.md)。
