# Task 3：实现验证、压力与延迟结果

日期：2026-11-17。代码基线 `50c0c6e`，本报告的后续文档提交不改变二进制。代码构造与内存布局见 [05](05_implementation_and_data_structures.md)；可解析结果保存为 [验证数据](validation/2026-11-17-results.json)。

## 1. 本次结果与复现

**Release 已注册的 26 项 CTest 全部通过，墙钟总耗时 159.79 秒；其中 Task 3 的 10 项全部通过。9 个新增 C++ suite 共 52 个用例，另有 Python 真实维护/恢复/Publisher/CLI 场景。8 项新增核心与协议测试的 UBSan 检查均通过。**

环境为本机 macOS 26.5 ARM64，AppleClang 16.0.0、CMake 3.31.3、C++20 Release。真实进程测试使用 loopback TCP、隔离数据目录和动态端口；native/lifecycle 开启 `--trace`。没有固定 CPU、关闭其他系统任务或使用远端网络，因此这些是本机验证数据，不是生产延迟保证。

```sh
./src/scripts/build.sh --jobs 8
cmake -S src -B build/task3 -DMARKETLAB_BUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build/task3 --target demo_tests -j8
ctest --test-dir build/task3 --output-on-failure --output-junit task3-validation.xml
```

正式构建脚本已生成 `build/bin/` 下的 Gateway、MatchingEngine、Router、GroupCoordinator、Publisher 和客户端。原始 CTest 证据为 `build/task3/task3-validation.xml`、`build/task3/final-ctest.log`；各 suite 的 JSON、CSV 样本及进程日志位于 `build/task3/spread-results/`。本次没有运行可选 SQLite 对照和八个独立的旧性能实验，不能把本表等同于所有可选 benchmark 均已运行。

网络测试需要本机 TCP/UDP 绑定权限。第一次受限环境运行在 bind 时被拒绝；授权端口后重新执行完整 26 项，以下仅列最终成功结果。被拒绝的运行记录保存在 `build/task3/sandbox-denied-ctest.log`。

## 2. 每项耗时

| CTest | 秒 | 结果 |
| --- | ---: | --- |
| `demo_engine` | 0.191 | 通过 |
| `demo_gateway` | 6.962 | 通过 |
| `demo_publisher` | 28.235 | 通过 |
| `demo_integration` | 20.146 | 通过 |
| `snapshot_mailbox` | 0.138 | 通过 |
| `persistence_correctness` | 0.166 | 通过 |
| `persistence_optimizations` | 0.156 | 通过 |
| `recovery_process` | 3.571 | 通过 |
| `bpt_correctness_concurrency_stress` | 3.257 | 通过 |
| `bpt_cache_control_correctness` | 2.073 | 通过 |
| `trade_history_correctness` | 0.261 | 通过 |
| `trade_history_process` | 2.892 | 通过 |
| `planned_restart_process` | 18.545 | 通过 |
| `replication_correctness` | 0.016 | 通过 |
| `test_runner_correctness` | 1.410 | 通过 |
| `client_reconnect_process` | 34.487 | 通过 |
| `spread_depth_correctness_stress` | 0.638 | 通过 |
| `spread_persistence_correctness` | 0.178 | 通过 |
| `spread_execution_correctness_stress` | 0.547 | 通过 |
| `spread_market_correctness` | 0.007 | 通过 |
| `spread_network_correctness` | 0.019 | 通过 |
| `spread_coordinator_correctness_stress` | 0.031 | 通过 |
| `spread_session_correctness_stress` | 0.750 | 通过 |
| `spread_publication_correctness` | 0.006 | 通过 |
| `spread_native_process` | 8.978 | 通过 |
| `spread_lifecycle_process` | 26.115 | 通过 |

总墙钟时间包含 CTest 调度和子进程退出；不等于业务延迟。失败后的调试尝试未混入下表的最终性能样本。

## 3. 正确性与 corner cases

- 12 条 IN/OUT 路径同时有进程内真实 DurableEngine 测试和真实 peer TCP 测试；核对各片只记录自己的实际订单与成交数量。
- 对不同 FIFO 切分 1/2 与 2/1 配对，核对共同 execution key、fillNo、时间及增量 OrderUpdate；每条命令每个实际参与者只封闭一个 Batch。
- 覆盖价格优先、真实 FIFO 优先、同价 AB 优先 AC、多轮 more 补档先于更差流动性；40 个固定种子随机买/卖场景与独立逐份全深度参考结果比较。
- 故意推迟成交明细，使来源刷新/ROUND_READY 先完成，验证双条件汇合和有序分配；包含最终剩余挂单同步、view-only 成员没有 Batch、hasMore 变化必须退出短路径。
- 覆盖负价、零价、限价与价格范围、纯 implied 档、同价 D/I、route 数量计数、空视图、delta/snapshot 版本和截断/非法报文。
- 持久化前不公开；MEM_DONE/落盘乱序、分块未完、重复块、错误参与者、窗口/字节预算均有断言。公开历史边界 0 即空，即使本片已有持久化成交，也不能回退为“最新”。
- 混合 Direct/Leg/Spread 日志、用户可见性、自成交去重、分页、原样复制、有符号价格快照及恢复；真实 Publisher 输出 V2，真实 CLI 完整显示三类成交。
- Shard 1—4 原路径、原 Gateway/Publisher/历史/计划重启均执行回归；关联组暂停时仍验证普通 shard 能继续接单撤单。

## 4. 真实 TCP 延迟和压力

每个单请求样本从客户端发送开始。短路径截止到持久化后的 Accepted；implied 截止到三片持久化条件满足并收齐三笔本地成交及 OrderUpdate。短路径先放置四个稳定价位，再在第五档挂撤，保持 top3 和 hasMore 不变；Coordinator 原始统计有 204 次 LOCAL_COMPLETE。

| 场景 | 样本 | mean ms | p50 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: |
| 本地短路径 | 100 | 0.349 | 0.337 | 0.548 |
| 三片 implied，收齐全部本地成交通知 | 100 | 0.865 | 0.850 | 1.055 |
| 5000 请求突发，等待 Accepted | 5000 | 987.606 | 982.635 | 1740.550 |

单请求两组各仅 100 个样本，p99 依赖少数尾部样本。5000 单场景在收到回执前一次性发出全部请求，所以约秒级的分位数主要包含队列等待，不能拿它当轻负载服务延迟。此次 Accepted 吞吐为 **2563 请求/秒**。

随后用一张 SP_AB 主动单消耗 5000 个 A 的一手 FIFO 订单，并配对 B 的 5000 手来源，产生 **15000 条本地 Trade 及对应更新**。从发送到全部落盘条件满足和通知收齐耗时 **158.346 ms**，折算 **31576 个价差 fill/秒**。这是单条大命令的一个样本，不提供每订单 p99，也不把三片本地记录数当三倍价差成交量。

## 5. 进程内计算测量

| 场景 | 次数 | 总耗时 ms | 百万次/秒 |
| --- | ---: | ---: | ---: |
| 脏来源推导 | 1,000,000 | 61.304 | 16.31 |
| 固定计划选择 | 1,000,000 | 35.046 | 28.53 |
| Coordinator 本地完成路径 | 1,000,000 | 25.859 | 38.67 |

这些循环不含 TCP 和 fsync；CSV 中的分位数是每批 1000 次的平均耗时分布，不是单笔端到端分位数。

四个真实 DurableEngine、进程内有序报文的 5000 FIFO 片段场景：MEM_DONE 为 **3.657 ms**，从同一开始点到全部实际参与者持久化为 **7.688 ms**。后一个时间已经包含前一个，不应相加；这组没有 socket，不能替代上一节真实网络数据。

## 6. 维护、重接入与故障

| Shard | 替换 | peer/视图准备 ms | 组停发窗口 ms | 维护总计 ms |
| --- | --- | ---: | ---: | ---: |
| 5 | A→B | 7.423 | 16.172 | 176.772 |
| 6 | A→B | 8.315 | 18.232 | 127.730 |
| 7 | A→B | 7.142 | 16.252 | 123.106 |
| 8 | A→B | 7.758 | 16.734 | 121.513 |
| 5 | B→A | 7.774 | 17.150 | 123.216 |

此组使用小数据量、无并发交易的维护样本，队列峰值均为 0；不是大数据复制或维护期间的吞吐压力结论。每次替换核对五字段 Boundary、活动订单、完整历史字节、所有成员行情，随后执行新的跨片成交；Router/Coordinator 以及健康成员 PID 保持不变。

- 监督进程发现完整边界上的成员 SIGKILL，只恢复该成员，恢复后验证时间 **446.621 ms**。
- 候选实例的 group 端口故意不可达，在激活前超时并重新准备原实例，恢复原服务；整个故障场景 **20635.754 ms**，主要是 20 秒维护截止等待。
- 干净停止后重开组并验证公开状态、Publisher V2、再次成交，耗时 **1061.416 ms**。不确定关闭标记会拒绝自动新建 Router/Coordinator。
- 另一个真实故障测试先 SIGSTOP A，确认 B 已修改真实来源并发送 SOURCE_DONE，然后 SIGKILL A 并重启它。Router 仍保留原 memory/publication 边界，不发主动单 Accepted 或片面成交；组持续暂停，Shard 1 仍能下单撤单。
- 单成员断线期间还验证未执行请求留在组队列，恢复后沿原关联只执行一次。

部分内存/部分落盘尾部没有自动跨片补齐或回滚协议。测试证明此时不误放行，不声称跨片故障原子提交已经解决。

## 7. Sanitizer 与局限

UBSan 使用独立 `build/task3-ubsan/`，`-fsanitize=undefined -fno-omit-frame-pointer`，运行时 `UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1`。7 个无 socket 项首次通过；网络项因沙箱绑定限制单独获准补跑后通过。最终 8 项均通过，有效测试执行耗时合计 **7.234 秒**。保留 `build/task3/ubsan-tests.log`、`ubsan-network-tests.log` 及对应 JUnit；合计为各最终有效测试时间，不是一次整体墙钟时间。

AddressSanitizer 构建成功，但在测试体执行前触发 `sanitizer_malloc_mac.inc:189` 初始化 CHECK；只有 `int main() { return 0; }` 的独立程序也复现，关闭 Nano allocator 仍相同。因此 **ASan 未完成验证，不能计为通过**。相关记录为 `build/task3/sanitizer-tests.log`、`asan-probe.cpp`、`asan-probe.log`、`asan-probe-nano-off.log`。未声称已经获得 ASan 内存安全证明。

测试没有覆盖多主机延迟、掉电介质语义、无限规模订单簿或任意故障下自动恢复；当前实现的相应行为边界与设计保持一致。
