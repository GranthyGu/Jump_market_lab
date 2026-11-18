# Task 3：按组件整理与回归验证

日期：2026-11-18。验证代码提交为 `71085fa`。本次按确认的目录结构重新归置实现，并同步构建目标、测试包含关系和文档。完整目录与职责见 [05 实现说明](05_implementation_and_data_structures.md)；原算法、压力和延迟测量见 [06](06_validation_results.md)，本次冻结的逐项结果见 [验证数据](validation/2026-11-18-layout-results.json)。

## 1. 代码归属与依赖

| 职责 | 当前文件 | 构建归属 |
| --- | --- | --- |
| Router 的参与者结果汇总、历史游标、连续公开边界 | [cluster/router_spreads_publication.*](../../src/matching_engine/cluster/router_spreads_publication.hpp) | Router 可执行程序；发布测试直接编译同一实现 |
| shard 连接与价差执行适配 | [runtime/spread_worker.*](../../src/matching_engine/runtime/spread_worker.hpp) | `marketlab_matching_runtime` |
| 真实/implied 行情合并、不可变候选、发布许可 | [market_data/spread_market.*](../../src/matching_engine/market_data/spread_market.hpp) | `marketlab_matching_core` |
| 组报文、三档与结果分块编解码 | [spreads/transport/protocol.*](../../src/matching_engine/spreads/transport/protocol.hpp) | 核心库中的共享组协议实现 |
| 连接握手/心跳与接入报告/端点目录 | [spreads/transport/links.*](../../src/matching_engine/spreads/transport/links.hpp)、[join.*](../../src/matching_engine/spreads/transport/join.hpp) | 核心库中的共享组通信实现 |
| 三档模型、推导、成员映射 | [spreads/depth.*](../../src/matching_engine/spreads/depth.hpp) | 核心库；移除对持久化编解码和组网络头的依赖 |
| 执行选择及单命令状态机 | [execution.*](../../src/matching_engine/spreads/execution.hpp)、[session.*](../../src/matching_engine/spreads/session.hpp) | 核心库；Session 不再包含 Coordinator 类头文件 |
| Coordinator 状态机与独立服务 | [spreads/coordinator/](../../src/matching_engine/spreads/coordinator/) | 状态机仍在核心库，服务和 main 编入 Coordinator 程序 |

`spreads/` 顶层只保留 depth、execution、session 六个文件，以及 transport、coordinator 两个子目录。按最终确认的结构保留独立的 links 与 join；没有合并连接状态和接入状态机。类名、命名空间、数据结构、消息线格式、线程及 Batch 提交机制保持。

原 publication 文件中的客户端事件正文继续复用已有协议，分块编码迁入 transport；磁盘订单/成交日志仍使用原 `DurableEngine::writer()` 和 `persistence/codec.*`。没有增加第二套日志编码，也没有增加消息往返或运行时转发层。

## 2. 构建与验证方法

```sh
cmake --build build/task3 --target demo_tests -j8
./src/scripts/build.sh --jobs 8
ctest --test-dir build/task3 --output-on-failure --output-junit component-layout-final.xml
```

第一步使用既有 Release 测试构建目录，第二步使用默认构建脚本，生成 Gateway、MatchingEngine、Router、GroupCoordinator、Publisher 和客户端六个运行程序。两个入口都经过实际构建。测试命令、可执行程序名称、日志事件名和启动参数保持；构建/启动脚本依赖 CMake 目标，无需新增一组脚本。

目录整理过程中发现 Session 测试曾间接通过 Session 头文件取得 Coordinator 定义；去除该依赖后，已在测试文件补上显式 include，再完成构建。初次编译输出保留在 `build/task3/component-layout-header-check.log`。编译器仍报告既有代码中的警告，例如第三方 ARC 宏的多余分号和聚合初始化警告；不将“构建成功”表述成零警告。

## 3. 结果与耗时

**Release 全部 26 项 CTest 通过，总墙钟耗时 168.95 秒。Task 3 的 10 项全部通过，其中 9 个 C++ suite 的 52 个用例全部通过，另有 Python 生命周期场景。**

| 检查 | 结果 | 墙钟耗时 |
| --- | --- | ---: |
| 第一批拆分后的行情、发布、native 多进程测试 | 3/3 通过 | 11.03 秒 |
| 显式 include 修正后的测试目标增量构建 | 成功 | 3.42 秒 |
| 默认 `src/scripts/build.sh --jobs 8` | 六个运行程序构建成功 | 13.58 秒 |
| 最终完整 Release CTest | 26/26 通过，包含 Task 3 的 10 项 | 168.95 秒 |

Task 3 每项耗时如下，合计 41.13 秒；这是上表完整回归的子集，不应重复计入总耗时。

| CTest | 秒 | 结果 |
| --- | ---: | --- |
| `spread_depth_correctness_stress` | 1.038 | 通过 |
| `spread_persistence_correctness` | 0.645 | 通过 |
| `spread_execution_correctness_stress` | 0.948 | 通过 |
| `spread_market_correctness` | 0.383 | 通过 |
| `spread_network_correctness` | 0.401 | 通过 |
| `spread_coordinator_correctness_stress` | 0.421 | 通过 |
| `spread_session_correctness_stress` | 1.304 | 通过 |
| `spread_publication_correctness` | 0.378 | 通过 |
| `spread_native_process` | 9.480 | 通过 |
| `spread_lifecycle_process` | 26.136 | 通过 |

覆盖包括 12 条真实 peer 路径、FIFO 配对与延迟明细、多轮补窗、负价/零价、未完成/乱序分块、不可变行情候选、有界窗口、5000 单压力、单成员故障恢复、部分跨片执行暂停、四成员计划替换及候选失败回退。完整回归也包含原 Gateway、Publisher、shard 撮合、双日志恢复、成交历史和客户端重连。此次未重新运行 sanitizer 或可选独立性能实验；原 UBSan/ASan 结果仍按 06 的日期和基线理解。

静态核对确认：旧源码路径在 src/tests/docs 中无残留引用，spreads 顶层严格为六个核心文件，修改涉及的文档链接及 Markdown 代码围栏检查通过。原撮合、双日志、恢复和公共行情传输的实现文件没有改动。

构建时间来自已有缓存的增量构建，不是全新目录的编译基准；CTest 耗时是验证墙钟时间，不是订单延迟。此次复用既有正确性、corner case、压力及真实进程测试，不新增只验证文件名或复制实现的运行时测试。

原始记录位于 `build/task3/component-layout-*.log`，第一批测试记录为 `component-layout-stage1.xml`，完整回归记录为 `component-layout-final.xml`。真实进程场景还保留本次独立的进程日志和数据目录，入口仍为 `build/task3/spread-results/`。此前 [06](06_validation_results.md) 的性能数字及冻结数据保持原测量口径。

## 4. 分批提交

- `8e28ddb`：拆开 Router 发布、shard 行情与运行时适配，迁移结果分块编码，更新对应构建和文档；3 项针对性回归通过。
- `71085fa`：组通信集中到 transport，三档编解码归入协议，公共成员映射移出 Coordinator，更新依赖和目录文档；完成默认构建与 26 项完整回归。

本报告及验证 JSON 随后单独提交，只记录上述已验证的代码与结果。
