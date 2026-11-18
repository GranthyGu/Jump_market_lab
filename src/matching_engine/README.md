# Matching engine 模块

运行程序默认采用方案 2 持久化；当前提交、快照与恢复路径见 [订单状态与双日志一致性](../../docs/task2_recovery/11_orderbook_delta_snapshot_consistency.md)。默认启动脚本由 `cluster/run.py` 管理 Router、独立 GroupCoordinator 与各 shard 的 A/B 进程，正常交易路径由 C++ 执行。

演示配置包含 24 个普通商品 `ITEM0001`—`ITEM0024`，按轮询分到 shard1—4，每片六个；shard5—8 承载 A、B、C 和三个价差品种，共 30 个商品。A/B/C/SP_AB/SP_AC/BF_ABC 的全局 ID 为 25—30。已有 1000 或 20 个 ITEM 的持久化数据不兼容新的品种字典，演示请使用独立目录：`./src/scripts/run_matching_engine.sh --data-dir src/config/.marketlab-data/exchange-demo24`；旧数据保留。

| 分组 | 文件 | 职责 |
| --- | --- | --- |
| 撮合核心 | [core/](core/)：`engine.*`、`active_index.hpp`、`backend.hpp` | 单线程价格/FIFO 撮合、活动订单定位、结果收集，以及状态所有者调用的后端接口 |
| 进程与服务 | [runtime/](runtime/)：`server.*`、`spread_worker.*`、`main.cpp` | 启动参数、订单 reactor、命令与通知适配、后台服务生命周期；`spread_worker.*` 接入价差 Session、peer 与恢复就绪状态 |
| 行情读通道 | [market_data/](market_data/)：`snapshot_mailbox.*`、`spread_market.*` | 通过邮箱请求状态所有者复制公开五档视图；`spread_market.*` 合并真实/implied，并管理不可变候选及发布许可 |
| 持久化 | [persistence/](persistence/) | 双日志写线程、有界结果生命周期、二进制编码、同步、通知提交屏障 |
| 快照与恢复 | [recovery/](recovery/) | 分页 COW 状态、快照发布、日志轮换、连续前缀恢复 |
| 成交历史 | [trade_history/](trade_history/)：`history_store.*` | 已提交成交日志的异步索引、索引快照与恢复、有界分页查询 |
| 历史索引实现 | [trade_history/index/](trade_history/index/)：`trade_index.hpp`、`page_file.*`、`cmu/` | 按用户与成交位置查找的 B+ 树、页文件、Buffer Pool、ARC；`cmu/` 保留 BusTub 来源与许可证 |
| 路由与计划重启 | [cluster/](cluster/)：`router.*`、`router_spreads.*`、`router_spreads_publication.*`、`worker.*`、`rpc.*`、`run.py` | C++ 订单路由与引擎控制；价差组调度及实际参与者结果汇总在主 Router 内完成；Python 协调复制、排空与接管 |
| 跨片价差 | [spreads/](spreads/)：`depth.*`、`execution.*`、`session.*` | 固定三档/20 路推导、共享来源分配、原 FIFO 上的执行计划和多轮 Session |
| 组通信 | [spreads/transport/](spreads/transport/)：`protocol.*`、`links.*`、`join.*` | 组帧、来源与持久化结果编解码，握手/心跳和恢复报告；Router、Coordinator、shard 共用 |
| 组协调器 | [spreads/coordinator/](spreads/coordinator/)：`coordinator.*`、`server.*`、`main.cpp` | 组来源/视图/内存完成状态机、TCP 服务与接入维护、独立 Coordinator 程序入口 |

`MatchingEngine` 保留纯内存接口，供原核心测试使用；`DurableEngine` 包装它并控制对外可见性。两个写线程只借用不可变结果，不访问工作订单簿。所有后台线程退出后才能销毁结果和快照页。

Task 3 继续调用同一个 `MatchingEngine`，`core/group_execution.cpp` 只提供组内执行所需的原簿操作。Shard 1—4 使用原普通路径；Shard 5—8 在启用 spreads 后使用 Session，每条命令每个实际参与者只封闭一次 Batch，MEM_DONE 与 fsync 分离。代码构造、缓存大小、报文和恢复边界见 [Task 3 实现说明](../../docs/task3_spreads/05_implementation_and_data_structures.md)，运行时检查与意外中断处理见 [工程补充](../../docs/task3_spreads/07_implementation_guards_and_interruptions.md)，测试入口见 [spreads 测试](../../tests/spreads/README.md)。

已确定结束的终结轮将 Batch 封闭与最终 implied 安装重叠；Coordinator 仍等待所有来源、视图确认和累计参与者封闭后才发 MEM_DONE。各片完成本地真实修改即报告 SOURCE_DONE，主动单数量确认与 FIFO 配对继续独立校验。Group 握手版本为 3，组内程序须一致；持久化格式保持原样。流程与实测见 [延迟优化验证](../../docs/task3_spreads/11_spread_latency_optimization.md)。

`trade_history/index/` 是成交历史的可重建派生索引；成交事实仍由 `trades.log` 保存。历史查询捕获每片的提交边界，等索引追齐后分页返回，索引写入不阻塞成交通知。具体见 [历史索引设计](../../docs/task2_recovery/bpt_trade_history/01_bpt_design_and_performance.md) 与 [查询 API](../../docs/task2_recovery/bpt_trade_history/02_history_api_and_client.md)。

跨组件通用设施位于 [shared_data/runtime/](../shared_data/runtime/)：`io.*` 管理文件描述符与通用网络 I/O，`thread.*` 管理停止和等待，`log.*` 提供日志，`wakeup.hpp` 唤醒 reactor，`availability.hpp` 记录可用性状态。订单协议仍在 `shared_data/orders/`，行情协议与分片视图传输仍在 `shared_data/market_data/`。

构建：仓库根目录执行 `./src/scripts/build.sh`。启动与维护见 [Exchange 运行指南](../../docs/EXCHANGE_RUN.md) 和 [计划重启设计](../../docs/task2_recovery/09_planned_restart_router_design.md)。测试见 [recovery 测试说明](../../tests/recovery/README.md)。

2026-11-18 按组件整理后的构建归属、复用关系及完整回归耗时见 [目录整理验证](../../docs/task3_spreads/08_component_layout_and_validation.md)。
