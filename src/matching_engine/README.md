# Matching engine 模块

运行程序默认采用方案 2 持久化；完整说明见 [当前设计](../../docs/task2_recovery/06_implemented_scheme2.md)。

| 分组 | 文件 | 职责 |
| --- | --- | --- |
| 撮合核心 | `engine.*`、`active_index.hpp` | 单线程价格/FIFO 撮合、结果收集、恢复订单插入 |
| 运行与接口 | `backend.hpp`、`server.*`、`main.cpp` | 网络命令、启动参数、提交后的通知 |
| 行情读通道 | `snapshot_mailbox.*` | 为行情线程提供主线程复制的已提交视图 |
| 持久化 | `persistence/` | 双写线程、有界结果生命周期、二进制编码、同步、通知屏障 |
| 快照与恢复 | `recovery/` | 分页 COW 状态、快照发布、日志轮换、连续前缀恢复 |

`MatchingEngine` 保留纯内存接口，供原核心测试使用；`DurableEngine` 包装它并控制对外可见性。两个写线程只借用不可变结果，不访问工作订单簿。所有后台线程退出后才能销毁结果和快照页。

构建：仓库根目录执行 `./src/scripts/build.sh`。测试见 [recovery 测试说明](../../tests/recovery/README.md)。
