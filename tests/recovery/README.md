# 方案 2：持久化与恢复正确性测试

完整实现说明：[docs/task2_recovery/06_implemented_scheme2.md](../../docs/task2_recovery/06_implemented_scheme2.md)。成交历史查询接口及其验收按最新要求暂缓；这里测试写入屏障、订单恢复与文件轮换。

## 构建与运行

仓库根目录：

```sh
cmake -S src -B build/persistence-check -DCMAKE_BUILD_TYPE=Debug -DMARKETLAB_BUILD_TESTS=ON
cmake --build build/persistence-check --parallel 4
ctest --test-dir build/persistence-check -R 'persistence_correctness|recovery_process' --output-on-failure
```

也可运行 `./tests/build.sh` 构建全部本地测试，再用 `./tests/run_all.sh --regression-only` 执行所有 CTest 回归；`./tests/run_all.sh` 还会运行独立性能实验、CMU 测试及故障审计。完整入口说明见 [tests/README.md](../README.md)。

每次测试在 `recovery-results/` 下创建新的隔离目录，保留日志和持久化文件供检查，不使用日常数据目录。真实进程测试使用动态 loopback 端口，需允许本机 TCP/UDP。用例失败返回非零。

## suite.cpp：13 个内部场景

| 场景 | 核对的业务和文件顺序 |
| --- | --- |
| trade_sync_before_notifications | 成交同步被卡住、订单侧完成：无业务通知，盘口不前进；两侧完成才发送 |
| order_sync_before_notifications | 反向阻塞订单同步，检查相同屏障；Accepted 必须先于本批成交事件 |
| restart_remaining_and_fifo | 部分成交后重建引擎，检查剩余量、FIFO、新订单/成交 ID 不复用 |
| continue_matching_during_previous_sync | 前一笔 fsync 被阻塞，下一笔仍可撮合；通知和可见状态按共同提交顺序推进 |
| snapshot_publication_and_delta_rotation | 暂停快照发布，同时撤单和成交；发布前旧代仍在，发布后只删旧代，恢复保留新代更新 |
| bounded_buffers_wait_without_loss | 在途容量设为 1，阻塞写线程；下一命令等待，释放后不丢失、不重复 |
| sync_failure_stops_visibility | 在订单同步前注入异常；失败批次不得对外通知或更新可见盘口 |
| incomplete_tail_and_corruption | 不完整尾帧可以截断；完整帧 CRC 损坏必须报错 |
| trade_only_crash_discards_unpaired_tail | 子进程只完成成交侧后直接退出，不执行析构；恢复不应用孤立成交 |
| order_only_crash_discards_unpaired_tail | 子进程只完成订单侧后退出；恢复不应用孤立订单变化 |
| zero_trade_order_only_crash_recovers | 新增未成交订单仅完成订单日志时崩溃；expected_trade_count=0 不要求存在成交帧 |
| zero_trade_gaps_after_checkpoint | 检查点之后混合新增、撤单和成交，成交帧序号有间隔时仍正确恢复 |
| published_snapshot_crash_before_prune | 新快照已经安全发布、旧代尚未删除时退出；恢复选择新快照，不重复应用旧增量 |

阻塞由可选内存钩子精确控制在 write/fsync/快照发布附近；时序不依赖猜测磁盘耗时。唯一短时间等待断言用于确认“容量确实阻塞”，释放条件仍由钩子决定。两个单边崩溃和快照崩溃用 fork 子进程内 `_exit`，不运行引擎正常关闭流程。

## optimizations.cpp：9 个优化正确性场景

注册为 `persistence_optimizations`，覆盖部分成交价格档位更新、紧凑订单记录的磁盘兼容、分页写时复制、异步用户查询、零成交查询、撤单与节点复用、查询数量/字节预算等待、写盘失败和旧文件恢复。

异步查询测试分别阻塞成交和订单同步，验证查询及后续撮合调用能先返回；只等查询对应的批次提交，固定结果不被后续撮合改变，`SnapshotEnd` 先于下一批业务通知。完整设计和场景表见 [文档07第10节](../../docs/task2_recovery/07_recovery_notifications_wal_and_queue_benchmarks.md#10-三项订单状态优化已实现)。

```sh
ctest --test-dir build/persistence-check \
  -R '^persistence_(correctness|optimizations)$' --output-on-failure
```

## process.cpp：真实 kill -9 与 publisher

启动正式 matching engine、gateway、publisher，执行：

1. 两位用户分别挂同价卖单 4、7，第三位买 8。
2. 消费正式业务通知，等待 publisher 查询显示剩余 3、订单数 1，以及磁盘快照发布。
3. 对引擎发送 SIGKILL，保持 gateway 和 publisher 运行。
4. 用同一配置/数据目录重启引擎。
5. 通过正常登录快照验证原订单 ID、filled=4、remaining=3。
6. 通过 publisher 正常查询入口验证新 epoch 与相同盘口。

这个测试核对 publisher 实际输出，不只检查引擎内部的内存状态。它不包含未实现的成交历史查询接口。

## 本次验证记录

2026-11-08，三项优化后：

- 完整 CTest：8/8 通过，63.79 秒，包含真实进程崩溃恢复。
- 原持久化套件 13 场景、新优化套件 9 场景通过。
- LLVM 22 ASan+UBSan：两套持久化测试通过，2.20 秒。
- LLVM 22 TSan：两套持久化测试通过，7.28 秒。
- 首次网络回归因沙箱禁止本机端口绑定失败；获准后完整重跑通过。

以下是优化前保留的历史记录。

2026-11-06，macOS arm64：

- 全量 CTest：7/7 通过，约 63.90 秒，含原 engine/gateway/publisher/integration 和邮箱测试。
- persistence_correctness：11 场景通过，约 0.57 秒。
- recovery_process：通过，约 3.57 秒。
- LLVM 22.1.8 ASan+UBSan：内部恢复场景全部通过，约 0.81 秒。
- LLVM 22.1.8 TSan：内部恢复场景全部通过，约 4.97 秒。

Sanitizer 构建位于 `build/persistence-llvm-asan`、`build/persistence-llvm-tsan`。AppleClang 16 自带检查器在本机初始化异常，后换 LLVM 22 验证；日常 Debug/Release 仍使用 AppleClang 16。

复现 ASan+UBSan（已安装 LLVM 的路径按本机调整）：

```sh
cmake -S src -B build/persistence-llvm-asan \
  -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm/bin/clang++ \
  -DCMAKE_BUILD_TYPE=Debug -DMARKETLAB_BUILD_TESTS=ON \
  '-DCMAKE_CXX_FLAGS=-fsanitize=address,undefined -fno-omit-frame-pointer' \
  '-DCMAKE_EXE_LINKER_FLAGS=-fsanitize=address,undefined'
cmake --build build/persistence-llvm-asan --target persistence_tests --parallel 2
ctest --test-dir build/persistence-llvm-asan -R persistence_correctness --output-on-failure
```

TSan 使用独立构建目录，把两处 sanitizer 参数换为 `-fsanitize=thread`。不要在同一构建目录混用两个检查器。
