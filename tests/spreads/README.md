# Task 3 测试

从仓库根目录构建、运行：

```sh
cmake -S src -B build/task3 -DCMAKE_BUILD_TYPE=Release -DMARKETLAB_BUILD_TESTS=ON
cmake --build build/task3 --target demo_tests -j8
ctest --test-dir build/task3 --output-on-failure -R '^spread_'
```

统一 `tests/run_all.sh` 自动包含这些 CTest、进程性能测试及八种逐单延迟场景；`--regression-only` 只运行 CTest。网络测试使用临时配置、动态 loopback 端口和独立持久化目录，不使用默认交易目录。

实现按组件分组：Router 发布控制在 `src/matching_engine/cluster/router_spreads_publication.*`，shard 接入在 `runtime/spread_worker.*`，组行情在 `market_data/spread_market.*`，通信在 `spreads/transport/`。测试仍按行为保留本目录分组；`spread_publication_tests` 与 Router 编译同一个发布源文件，`session.cpp` 显式包含 Coordinator 测试依赖。`tests/build.sh`、`tests/run_all.sh` 和运行程序构建脚本继续通过 CMake 目标定位源码，不依赖旧文件路径。

| 文件 | 验证内容 |
| --- | --- |
| `depth.cpp` | 固定三档、12 条推导、依赖位图、more、版本、紧凑 delta/snapshot、百万次推导 |
| `shared_depth.cpp` | 六合约、20 路、共享来源价格与优先级、部分重叠、跨档两手 B、九段上界、B 报价取整；60,000 个目标侧与独立逐手 oracle 比较、增量重算与全量比较、百万次共享重算 |
| `execution.cpp` | 价格/真实 FIFO/AB 优先、补档屏障、固定选择计划、FIFO 片段配对及百万次选择 |
| `persistence.cpp` | 旧/新类型混合帧、负价/零价、未封闭 Result 不落盘、显式公开边界 0、索引分页、原样复制及重启 |
| `market.cpp` | 复用真实五档合并 D/I、同命令候选刷新、第四／五档及订单数变化、纯 implied／空视图、V1/V2 编码和客户端 delta |
| `network.cpp` | 独立组头、分段入发送队列、输入生命周期、容量复用与压缩、整体容量检查、TCP 分帧、多监听器、连接/epoch/实例约束 |
| `bootstrap.cpp` | 确定性重现握手后、首次接入前的 Pause；Worker/Router 可继续恢复，接入后错误 epoch 仍断开，Router 不发送 epoch=0 的通知 |
| `coordinator.cpp` | 来源/视图完成乱序、脏路径重算、view-only、正常结束/短路径、百万次控制循环 |
| `session.cpp` | 四个真实 DurableEngine、12 路所有权、不同 FIFO 切片、延后明细与先行视图、多轮、5000 小单、随机参考模型 |
| `butterfly_session.cpp` | 新增八路、1:2:1 数量与金额守恒、四片 Batch/恢复、跨路径执行顺序、110/111 B 成交、奇数余量走 AB、第三/四档配对、10,000 笔共享来源订单；与旧测试共用 `session_fixture.hpp` |
| `publication.cpp` | MEM_DONE 与落盘乱序、参与者集合、分块完整性、有界窗口、每片历史游标、不可变行情候选 |
| `process.cpp` | 8 引擎 + Router + Coordinator + Gateway，旧 12 路及蝶式跨档、B 双手、行情、类型化历史和恢复；五档补窗、短路径/单请求延迟、5000 单突发与 15000 本地成交通知、部分内存故障暂停 |
| `latency.cpp`、`latency.py`、`latency_analysis_test.py` | 八种真实 TCP 负载，逐笔关联 start/MEM_DONE/客户端完整成交；参与片与计时完整性校验，隔离的探针构建，八个分析器测试 |
| `lifecycle.py` | 真实维护启动器：S/A/B/C 替换、S 的 A→B→A、不可达候选回退、监督单成员故障重接入、干净全组关闭恢复、拒绝不确定关闭后重新启动、真实 Publisher V2 与 CLI 混合成交显示 |

结果在 `build/task3/spread-results/<项目>/`。C++ 测试写 `suite.json`、`metrics.csv`、`samples.csv`；两个进程项目保留本次配置、全部子进程日志和数据目录，生命周期测试另有 `results.json`。重新执行会更新该项目汇总，原进程运行目录保留用于定位问题。

性能口径必须分开：核心循环为进程内批次平均；单请求测量包含 Gateway TCP、组通信、fsync 和回复；5000 请求同时入队的分位数包含排队时间；一个 5000 片段扫单只有一个样本，不能当作每订单 p99。`--trace` 会增加日志开销。详细实测见 [验证报告](../../docs/task3_spreads/06_validation_results.md)。

独立延迟测试使用相同进程程序的 `--benchmark` 模式，关闭逐消息 trace；短路径和 implied 场景各预热 200 次、采样 2000 次，另运行 5000 请求突发及 5000 片段扫单。默认重复五次并汇总原始样本：

```sh
python3 tests/spreads/benchmark.py --build-dir build/task3/bin \
  --output build/task3/spread-benchmark --repeats 5
```

可加 `--reference-dir <另一份完整二进制目录>`，交替运行两个构建；结果分别保存在 `current/`、`reference/`，`summary.json` 保存合并后的分位数与每轮数据，`comparison.json` 保存比较比例。测试串行运行，每轮使用新的隔离数据目录；不同时运行构建或其他压力测试。普通 CTest 继续开启 trace 并覆盖完整故障流程。

每轮另调用 `spread_market_tests <结果目录> --benchmark`：预热 1000 次后，对真实五档和 implied 不变的同命令候选捕获执行 100 万次。该微基准的样本是每批 1000 次的平均调用时间，不是端到端延迟。

逐单延迟测试分别测 `memory − start`（进入执行后至内存完成，不含此前排队）和 `client − send`（发送至收齐本单全部成交和最终 Filled，包含排队与落盘回包）。正常服务的探针默认关闭，脚本可从已有正常构建准备独立测量程序：

```sh
python3 tests/spreads/latency.py --prepare-build build/task3 \
  --output build/task3/latency-next --count 5000 --warmup 200 --windows 128 --repeats 3
```

`--output` 必须是新目录。也可用 `--build-dir <已启用探针的 bin>`。默认覆盖 ordinary-hot、ordinary-balanced、group-direct、group-implied、group-butterfly-in、group-butterfly-out、group-butterfly-b、group-shared。B 蝶式场景每笔订单两手，其他场景每笔一手。共享场景先由 AB 使用共同的 B 池，之后切换到蝶式；它不是每笔一手订单同时执行两条路径，单笔多路径在 session 测试中验证。

原始时间戳、参与片、成交校验、每轮统计保存到结果目录；不把预热订单计入均值，也不裁去慢样本。最新推导语义和本次测试实测分别见本地 [09](../../docs/task3_spreads/09_shared_sources_and_butterfly_implied.md)、[10](../../docs/task3_spreads/10_shared_implied_validation_and_latency.md)。`docs/` 继续由 Git 忽略。

故障测试中的 SIGSTOP/SIGKILL 只针对测试创建的进程。部分跨片执行发生故障时，正确结果是保留未完成状态并暂停，且不发布片面成交；此测试不声称已有跨片自动回滚或分布式故障原子提交。
