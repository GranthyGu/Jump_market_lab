# Task 3 测试

从仓库根目录构建、运行：

```sh
cmake -S src -B build/task3 -DCMAKE_BUILD_TYPE=Release -DMARKETLAB_BUILD_TESTS=ON
cmake --build build/task3 --target demo_tests -j8
ctest --test-dir build/task3 --output-on-failure -R '^spread_'
```

统一 `tests/run_all.sh --regression-only` 自动包含这些 CTest。网络测试使用临时配置、动态 loopback 端口和独立持久化目录，不使用默认交易目录。

| 文件 | 验证内容 |
| --- | --- |
| `depth.cpp` | 固定三档、12 条推导、依赖位图、more、版本、紧凑 delta/snapshot、百万次推导 |
| `execution.cpp` | 价格/真实 FIFO/AB 优先、补档屏障、固定选择计划、FIFO 片段配对及百万次选择 |
| `persistence.cpp` | 旧/新类型混合帧、负价/零价、未封闭 Result 不落盘、显式公开边界 0、索引分页、原样复制及重启 |
| `market.cpp` | D/I 合并、纯 implied 档、route 订单计数、空视图、V1/V2 编码和客户端 delta |
| `network.cpp` | 独立组头、TCP 分帧、多监听器、连接/epoch/实例约束 |
| `coordinator.cpp` | 来源/视图完成乱序、脏路径重算、view-only、正常结束/短路径、百万次控制循环 |
| `session.cpp` | 四个真实 DurableEngine、12 路所有权、不同 FIFO 切片、延后明细与先行视图、多轮、5000 小单、随机参考模型 |
| `publication.cpp` | MEM_DONE 与落盘乱序、参与者集合、分块完整性、有界窗口、每片历史游标、不可变行情候选 |
| `process.cpp` | 8 引擎 + Router + Coordinator + Gateway，全部 12 路真实 peer 交易、五档补窗、客户端历史、短路径/单请求延迟、5000 单突发与 15000 本地成交通知、单片恢复、部分内存故障暂停 |
| `lifecycle.py` | 真实维护启动器：S/A/B/C 替换、S 的 A→B→A、不可达候选回退、监督单成员故障重接入、干净全组关闭恢复、拒绝不确定关闭后重新启动、真实 Publisher V2 与 CLI 混合成交显示 |

结果在 `build/task3/spread-results/<项目>/`。C++ 测试写 `suite.json`、`metrics.csv`、`samples.csv`；两个进程项目保留本次配置、全部子进程日志和数据目录，生命周期测试另有 `results.json`。重新执行会更新该项目汇总，原进程运行目录保留用于定位问题。

性能口径必须分开：核心循环为进程内批次平均；单请求测量包含 Gateway TCP、组通信、fsync 和回复；5000 请求同时入队的分位数包含排队时间；一个 5000 片段扫单只有一个样本，不能当作每订单 p99。`--trace` 会增加日志开销。详细实测见 [验证报告](../../docs/task3_spreads/06_validation_results.md)。

故障测试中的 SIGSTOP/SIGKILL 只针对测试创建的进程。部分跨片执行发生故障时，正确结果是保留未完成状态并暂停，且不发布片面成交；此测试不声称已有跨片自动回滚或分布式故障原子提交。
