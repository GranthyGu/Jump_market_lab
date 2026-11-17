# 测试与统一运行

测试包含组件演示、持久化恢复、成交历史查询、计划内重启，以及独立性能实验。Gateway、Publisher、Engine 和全链路四套组件测试共用根目录的 `harness.hpp`、`network.hpp`，由它们提供结果记录、进程管理和网络驱动。

## 构建与统一运行

```sh
./tests/build.sh
./tests/run_all.sh
```

`build.sh` 编译所有本地回归、正式服务、`gateway_client`、SQLite 对照和独立性能程序，输出到 `build/demo-tests/bin/`。需要 CMake、C++20 编译器、Python 3、SQLite 开发库及 POSIX 网络接口；SQLite 仅用于测试。

`run_all.sh` 默认执行 **所有已注册 CTest + 8 个独立测试/实验组**，按顺序运行，避免性能实验相互干扰。CTest 清单动态读取，新增已注册测试会自动纳入。某项失败、未构建或超时不会被忽略，也不会阻止后续项目运行；最终返回非零。

```sh
./tests/run_all.sh --list                  # 仅显示完整清单与命令
./tests/run_all.sh --quick                 # 全部项目；缩小独立性能实验的负载
./tests/run_all.sh --regression-only       # 仅 CTest 日常回归
./tests/run_all.sh --verbose               # 同时向终端输出每项完整日志
./tests/run_all.sh --build-dir build/demo-tests --output build/my-test-results
```

`--quick` 不删减测试项目，不缩小 CTest 内的正确性场景；它将独立性能实验减至一轮并缩小数据量，不能作为正式性能成绩。`--regression-only` 明确排除的项目会列入报告。默认完整性能实验使用各实验原有的正式参数，耗时明显长于原来的四套演示。

本地测试需要先构建；CMU 组首次运行会下载固定版本 BusTub，再构建上游测试和 benchmark，需要网络及上游构建依赖。可用 `--cmu-source PATH` 指定已有的固定版本源码，`--jobs N` 控制 CMU 编译并行度。下载或构建失败会报告失败，不会悄悄跳过。`run_all.sh` 不提交官方排行榜。

`--build-dir` 接受 CMake 构建目录或其 `bin/`；相对路径以调用时的目录解释。`--scale 1/2/3` 控制四套组件演示的负载。默认超时按各项目分别设置；`--timeout SECONDS` 可覆盖统一入口的外层时限，CTest 自身时限仍生效。

原来的四套组件演示仍可独立运行：

```sh
python3 tests/run.py all
./tests/gateway/run.sh
./tests/publisher/run.sh
./tests/engine/run.sh
./tests/integration/run.sh
```

## 测试范围

| 目录 | 正确性重点 | 压力和性能 |
| --- | --- | --- |
| [gateway](gateway/README.md) | 登录、会话替换、快照、权限、TCP 分帧、路由及后端故障 | 五组规模，固定 1000 商品对比 1/4/32 分片；最多 2000 商品；8 用户、16 请求窗口 |
| [publisher](publisher/README.md) | 全品种每秒发布、净增量、空增量、三接收端一致性、周期快照、来源故障 | 同样五组规模；并发 TCP 查询与组播接收同时进行 |
| [engine](engine/README.md) | 独立参考模型核对撮合、撤单、用户快照、五档；并发读取 | 16/1000/2000 商品、新单撤单、64 单扫单和自成交 |
| [integration](integration/README.md) | 真实交易与公开行情一致、双方成交、跨片快照、丢包恢复和重启 | 64 商品/2 分片与默认规模 1000 商品/4 分片，交易、采样、组播和查询同时运行 |
| [recovery](recovery/README.md) | 双日志通知屏障、FIFO 恢复、快照/增量一致性、写失败与 kill -9 | bench 子目录包含队列与分片性能对照 |
| [bpt](bpt/) | 索引、缓存、并发、成交历史 API/CLI、重启、CMU 公开测试 | 缓存/并发、SQLite、完整成交历史与恢复性能 |
| [restart](restart/README.md) | 复制、A→B→A、请求完整性、FIFO、历史连续性、切换失败回退 | 切换暂停、排队及客户端延迟 |
| [client](client/README.md) | 20 秒重连期限、不重发未知订单、真实引擎反复中断、红色告警去重及恢复后交易 | 记录故障期限和恢复耗时 |
| [runner](runner/) | 新 CTest 自动纳入、失败后继续、缺失程序报错、超时终止进程组 | 不设性能指标 |
| [spreads](spreads/README.md) | 三档/12 路、优先级、跨片 FIFO 配对、类型化历史、公开边界、负价/零价、组替换与故障暂停 | 百万次核心循环、5000 片段、多进程单请求和突发排队、维护耗时 |

网络测试启动真实组件子进程，并使用新生成的隔离配置及动态 loopback 端口，不依赖手工启动服务，不占用默认 7001/9100。服务日志和本次配置保留在结果目录；正常结束、用例异常及脚本超时都有清理路径。动态端口在启动前释放，存在常见的端口被外部进程抢占窗口；发生时结果会明确失败，重新运行即可。

## 结果怎么读

统一入口打印 `RESULT_DIR=...`，每次创建新的结果目录：

```text
build/test-results/日期时间-随机后缀/
  report.md                 # 每个测试/实验组的状态与日志链接
  summary.json              # 选中清单、命令、退出码、超时和显式排除项目
  demo_gateway/
    console.log
    ctest.xml
    artifacts/              # 原组件报告、配置及性能样本
  bpt_cache_concurrency_comparison/
    console.log
    artifacts/              # 每轮原始结果和汇总
    data/
```

CTest 内部测试可能继续把持久化数据写入构建目录下的原结果路径，具体见保存的命令与日志。四套组件的详细报告保持原格式；独立实验的原始结果保存在各组 `artifacts/` 中。`NOT RUN` 表示中断前尚未执行，不能视为通过。

测试使用隔离数据和本机动态端口，不依赖手工启动服务。网络项目需要允许本机 TCP/UDP。性能数据应结合各实验的计时范围阅读；不把核心耗时、TCP RTT、发布耗时和恢复时间混为同一指标。

## CTest 与独立项目

当前关闭可选 SQLite 时注册 26 项 CTest，启用时为 27 项（含统一入口自身的正确性测试）；以 `./tests/run_all.sh --list` 的实时清单为准：

- 四套组件演示、快照邮箱。
- 持久化正确性、优化及真实进程恢复。
- BPT 并发压力、缓存控制、SQLite 对照正确性、历史查询及真实 API/CLI。
- 计划内重启、复制正确性、客户端断线恢复、统一入口的失败传播与超时清理。
- Task 3 的 8 个核心/协议/持久化测试，以及真实组交易、替换与恢复两个进程测试。

8 个独立组：两种 SPSC 队列的恢复检查、队列性能对照、快照竞争、成交历史性能、BPT 缓存/并发对照、SQLite 性能对照、CMU 公开测试与 benchmark。

直接运行 CTest 只运行已注册回归，不包含上述独立组：

```sh
ctest --test-dir build/demo-tests --output-on-failure
```

历史文档中的结果不是本次通过的证明；当前结果以本次 `summary.json` 和原始日志为准。不同平台、编译模式和 sanitizer 构建需要分别验证，统一入口不会自动创建全部 sanitizer 构建。
