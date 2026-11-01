# 现场演示测试

四个目录分别对应 Gateway、Publisher、Engine 和全链路。它们是根据当前接口重新实现的测试，不调用旧测试程序、旧 fixture 或旧参考模型。根目录的 `harness.hpp`、`network.hpp` 只为这四套新测试提供结果记录、进程管理和网络驱动。

## 一次编译，现场直接运行

在仓库根目录执行一次：

```sh
./tests/build.sh
```

生成服务程序及四个测试程序到 `build/demo-tests/bin/`。需要 CMake、C++20 编译器、POSIX 网络接口和 Python 3；Python 只使用标准库。普通 `src/scripts/build.sh` 继续只编译日常程序，演示测试使用上面的独立构建目录。

现场运行全部：

```sh
./tests/run_all.sh
```

也可以单独运行任意部分：

```sh
./tests/gateway/run.sh
./tests/publisher/run.sh
./tests/engine/run.sh
./tests/integration/run.sh
```

脚本只运行已编译程序，不会现场配置 CMake、安装依赖或重新编译。可以从其他工作目录通过绝对路径启动脚本。自定义参数：

```sh
./tests/run_all.sh --build-dir build/demo-tests --output build/my-demo-results
./tests/gateway/run.sh --scale 2
./tests/run_all.sh --help
```

`--build-dir` 接受 CMake 构建目录或其中的 `bin/`；相对路径以调用时的工作目录解释。默认 `--scale 1`；2 或 3 只增加请求数量、部分压力持续时间，商品数量始终不超过 2000，分片始终不超过 32。默认面向约两分钟内的完整演示，不是硬实时运行时限。慢机器的耗时可能增加。`--timeout` 默认每套 150 秒，超时返回失败并终止本次测试进程组。

## 测试范围

| 目录 | 正确性重点 | 压力和性能 |
| --- | --- | --- |
| [gateway](gateway/README.md) | 登录、会话替换、快照、权限、TCP 分帧、路由及后端故障 | 五组规模，固定 1000 商品对比 1/4/32 分片；最多 2000 商品；8 用户、16 请求窗口 |
| [publisher](publisher/README.md) | 全品种每秒发布、净增量、空增量、三接收端一致性、周期快照、来源故障 | 同样五组规模；并发 TCP 查询与组播接收同时进行 |
| [engine](engine/README.md) | 独立参考模型核对撮合、撤单、用户快照、五档；并发读取 | 16/1000/2000 商品、新单撤单、64 单扫单和自成交 |
| [integration](integration/README.md) | 真实交易与公开行情一致、双方成交、跨片快照、丢包恢复和重启 | 64 商品/2 分片与 1000 商品/32 分片，交易、采样、组播和查询同时运行 |

网络测试启动真实组件子进程，并使用新生成的隔离配置及动态 loopback 端口，不依赖手工启动服务，不占用默认 7001/9100。服务日志和本次配置保留在结果目录；正常结束、用例异常及脚本超时都有清理路径。动态端口在启动前释放，存在常见的端口被外部进程抢占窗口；发生时结果会明确失败，重新运行即可。

## 结果怎么读

启动时打印 `RESULT_DIR=...`，每次创建新目录，不覆盖上次演示：

```text
build/demo-results/日期时间-随机后缀/
  report.md                 # 全部用例、性能表和计时口径
  summary.json              # 总结果、平台、编译器、退出码和错误详情
  gateway/                  # 单独运行时只有被选择的目录
    console.log             # 现场终端输出的副本
    suite.json              # 每个 case 的结果和指标
    metrics.csv             # 吞吐、mean/p50/p95/p99/max
    samples.csv             # 每个延迟原始样本，单位微秒
    .../                    # 隔离配置、gateway/engine/publisher 日志
```

任何断言失败、缺失响应、异常退出或超时，脚本返回非零；单个用例失败仍会继续其他用例，运行全部时也会继续后续组件。终端同时打印每组压力的校验计数。PASS 表示相应测试中的检查全部通过，不表示所有可能负载或故障都已验证。

分位数从原始样本按 nearest-rank 计算，不平均多个 p99。性能是观测值，没有人为设置吞吐或低延迟门槛。必须结合报告内的 `boundary` 阅读：核心命令耗时、完整 TCP RTT、Publisher 发布一轮耗时和 UDP 到达年龄不是同一指标。并发压力是有限请求窗口的闭环负载，不能当作开放式无限发单容量。

## CTest 与旧报告

新 CMake 只注册 `demo_gateway`、`demo_publisher`、`demo_engine`、`demo_integration` 四项：

```sh
ctest --test-dir build/demo-tests --output-on-failure
```

旧测试源码、旧辅助代码和旧 benchmark 已被本套件替换。`docs/` 中此前的性能与验证文档属于历史记录，不能作为新测试已执行的证明；本次结果只以新的 `report.md` 和 `summary.json` 为准。测试按 Linux POSIX API 编写，本机执行结果会记录实际平台，macOS 结果不等于 Linux 或跨机器验收。
