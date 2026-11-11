# 客户端断线与行情告警测试

`client_reconnect_process` 使用隔离数据与动态 loopback 端口，覆盖交易 CLI、实际 `run_client.sh` 启动器、Gateway、Matching Engine、Publisher 和 32 个品种的行情接收端。

```sh
./tests/build.sh
ctest --test-dir build/demo-tests --output-on-failure -R '^client_reconnect_process$'
```

测试分两部分：

- 协议故障注入：服务端收到订单后断开，验证客户端频繁重试、只提示一次、恢复后不重发结果未知的订单；随后反复接受连接和登录，但不发快照结束标记，验证同一次故障仍在 20 秒左右退出。另检查离线下单被拒、离线 `quit` 可用、无效凭据立即失败。
- 真实进程中断：通过伪终端运行 `run_client.sh`，连续两轮强制终止并重启引擎。每轮验证窗口保留、自动重新登录、Gateway / Publisher / 行情端各只有一条加粗红色警告、持续故障期间不刷屏、恢复通知出现，以及恢复后可正常下单。另核对客户端断线为红色、`ACCEPTED` 关键字为绿色。

成交查询的显示验证在 `trade_history_process` 中：通过 direct / router 两种路径查询四个分片的完整记录，逐笔对照 API 的时间、方向、商品、数量、价格和成交号，检查连续编号、去重、总数及紫色 `TRADE`，并拒绝包含分片、游标等内部信息的输出。

结果保存于 `<build-dir>/client-reconnect-results/reconnect-*/results.json`，同目录保留逐进程日志。`timeout_seconds` 从第二次断线注入计时至客户端退出，允许 19.5—22.5 秒的调度余量；`recovery_seconds` 从杀死引擎至恢复后下单成功，包含测试主动等待的故障时间，不能当作纯重连延迟。该测试不改变运行配置或正式交易数据。

## 首次重连验证结果

2026-11-11，macOS arm64 / AppleClang 16，Release，`build/planned-restart`（SQLite 对照关闭）：

- 16 项已注册回归最终均通过，覆盖 Gateway、Publisher、撮合、行情恢复、持久化、成交历史及 A→B→A 计划重启。首轮 14 项通过；修正两处依赖日志文本的测试断言后，两项复跑通过。
- 新增断线测试耗时 **34.52 秒**。持续收到不完整登录结果时，客户端在 **20.096 秒**退出，期间发起 7 次登录；每次登录最多等待 3 秒，失败后等待 200 ms 重试。
- 两轮真实引擎中断均保留客户端窗口，各端每次故障只输出一条告警；恢复后登录、快照及下单成功。两轮完整故障流程分别为 **4.806 / 5.018 秒**，包含测试主动等待时间。未知结果订单的自动重发次数为 **0**。

原始结果：[results.json](../../build/planned-restart/client-reconnect-results/reconnect-2lbwe7lb/results.json)。完整运行日志在 `/tmp/marketlab-reconnect-regressions.log`，两项复跑日志在 `/tmp/marketlab-reconnect-rerun.log`。日常运行的 `build/bin/` 已通过 `src/scripts/build.sh` 重新编译。

## 日志配色与成交列表验证

2026-11-11，同一构建配置，16 项回归最终全部通过。首轮 15 项通过；计划重启测试仍检查旧的 `HISTORY_COMPLETE` 文案，更新为新的 `COMPLETE` 断言后复跑通过。同时复跑了成交查询，验证最终列间距没有影响逐笔内容。

- `client_reconnect_process`：34.49 秒，两轮真实引擎中断中 Gateway / Publisher / 行情端均每次只显示一条加粗红色告警；客户端断线红色、接单绿色；持续失败 **20.116 秒**后退出，未知订单重发次数为 0。
- `trade_history_process`：3.81 秒，direct / router 各 **1,082 笔**记录，逐笔验证时间、方向、商品、数量、价格和成交号，连续编号与总数一致，全部紫色 `TRADE` 关键字正确，未展示分片、游标或提交信息。
- `planned_restart_process`：18.54 秒，A→B→A、历史连续性及切换后客户端查询均通过。

断线结果：[results.json](../../build/planned-restart/client-reconnect-results/reconnect-evc7pcf_/results.json)。成交列表实测：[direct](../../build/planned-restart/history-process-results/history-process-08rex6ge/direct/client.log) / [router](../../build/planned-restart/history-process-results/history-process-08rex6ge/router/client.log)。完整回归日志 `/tmp/marketlab-console-tests.log`，两项最终复核 `/tmp/marketlab-console-rerun.log`。日常 `build/bin/` 已更新，已有进程需重新启动才能使用新版显示。
