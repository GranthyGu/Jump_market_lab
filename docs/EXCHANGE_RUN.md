# 启动 Exchange 与计划重启

当前运行入口为 `src/scripts/`，命令在仓库根目录执行。业务组件使用 C++20/POSIX；撮合组启动器使用 Python 3 标准库，负责管理 Router、独立 GroupCoordinator 与 A/B 维护，不进入正常交易路径。

Task 3 本次实现与验证见 [代码和数据结构](task3_spreads/05_implementation_and_data_structures.md)及 [测试结果](task3_spreads/06_validation_results.md)。本文后部历史测量保留原上下文。

## 配置和构建

| 配置 | 内容 |
| --- | --- |
| [exchange.conf](../src/config/exchange.conf) | 默认 routing=router；账户、8 shard、1006 品种、Gateway 7001 和 Router 7100 |
| [publisher.conf](../src/config/publisher.conf) | 稳定行情源 9201—9208、UDP 组播、公开快照 9100 |
| [engine_cluster.json](../src/config/engine_cluster.json) | 每片 A/B 订单、复制控制、内部行情端口；管理端口 7110/7111 |

```sh
./src/scripts/build.sh
```

生成 `build/bin/market_gateway`、`market_matching_engine`、`market_engine_router`、`market_group_coordinator`、`market_broadcast`、`gateway_client`。常规构建关闭测试。脚本不需要 `--clean`，不会删除已有交易数据。

当前八分片布局：

| Shard | 品种 | 全局 symbolId | 启动用昨收价 |
| --- | --- | --- | --- |
| 1—4 | 原有 ITEM0001—ITEM1000，每片 250 个，继续按 `1 + (symbolId - 1) % 4` 分配 | 1—1000，保持不变 | 100 |
| 5 | SP_AB、SP_AC、BF_ABC | 1004、1005、1006 | 10、15、5 |
| 6 | A | 1001 | 120 |
| 7 | B | 1002 | 110 |
| 8 | C | 1003 | 105 |

默认 `spreads=enabled`：`SP_AB = A - B`、`SP_AC = A - C` 支持直接成交和跨片 IN/OUT，A/B/C/SP_AB/SP_AC 同属一个串行内存执行组。两价差的允许价格从两腿区间推导，支持零价与负价；默认 AB=[-13,33]、AC=[-7,38]。BF_ABC 保留普通订单簿，不做蝶式腿联动。代码、数据结构和范围见 [Task 3 实现](task3_spreads/05_implementation_and_data_structures.md)。

全局 ID 由 `exchange.conf` 中 `symbol=` 条目的顺序决定，新品种追加在末尾；`publisher.conf` 的名称映射和来源归属必须同步更新。`engine_cluster.json` 中的 A/B 是每片的活动/备用实例槽位，与期货品种 A/B 不同。正常运行有八个活动引擎，维护时临时启动所选分片的备用实例。

## 三个运行终端

```sh
# 终端一：Router + GroupCoordinator + 维护启动器 + 八片活动引擎
./src/scripts/run_matching_engine.sh
# 终端二：Gateway
./src/scripts/run_gateway.sh
# 终端三：Publisher
./src/scripts/run_publisher.sh
```

启动器核对两份业务配置中的商品名称、全局 ID、分片归属及行情来源，并为每片验证两套端口。维护组部署在同一主机，实例间通过 TCP 通信。A 槽位默认订单端口为 7201—7208，B 槽位为 7301—7308；控制端口分别为 7401—7408、7501—7508，内部行情端口分别为 9301—9308、9401—9408。Gateway 只连接 Router 7100。Publisher 始终连接 Router 的 9201—9208，不需要跟随 A/B 槽位切换修改配置。

Coordinator 监听 7120；关联成员 5—8 的控制/peer 共用各自独立组端口，A 槽为 8605—8608，B 槽为 8705—8708。只建立 S–A、S–B、S–C、A–B、A–C 五条 peer 连接。

用户客户端仍连接 Gateway 7001；公开行情仍为 `239.10.10.1:9001`，公开快照查询为 9100。业务、复制、内部行情和组端口分别由两份配置明确列出。

## 在运行界面中重启

终端一会持续显示各进程日志，每 5 秒显示各 shard 的活动槽位和维护阶段。它同时接受以下命令，每行一条：

```text
status
restart 1
restart 1
restart all
restart 2 /absolute/path/to/new/market_matching_engine
```

第一次 `restart 1` 是 A→B，第二次是 B→重新创建的 A。`restart all` 顺序维护八片，始终保持一个维护任务；也可以用 `restart 5` 到 `restart 8` 单独维护新增分片。指定的新版二进制必须与现有磁盘格式、控制协议和业务配置兼容。

也可从另一个终端执行：

```sh
./src/scripts/run_matching_engine.sh --command status
./src/scripts/run_matching_engine.sh --command 'restart 1'
./src/scripts/run_matching_engine.sh --command 'restart all'
```

维护期间有明确日志：`SYNC_BEGIN`、`SYNC_FROZEN`、`BASELINE_COPY`、`SYNC_PROGRESS`、`ROUTER_CUT`、`ENGINE_FENCED`、`ENGINE_ACTIVATED`、`ROUTER_RESUMED`、`RESTART_COMPLETE`。结果包含复制、启动、追赶、排空、最终路由停发时间和队列峰值。关联成员还记录 `GROUP_REPLACEMENT_PREPARED` 及 `group_prepare_ms`；先准备 peer 与 implied，再发送激活。关联组的队列峰值表示整个组的待派发请求。

复制期间冻结源订单簿检查点刷新；源仍撮合、写双日志和发送通知。最终交接时 Gateway 短暂停止读取新请求，已有连接保留。运行中无需手动 kill 旧引擎，也无需重启 Gateway、Router 或 Publisher。

在终端一 Ctrl-C 先排空关联组，再停止整组，成功时显示 `GROUP_SHUTDOWN_DRAINED`；终端二／三 Ctrl-C 只停止各自组件。维护成功后旧引擎的正常退役不会触发整组停止。

## 就绪、日志与数据目录

- `CLUSTER_READY` 表示 Router 已与全部活动引擎握手，关联组也已完成来源、peer 和 implied 初始化；`CONTROL_LISTENING` 会注明复制端口。
- Gateway 显示 `GATEWAY_BACKENDS_READY 1/1` 和 `GATEWAY_READY`；Router 内部管理八个逻辑 shard。
- Publisher 显示 `Published index=...`，快照接口应返回有效数据。

每次启动产生 `build/logs/matching_engine/run-日期时间-随机后缀/`，包含各进程日志、运行配置、稳定行情映射、`coordinator-stats.json` 和 `restart-results.json`。传 `--trace` 可记录 `GROUP_TX/GROUP_RX` 的类型、epoch、seq、round 和字节数；默认不开逐帧跟踪。`GROUP_PAUSED` 记录暂停原因与未完成进度。

默认交易根目录为 `src/config/.marketlab-data/exchange/`。首次启动保留已有 `shard-N` 数据作为 A；新实例使用 `replicas/shard-N/A或B/唯一代号/shard-N/`。`cluster-state.json` 持久化各片当前槽位、二进制和数据根目录。正常重开整组时恢复这个清单。不要删除正在使用的清单或目录；旧代保留用于检查，由用户另行清理。

从原来的四分片配置扩容时，先停止旧服务并重新构建，再按上述三个终端启动。启动器保留 shard 1—4 的活动槽位、数据根目录和二进制路径，追加 shard 5—8 的 A 槽位并原子保存清单。恢复层允许旧品种清单末尾追加仅属于其他 shard 的品种，因此原四片快照、挂单和成交记录可以继续恢复；旧品种的名称、顺序、昨收价、归属及账户顺序必须保持一致。删除已有 shard、本片新增品种或修改已有品种需要单独设计迁移，不在此次兼容范围内。若已有清单指向自定义旧二进制，该路径也必须更新为包含此恢复兼容逻辑的版本。

启用 spreads 后，启动器仅在确认组排空并关闭后写 `group_clean_shutdown=true`；异常退出保留 false，重开时拒绝自动推断跨片尾部。启动器对关联成员故障保留 Router/Coordinator，从原目录重启该成员，只有边界和视图核对成功才输出 `GROUP_MEMBER_RESUMED`；部分内存/落盘尾部会保持组暂停。不要通过删除状态文件强行恢复。

若清单存在未完成的 `transition`，启动器拒绝猜测活动实例。激活确认丢失等不确定情况不能自动切回旧 A；这是与正常计划重启不同的故障处理边界。

## 客户端与自定义参数

```sh
./src/scripts/run_client.sh
./src/scripts/run_market_client.sh
./src/scripts/run_snapshot_client.sh
```

客户端保留原下单/撤单、订单号、登录快照及历史游标规则；历史新增 implied 腿与价差组合记录类型，行情新增含 D/I 的 V2 格式。请一并更新 Gateway、Publisher 和客户端二进制。连接正常时，自带交易 CLI 的请求超时仍为 35 秒，为计划维护保留等待预算。

### 意外断线与自动恢复

`run_client.sh` 检测到断线后保留窗口，输出一次 `DISCONNECTED`，在失败后间隔 200 ms 重试。单次 TCP 连接最多等待 500 ms；重连后的登录和快照最多等待 3 秒，再次失败继续尝试。**整次故障只有 20 秒预算**，从首次检测到断线起计时；反复连上 TCP 或收到 `LoginAccepted` 不会重置期限。只有重新登录并收到完整活动订单快照，才输出 `RECONNECTED` 和 `READY`，恢复交易；20 秒仍未完成则输出 `RECONNECT_TIMEOUT` 并以非零状态退出。

重连期间可输入 `help`、`quit`，新交易命令会提示 `NOT_READY`，不排队发送。断线前已发出的下单、撤单不会自动重发；未收到结果的请求会在断线提示中标为 `outcome unknown, not replayed`，避免重复下单。恢复后可用 `relogin` 查看当前挂单、用 `trades` 核对成交；被中断的历史查询需重新运行。凭据错误仍立即退出。

Gateway、Publisher 与行情接收端持续在后台恢复，每次故障只显示一条**加粗红色警告**：`GATEWAY_UNAVAILABLE` / `PUBLISHER_UNAVAILABLE` / `MARKET_DATA_UNAVAILABLE`，不会逐次重试或逐品种刷屏。交易客户端的 `DISCONNECTED` 和最终超时也用红色显示。Gateway 只有在全部后端恢复后，才输出绿色 `GATEWAY_RESTORED`；部分后端反复连接不会重复告警。行情端保留旧显示，但警告明确提示价格可能过期；所有配置品种恢复同步后，输出一次 `MARKET_DATA_RESTORED` 并刷新行情。Publisher 恢复发布时输出一次 `PUBLISHER_RESTORED`。下一次独立故障会重新告警。`--raw` 模式连续 3 秒没有有效行情报文也会告警。颜色控制码保留在服务日志中，启动脚本转发日志时仍能显示颜色。

自动恢复不能恢复未持久化订单，也不会把断线当作订单已撤销。可复现的故障注入测试见 [客户端断线测试](../tests/client/README.md)。

### 日志颜色与成交查询

交易终端以业务结果为主：绿色 `ACCEPTED` / `FILLED` / `READY`，紫色 `TRADE`，黄色 `PARTIAL` / `CANCELED` / `NOT_READY`，红色 `REJECTED` / `DISCONNECTED` / `ERROR`。青色用于连接、挂单和行情标题，时间及诊断信息用灰色弱化。内部发送、登录握手、快照结束和逐品种状态行不再额外刷屏；`help` 查看完整命令。

implied 腿记录显示公共 execution key，价差组合显示本片价差单及两腿引用，不伪造价差对手方。行情分别显示 `D=真实量` 和紫色 `I=implied量`，纯 implied 档位有效。

输入 `trades` 后，每笔成交显示一行：连续编号、原始成交时间（本地时区，毫秒）、买卖方向、商品、数量、价格和成交号。查询自动读完全部结果，界面不显示分片、分页游标、请求号或提交边界，最后显示 `COMPLETE` 和总笔数；没有成交时显示 `No trades yet.`。同一账户自成交显示 `SELF`，一笔成交只列一次。实时成交带 `[时:分:秒.毫秒] TRADE` 前缀，与带序号和日期的查询记录区分。

```text
TRADE HISTORY — alice
        No.  Time (local)             Side  Symbol           Qty     Price  Trade ID
TRADE     1  2026-11-11 12:30:01.125  BUY   ITEM0001            4       100  281474976710657
TRADE     2  2026-11-11 12:31:02.250  SELL  ITEM0002            2       105  562949953421313
COMPLETE  2 trades
```

列表逐条输出，不等待所有记录在内存中聚合；序号是展示编号，成交以实际时间和成交号识别。中途断开或查询失败会明确提示结果不完整，不会输出成功的 `COMPLETE`。

### 自定义参数

```sh
./src/scripts/run_matching_engine.sh --build-dir build/planned-restart/bin \
  --data-dir /path/to/data --log-dir /path/to/logs --checkpoint-ms 600000
./src/scripts/run_matching_engine.sh --help
```

自定义端口时修改 `engine_cluster.json`，稳定行情端口同时与 `publisher.conf` 对齐；通过 `--cluster-config PATH` 传给启动和 `--command` 调用。`--sync-timeout` 默认 300 秒；`--copy-mib-per-second` 默认 0（不限速），可在维护时限制基线读流量。`--pending-mib` 默认每引擎 64 MiB。

原脚本的 `--shard` 单片启动方式不用于管理组。仅需独立测试某片时，使用 `market_matching_engine --shard ID --config ...`，配套 direct 配置；正常维护使用 `restart ID`。

更多细节见 [设计和路由图](task2_recovery/09_planned_restart_router_design.md)、[实施与测试报告](task2_recovery/10_planned_restart_implementation_and_results.md)、[可复现测试](../tests/restart/README.md)。以下保留先前阶段的历史记录，旧模式、旧端口说明以本节当前流程为准。

## 历史启动脚本实测（改为组播前）

在 macOS / AppleClang 构建产物上，用三个脚本从仓库之外的工作目录启动，验证结果如下：

| 检查 | 结果 |
| --- | --- |
| 32 个撮合进程 | 全部存活，各自提供订单与行情接口 |
| gateway TCP 7001 | 登录成功，快照结束标记正确，32 条后端长连接 |
| 真实撮合 | 接受先于成交，双方通知一致；成交 4，剩余 6 |
| 跨片订单路由 | 分片 1、32、8 均验证通过 |
| publisher TCP 9100 | 完整快照与成交后的剩余量一致 |
| UDP 9001 / 9002 / 9003 | 均收到行情；同一批次 ITEM0001 报文逐字节一致 |
| 重复启动 publisher | 新实例失败，原实例继续工作 |
| 自定义部署 | 分片 7、任意端口、任意来源名称、含空格路径通过 |
| 配置不匹配 | 启动前拒绝，没有启动分片 |
| Ctrl-C 与组件隔离 | 全部试运行实例已停止，无遗留子进程 |
| 默认配置 | 内容保持不变 |

详细结果在本机 `build/launcher-verification/status.json`。该目录中的验证 Python 程序只是本次检查产物，三个启动脚本及服务运行不依赖它。

## 历史交互测试

当前三账户与组播接收联调步骤见 [命令行联调](gateway/GATEWAY_COMMAND_LINE_GUIDE.md)。交易终端把交易所事件时间转换成本地时区显示；内部 trace 保留 loggedAtNs，默认不启用；这些显示调整不改变线上协议。

完整端口用途、谁监听及谁主动连接见 [网络接口清单](NETWORK_INTERFACES.md)。

## 客户端启动脚本

所有 src 下的 shell 脚本统一放在 src/scripts/：

| 脚本 | 用途 |
| --- | --- |
| build.sh | 编译四个日常运行程序 |
| run_matching_engine.sh | 启动并管理撮合分片 |
| run_gateway.sh | 启动 gateway |
| run_publisher.sh | 启动 publisher |
| run_market_client.sh | 加入行情组播，显示与 observe 相同的内容 |
| run_snapshot_client.sh | 通过 publisher TCP 查询某品种最新完整快照 |
| run_client.sh | 显示终端登录界面，认证后进入交易控制台 |

行情接收终端：

```sh
./src/scripts/run_market_client.sh
./src/scripts/run_market_client.sh --interface 192.168.1.20 --count 20
```

多个终端运行同一命令即可加入同一组。支持 --config PATH、--interface IPv4、--count N、--build-dir PATH；Ctrl-C 只停止当前接收进程。该脚本调用 observe，默认进行版本重排、缓存和自动 TCP 恢复，显示已提交五档及同步状态；--raw 显示原始报文。跨机器可用 --host / --port 指定 Publisher 快照地址。

交易终端：

```sh
./src/scripts/run_client.sh
./src/scripts/run_client.sh --host 192.168.1.10 --user alice
```

默认连接 127.0.0.1:7001；可用 --host、--port、--user 和 --build-dir 覆盖。终端先显示登录界面，输入用户名和隐藏的密码，收到 LoginAccepted 和快照终止后显示 READY。脚本不接收密码参数；由 C++ 客户端在终端读取，不写入 shell 历史或启动参数。原 gateway_client 的显式凭据参数继续供自动化使用。

进入 READY 后输入：

```text
buy ITEM0001 100 10
sell ITEM0001 100 4
cancel 实际订单ID
cancel-last
relogin
help
quit
```

用户名密码来自 exchange.conf；默认账户为 alice/alice-pass、bob/bob-pass、carol/carol-pass。退出客户端不撤销已有挂单。密码错误会显示登录拒绝并退出，可重新运行脚本登录。

新脚本和迁移后的脚本都可从任意工作目录使用绝对路径启动；自定义相对配置路径和 --build-dir 按调用时的工作目录解释。

### 客户端脚本实测

迁移后的 src/scripts/build.sh 已重新构建四个日常程序。使用隔离的真实撮合分片、gateway 和 publisher，验证了终端交互登录、密码不回显、买卖双方真实成交、撤单、重登、quit，以及密码输入期间中断后恢复终端回显。三个 run_market_client.sh 接收者的快照与增量输出逐字节一致，数量与真实成交后的剩余量一致。

六个脚本从仓库外执行 --help 均通过；三个服务脚本以含空格的相对配置、程序和日志路径从仓库外启动通过。验证程序和结果保存在 build/verify_client_launchers.py 与 build/client launcher verification/results.json；本次检查实例均已停止。

## TCP 快照查询脚本

Publisher 已启动且来源就绪后执行：

```sh
./src/scripts/run_snapshot_client.sh
./src/scripts/run_snapshot_client.sh --symbol 32
./src/scripts/run_snapshot_client.sh --host 192.168.1.10 --port 9100 --symbol 1
```

默认打开交互查询终端，读取 publisher.conf 的 query_host/query_port；进入后输入 query 1 或直接输入 1 来选择品种，可连续查询不同编号，quit 退出。只有显式指定 --symbol ID 时才单次查询后退出。--host 填 publisher 的实际地址，--port 填其 TCP 快照端口；不是本地接口或组播组地址。也支持 --config PATH 和 --build-dir PATH，相对路径按调用时的工作目录解释。每个查询通过 TCP 取得一份快照，打印 SNAPSHOT、epoch/index、时间戳和五档；交互模式继续等待下一条命令，无需登录或加入组播。

脚本为每条查询调用 market_broadcast query；对应命令行地址覆盖在 src/publisher/app.cpp，实际 TCP 请求由 src/client/market_data.cpp 的 query_snapshot() 完成。服务器返回最近已提交采样批次，查询不触发引擎即时重采样。品种不存在、来源未就绪或网络失败时保留原查询错误输出；交互模式继续等待，单次 --symbol 模式以非零状态退出。

## 当前源码分组

业务入口仍在 `client/`、`gateway/`、`matching_engine/`、`publisher/`；目录按职责拆分，启动脚本和交易协议保持原有入口。

| 目录 | 职责 |
| --- | --- |
| [matching_engine/core/](../src/matching_engine/core/) | 撮合、活动订单索引、后端接口 |
| [matching_engine/runtime/](../src/matching_engine/runtime/) | 引擎进程入口、订单 reactor、服务生命周期 |
| [matching_engine/market_data/](../src/matching_engine/market_data/) | 状态所有者与行情读线程之间的快照邮箱 |
| [matching_engine/persistence/](../src/matching_engine/persistence/)、[recovery/](../src/matching_engine/recovery/) | 双日志提交、检查点、恢复与已提交订单状态 |
| [matching_engine/trade_history/](../src/matching_engine/trade_history/) | 历史索引和查询协调；`index/` 保存 B+ 树、页文件与 CMU 子集 |
| [matching_engine/cluster/](../src/matching_engine/cluster/) | Router、引擎控制与 Python A/B 维护协调器 |
| [shared_data/runtime/](../src/shared_data/runtime/) | 通用 FD/网络 I/O、线程停止、日志、reactor 唤醒与可用性通知 |
| [shared_data/orders/](../src/shared_data/orders/)、[market_data/](../src/shared_data/market_data/) | 订单协议/连接与行情格式/分片视图传输 |

更细的文件职责见 [Matching Engine 模块说明](../src/matching_engine/README.md)。通用运行设施不承载订单或行情业务规则。

## CMake 文件职责与一键编译

`src/CMakeLists.txt` 需要保留：它是 `cmake -S src` 和 `src/scripts/build.sh` 的整项目入口，负责统一 C++20、线程库、共享库依赖、bin/lib 输出路径，并加载 client/gateway/matching_engine/publisher 各自的 CMakeLists.txt。各组件文件定义自己负责的库与可执行程序，也可作为独立构建入口。

测试注册只占总入口的一部分，默认 MARKETLAB_BUILD_TESTS=OFF；仅显式设置 ON 才加载 tests。删除总入口会使一键构建失败。原 src/cmake/MarketLab.cmake 的公共设置已经合并进总入口，src/cmake 目录已删除；组件包含总入口时只取得公共设置，不会编译无关的兄弟组件。

`build/cmake/` 是一键编译生成的缓存、对象文件与构建规则，和已移除的源码辅助目录 `src/cmake/` 含义不同。

```sh
# 全项目一键编译
./src/scripts/build.sh
# 例如，只构建 publisher 组件
cmake -S src/publisher -B build/components/publisher -DCMAKE_BUILD_TYPE=Release
cmake --build build/components/publisher --parallel 4
```

本次验证：整项目一键编译和四个组件的全新独立构建均通过，启用可选测试的 CMake 配置也通过。所有程序提示和日志文字均使用英文；登录界面显示 MarketLab Trading Login、Username、Password、Connecting and logging in...。源码及脚本扫描无中文。

TCP 快照脚本已实测：默认品种、自定义 host/port、空簿、非法参数、未知品种及含空格的相对配置路径通过。结果位于 build/snapshot launcher verification/results.json。

交互查询示例（不需要在启动时选择品种）：

```sh
./src/scripts/run_snapshot_client.sh
```

```text
snapshot> query 1
snapshot> 32
snapshot> query 1000
snapshot> quit
```

输入错误或单次查询失败后可继续输入。--symbol ID 保留用于自动化的一次性查询；--host 和 --port 只选择 Publisher 地址。

行情完整 UDP 快照周期已改为默认 10 秒；客户端恢复参数、状态含义、缓存限制及代码入口见 [网络接口说明第 9 节](NETWORK_INTERFACES.md#9-客户端乱序丢包与自动-tcp-恢复)。
