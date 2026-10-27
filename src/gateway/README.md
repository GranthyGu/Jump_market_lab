# C++ Gateway：接口、编译与测试

本实现以根目录 [README](../../README.md) 和 [protocol.h](../../protocol.h) 为标准，需求原文整理见 [Gateway 组件要求](../../docs/gateway/GATEWAY_COMPONENT_REQUIREMENTS.md)。按当前要求，在 Apple Silicon / macOS 上编译和实测；客户端协议没有修改。下文的性能数字来自本机不同进程之间的真实 TCP 往返。

## 1. 目录与进程

| 位置 | 职责 |
| --- | --- |
| `gateway.cpp` / `gateway.hpp` | 认证、唯一在线会话、请求关联、路由、快照汇总和个人通知 |
| `config.*` / `example.conf` | 账户、静态分片、产品、端点和资源限制 |
| `net/tcp_reactor.*` | 非阻塞 TCP、组帧、部分读写、缓冲限制和超时 |
| `net/engine_connections.hpp` | 引擎持久连接、版本握手、心跳及收发追踪 |
| `protocol/engine_link.*` | 集中定义所有 gateway ↔ engine 命令和事件的内部报文 |
| `protocol/engine_backend.hpp` | 以后替换模拟引擎时实现的 `EngineBackend` 接口 |
| `../tests/gateway_*` | C++ 协议、联调、故障和性能测试及进程管理工具 |
| `../mock_matching_engine/order_*` | 独立撮合分片模拟器、可选入口 router、TCP 接口适配器 |

没有向 `common/` 新增文件或修改其实现。它仍只有 `protocol.hpp/.cpp`（既有行情协议与配置）、`io.hpp/.cpp`（既有传输、共享内存等工具）、`top_five.hpp`（既有前五档算法）。订单路径代码归 `gateway/`，模拟撮合状态归 `mock_matching_engine/`。测试源码统一放在 `src/tests/`；测试日志、CSV、生成配置和二进制均写入忽略的 `build/`，不散落到源码目录。

默认采用 `routing=direct`：一个 gateway 进程与每个分片各保持一条 TCP 连接，全部用户复用这些连接。新单按 symbol 查静态路由，撤单按 orderId 高 16 位定位分片。gateway 不做撮合。

```text
客户端 --TCP--> gateway --持久 TCP--> shard 1
                      └--持久 TCP--> shard 2

shard --既有行情 TCP / 共享内存接口--> market_publisher --UDP--> 行情接收方
```

也实现 `routing=router`：gateway 只连接一个引擎入口进程，入口再连接每个分片并负责分片路由和快照汇总。它满足“先把消息集中发给引擎，再由引擎分片”的部署方式，且与 direct 使用同一套客户端/内部协议与测试。

直连每片减少一跳进程转发与一组 TCP 收发，不让不同分片共用入口连接的发送队列；代价是 gateway 需要知道静态分片配置。router 集中分片决策，但多一个进程和转发路径。本项目允许静态分片、只要求一个 gateway，默认选 direct；第 8 节给出实测对比。直连中的单个分片仍只用一条连接，不能消除该连接自身的 TCP 队头阻塞。

## 2. 登录、连接与通知

README 原文：

> Each user may have only one active logged-in TCP session. A successful new login disconnects the existing session for that user.

> Open orders remain active when a client's TCP connection closes.

1. 客户端先建立到 `gateway_listen` 的 TCP 连接，再在这条连接发送 `LoginRequest`。登录是在已有 TCP 连接上建立已认证会话，不会为该用户另外建立引擎连接。
2. gateway 用配置的用户名/密码认证。失败返回 `RequestRejected(InvalidCredentials)`，保留该用户名已有的有效会话。密码只在客户端 ↔ gateway 上传输，不进入 engine link。
3. 引擎链路全部握手就绪后，成功登录先返回 `LoginAccepted`。同一用户已有另一条有效连接时，关闭旧连接、清理旧请求关联，然后将新连接登记为唯一在线连接。同一 TCP 再登录也会清理旧逻辑会话并重新获取快照。
4. gateway 向所有分片查询该用户的活动订单，转发每条 `ActiveOrderSnapshot(isEnd=0)`；收齐所有分片完成标记后，只向客户端发一条 `isEnd=1`、`OrderState` 全零的终止消息。空快照也发终止消息。所有快照记录回显本次登录的 requestId。
5. 登录后，下单、撤单、响应、订单状态、成交通知都走原来的客户端 TCP 连接。快照可与实时消息交错；不实现 README 明确免除的交错对账或全局原子快照。
6. 普通断线只清理会话，绝不发送撤单。离线订单继续参与撮合；离线实时通知不缓存，重登通过活动订单快照取得当前状态。

`username → userId` 使用哈希表，`userId → 当前 ConnectionId` 使用数组。个人订单/成交通知由引擎报文中的 userId 定位当前有效 TCP 会话；这与面向行情接收方的 UDP broadcast 是两条路径，行情发布不依赖登录用户列表。

每个内部请求分配独立 token，关联 `{userId, ConnectionId, client requestId, 目标分片, deadline}`。不同用户甚至同一用户重复使用 requestId 都不会覆盖内部关联。重新登录或断线时删除旧 token，迟到的旧响应/旧快照丢弃。无请求关联的实时通知使用 token=0、session=0、requestId=0，送给用户此刻的有效会话。

## 3. 客户端及内部网络接口

### 客户端接口

使用根目录 packed C++20 协议，小端，公共头 14 字节。`messageLength` 包含头；先收齐头再按长度组帧，同一 read 可含多条消息。不能把一次 `recv()` 当成一条报文。

| 方向 | 消息 | 总字节 | 行为 |
| --- | --- | ---: | --- |
| 客户端 → gateway | LoginRequest | 78 | 预配置账户认证 |
| 客户端 → gateway | NewOrderRequest | 55 | symbol、side、price、quantity 原样进入对应引擎命令 |
| 客户端 → gateway | CancelRequest | 22 | orderId 原样进入撤单命令 |
| gateway → 客户端 | LoginAccepted | 22 | 回显登录 requestId |
| gateway → 客户端 | OrderUpdate | 80 | 接受、撤单成功回显请求 ID；成交状态更新 ID=0 |
| gateway → 客户端 | Trade | 79 | 每个参与订单单独通知；ID=0；保留引擎事件时间 |
| gateway → 客户端 | ActiveOrderSnapshot | 81 | isEnd=0 为订单，isEnd=1 为终止；回显登录 ID |
| gateway → 客户端 | RequestRejected | 24 | 回显被拒请求 ID |

固定字符串必须在 32 字节内包含 NUL，后面全部补零；请求 ID 非零。长度、类型、字符串等不合法时断开该客户端。价格/数量/价格带和撤单归属由引擎校验；无对应路由的 symbol/orderId 可在 gateway/router 直接返回规定的拒绝原因。

### 内部统一接口

接口定义集中在 [engine_link.hpp](protocol/engine_link.hpp)，客户端报文作为 payload 完整携带，未丢弃订单字段。内部帧头 `WireLinkHeader` 为 26 字节；接收后解码成自然对齐的 `LinkHeader` 再做查表，避免把 packed 字段绑定到标准库引用。

| 字段（线序） | 类型 | 用途 |
| --- | --- | --- |
| length | uint32 | 整帧长度，包括 26 字节头 |
| kind | uint16 | 下表的内部消息种类 |
| token | uint64 | gateway 分配的请求关联值；主动通知为 0 |
| user | uint32 | 已认证用户身份；配置账户从 1 开始编号 |
| session | uint64 | gateway 的单调 ConnectionId；主动通知为 0 |

| kind / 编码 | 方向 | payload / 完成约定 |
| --- | --- | --- |
| Hello=1 / Ready=2 | 连接发起方 / 接受方 | 8 字节版本标记 `MLGW` + `1,0,0,0`；先握手再业务 |
| Ping=3 / Pong=4 | 双向 | 无 payload；检测对端存活 |
| NewOrder=10 | gateway → 引擎 | 完整 NewOrderRequest；返回带相同 token/user/session 的接受或拒绝 |
| Cancel=11 | gateway → 引擎 | 完整 CancelRequest；返回带相同上下文的撤单成功或拒绝 |
| Snapshot=12 | gateway → 引擎 | 14 字节 Header，type=ActiveOrderSnapshot，requestId=登录 ID；无需转发登录密码 |
| Event=20 | 引擎 → gateway | 完整 OrderUpdate / Trade / RequestRejected / 快照记录；主动通知上下文为零 token/session |
| SnapshotEnd=21 | 引擎 → gateway | 完整快照终止记录，isEnd=1，OrderState 全零 |

router 转发保留上下文；每个 shard 返回自己负责的订单；汇总者只在收齐所有完成标记后向上游发一个终止标记。正常最大内部帧为 107 字节。客户端字段、内部 token、OrderId 都保持整数传输，无 JSON/浮点转换。所有 timestamp 为 Unix 纳秒，耗时测量另用单调时钟。

当前模拟器的 ID 约定为 `orderId = (uint64(shardId) << 48) | localSequence`，低 48 位从 1 开始；tradeId 同样加分片前缀。撤单路由依赖该约定，替换真实引擎时须保留编码或同时替换路由目录。README 不要求重启恢复，因此 ID 唯一性针对一次运行中的多分片内存交易系统，未声称跨独立重启持久唯一。

### 接入正式撮合引擎

实现 [EngineBackend](protocol/engine_backend.hpp) 的 `command(context, payload, emit)` 和 `market_view()`，用 `serve_order_engine()` 复用现有 TCP 适配器即可。`MockOrderBook` 是可运行的参考实现。`emit` 在 command 返回前调用，不保留 callback 或 payload span；需要异步引擎时可直接接入同一内部线协议并自行维护事件队列。

引擎必须保留 user/token/session、回显 requestId、先接受再通知成交；每笔 fill 对双方各产生 Trade + OrderUpdate，自成交也两份。价格带使用 int64 中间值计算 `close*90/100` 和 `close*110/100`，正数整数除法满足 truncation；限价单按最优价、同价 FIFO、挂单价成交。完整业务状态属于引擎，gateway 只验证传输上下文并交付消息。

## 4. TCP 稳定性与数据结构

使用系统 socket API：`socket`、`bind`、`listen`、`accept`、非阻塞 `connect`、`recv`、`send`、`getsockopt(SO_ERROR)`、`setsockopt`、`fcntl`、`close`。macOS 通过 `poll` 复用连接，Linux 分支使用 `epoll`；本轮实际运行的是 macOS 分支。

| 机制 | 实现与作用 |
| --- | --- |
| 持久连接 | 引擎连接启动后复用，登录/每次下单不重新握手；`TCP_NODELAY`、`SO_KEEPALIVE`、`FD_CLOEXEC` |
| 部分读写 | 每连接输入/输出 vector 和游标；EAGAIN 留待下次就绪，EINTR 重试，不丢字节或重复执行 |
| 调度 | 单个 reactor 拥有 socket，无每用户线程；每次 ready peer 最多处理 256 KiB，一轮最多 accept 64 个 |
| 内存与查表 | user/session/token/symbol/shard 哈希索引；预留容量；复用 poll/event 数组；已消费缓冲按需压缩 |
| 模拟订单簿 | 有序价格 map + 同价 FIFO list；orderId 哈希表保存 list 迭代器；撤单查找及队列摘除平均 O(1)，删除空价格档 O(log L) |
| 登录快照 | 用户活动订单 ID 集合，只遍历该用户的活动订单，O(该用户活动订单数)；成交/撤单完成即释放活动索引 |
| 慢连接限制 | 输入/输出各自有上限；部分报文/发送无进展超时；在途请求全局及每客户端有上限，超限关闭连接 |
| 心跳与重连 | Hello/Ready 后定期 Ping/Pong；无收包超时关闭；有限间隔重连；错误内部上下文关闭后端连接 |
| 不确定执行结果 | 后端断线关闭现有客户端会话；不自动重发，不把超时伪造成业务拒绝，重登查询权威快照 |

默认每连接输入/输出上限各 4 MiB，客户端上限 4096，全局在途 65536，每用户连接在途 1024；心跳 500 ms、链路失活 5000 ms、请求 5000 ms、部分包/无发送进展 2000 ms、重连间隔 200 ms。检查周期约 50 ms，操作系统调度还会影响实际触发时间。健康已登录客户端没有业务空闲超时，空闲期间不要求客户端发送自定义心跳。

任何一个后端分片失联时，当前实现关闭所有客户端会话，待全部后端恢复再允许登录。这是明确的整体可用性策略，尚未实现只隔离故障分片。请求执行结果不确定时，客户端不能盲目重发。内存状态、明文预配置凭证和可信内部端点符合当前教学范围；内部版本握手不等于认证，未实现 TLS。

## 5. 编译与运行

在仓库根目录执行；仅需 C++20 编译器、CMake ≥ 3.20 和系统线程库，无新增第三方依赖：

```sh
cmake -S src -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j 4
```

根 `src/CMakeLists.txt` 通过 `add_subdirectory(gateway)` 注册目标。`marketlab_order_link` 编译内部协议、配置及 reactor；`marketlab_gateway` 编译会话与路由；`marketlab_mock_orders` 编译入口、TCP 适配器及模拟订单簿并复用既有行情库。它们静态链接为 `build/market_gateway`。同一二进制提供三个角色，每次启动均是独立 OS 进程。CMake 同时构建现有 `market_broadcast` 与测试目标。gateway 的 CMake 从 `../tests/gateway_*.cpp` 读取测试源码，测试仍纳入同一轮 CTest。

默认配置 [example.conf](example.conf) 为 direct。在三个终端分别执行：

```sh
./build/market_gateway shard --config src/gateway/example.conf --shard 1
```

```sh
./build/market_gateway shard --config src/gateway/example.conf --shard 2
```

```sh
./build/market_gateway gateway --config src/gateway/example.conf
```

客户端接 `127.0.0.1:7000`，示例账户为 `alice/alice-pass`、`bob/bob-pass`。测试的生成配置使用 `pass`，与示例配置区分。Ctrl-C / SIGTERM 正常停止；`--stats PATH` 在正常停止时保存 JSON 计数；`--trace` 打印内部收发种类、token、user、session、字节数，不记录密码。性能测试关闭 trace，避免日志 I/O 干扰。

改用入口方案：复制配置到 `build/`，设置 `routing=router`，保持 `engine_endpoint` 指向 router，再额外启动 `market_gateway router --config 配置路径`。两种模式共享 shard 进程及客户端连接方式。

跨机器部署时，用各机器可绑定/可达的数字 IPv4 替换 `gateway_listen`、`shard` 地址；router 模式额外配置 `router_listen` 和 `engine_endpoint`。配置中的所有进程必须对 **账户排列（userId）、symbol 排列（行情 symbolId）、previous close、shardId 与 symbol→shard 归属保持一致**。当前监听和连接共用配置的 shard 地址，因此使用分片机器的实际 IP；不要把 `0.0.0.0` 作为连接目的地址。不需要共享文件或本机共享内存才能走订单 TCP 路径。

## 6. 与已有 broadcast 的联调接口

模拟分片支持可选 `--market-port` 和 `--market-shm`，输出同一订单簿的前五档视图，复用原有 `md::serve_shard` 与 `SharedViewWriter`，没有发明第二套行情协议。

配合现有 `src/market_publisher/example.conf`，可分别这样启动两个订单分片，替代原来的合成行情 mock：

```sh
./build/market_gateway shard --config src/gateway/example.conf --shard 1 --market-port 9201 --market-shm /tmp/marketlab-shard-a.mmap
```

```sh
./build/market_gateway shard --config src/gateway/example.conf --shard 2 --market-port 9202 --market-shm /tmp/marketlab-shard-b.mmap
```

然后另起 gateway、原有 `market_broadcast publish --config src/market_publisher/example.conf` 和行情观察客户端。产品顺序同为 AAPL/MSFT/EMPTY，因此行情 ID 为 1/2/3，分片拥有的 ID 为 `{1,3}` / `{2}`。这些端口不可再同时启动旧 mock 占用。TCP 行情视图按既有 publisher 请求获取；共享视图约每 100 ms 更新，publisher 仍按既有每秒发布策略运行。

测试直接通过既有 TCP source 和 shared-memory source 读取订单视图，检查同价多单聚合及超过 int32 的总量；原有 publisher 测试另做回归。这并非声称已经在跨机器环境完成端到端 UDP 联调。

## 7. 测试原理与功能结果

测试源码现位于 `src/tests/gateway_*`，其余目录恢复到首次完成实现时的组织方式。本节表格保留首次实现的测量记录；测试搬迁后的四项功能回归全部通过，运行 10.62 s，日志为 `build/gateway/relocation-check.log`。

```sh
ctest --test-dir build -R '^gateway_' --output-on-failure -V
ctest --test-dir build --output-on-failure
```

协议测试用本地真实 TCP 检查逐字节拆包、合包、强制小发送缓冲和慢读者；联调用 `posix_spawn` 启动独立 gateway 与两个独立引擎进程，router 模式再增加独立入口进程。客户端逐字段核验实际回包。故障测试使用可编排 TCP 对端，主动制造迟到快照、执行后断链、只回心跳不回应业务、完全静默及错误用户上下文。

每个功能 case 记录单调时钟运行时间，写入 `build/gateway/results/{protocol,integration_direct,integration_router,failures}/*.csv`；进程日志与正常停机计数也保存在对应目录。功能 case 的 runtime 包含整个场景及必要等待，**不是单笔请求 latency**。下表为 Release 实测，长等待用例刻意跨过超时阈值。

全量 CTest **17/17 通过，总运行 82.10 s**，其中原有 broadcast/压力测试 12 项、gateway 测试 5 项。gateway 子集耗时 20.94 s（包含 10.50 s 性能测试）；四个功能入口共 40 个 case 全部通过。UBSan 下相同四个功能入口 4/4 通过，总运行 9.65 s，无未定义行为报告。

| CTest 入口 | 场景数 | Release 运行时间 | 结果 |
| --- | ---: | ---: | --- |
| gateway_protocol | 5 | 0.79 s | PASS |
| gateway_integration_direct | 15 | 3.85 s | PASS |
| gateway_integration_router | 15 | 3.19 s | PASS |
| gateway_failures | 5 | 2.61 s | PASS |
| gateway_performance | 6 轮 / 12 组场景 | 10.50 s | PASS |

协议场景：

| case / 核验内容 | runtime ms | 结果 |
| --- | ---: | --- |
| wire_layout_and_roundtrip：字节序、尺寸、完整字段、uint64 边界 | 0.020 | PASS |
| invalid_frames_and_strings：截断、零请求 ID、未知类型、无 NUL/脏 padding | 0.079 | PASS |
| tcp_fragmentation_and_coalescing：逐字节拆包和多消息合包 | 80.466 | PASS |
| partial_writes_and_slow_reader：小发送缓冲、延迟读取，核验 8000 条报文的全部字节 | 178.187 | PASS |
| buffer_limits_and_partial_deadline：缓冲溢出及不完整包超时清理 | 90.151 | PASS |

两种拓扑使用相同断言：

| case / 核验内容 | direct ms | router ms | 结果 |
| --- | ---: | ---: | --- |
| authentication_and_empty_snapshot：认证、空终止、未登录禁止交易 | 1.822 | 2.472 | PASS |
| bad_credentials_preserve_online_session：错密码不踢旧登录 | 2.526 | 3.630 | PASS |
| successful_relogin_invalidates_old_connection：新登录踢旧连接并保留订单 | 3.205 | 4.067 | PASS |
| same_tcp_reauthentication_restarts_snapshot：同 TCP 重认证重新快照 | 2.779 | 3.523 | PASS |
| invalid_orders_and_price_boundaries：非法 symbol/方向/价格/数量、上下限、close=101 截断、int32 最大数量 | 9.671 | 9.513 | PASS |
| fifo_partial_fills_and_cancel_validation：同价 FIFO、部分成交快照、四类撤单失败 | 6.148 | 4.464 | PASS |
| price_priority_and_resting_price：最优价优先、挂单价成交 | 1.672 | 2.232 | PASS |
| self_trade_two_participant_notifications：接受先于 fill、自成交双份 Trade/Update | 1.052 | 1.336 | PASS |
| cross_shard_ids_and_request_id_collisions：跨片唯一 ID、用户请求 ID 碰撞、全分片快照 | 2.965 | 4.244 | PASS |
| disconnected_orders_remain_live：离线继续成交、重登恢复剩余量 | 1.648 | 2.438 | PASS |
| coalesced_login_order_and_snapshot_interleaving：登录与下单合包、快照终止和响应关联 | 0.584 | 0.927 | PASS |
| idle_persistent_links_and_malformed_client_isolation：跨心跳周期空闲、坏包隔离 | 2648.330 | 2631.400 | PASS |
| slow_client_does_not_block_other_accounts：过载账户不阻塞健康用户 | 10.031 | 16.738 | PASS |
| engine_link_failure_and_reconnection：断链清会话、自动重连、订单快照恢复 | 87.408 | 195.017 | PASS |
| existing_market_data_tcp_and_shared_memory_adapter：既有行情接口、两笔 int32 最大数量聚合 | 133.102 | 149.213 | PASS |

独立故障注入：

| case / 核验内容 | runtime ms | 结果 |
| --- | ---: | --- |
| stale_snapshot_generation_and_live_interleaving：旧登录迟到快照丢弃，实时通知可穿插 | 50.650 | PASS |
| unknown_outcome_no_replay_and_snapshot_recovery：引擎已执行但回包前断链，不重发，重登取得订单 | 95.602 | PASS |
| request_timeout_without_false_rejection：正常心跳但业务不响应，断开客户端且不伪造拒绝 | 815.743 | PASS |
| heartbeat_detects_silent_tcp_peer：TCP 未主动关闭但引擎完全静默，心跳超时并恢复 | 990.378 | PASS |
| wrong_user_engine_response_fails_closed：内部响应身份错配，关闭后端及用户会话 | 56.590 | PASS |

测试日志：`build/gateway/results/release-suite.log`；UBSan 日志副本：`build/gateway/results/ubsan-results.log`。

额外以 Debug + UBSan（`-fsanitize=undefined -fno-sanitize-recover=all`）运行全部四个 correctness 测试入口。AddressSanitizer 在本机运行库初始化阶段报 `sanitizer_malloc_mac.inc:189`，未能完成测试，因此不将其列为通过。没有据此宣称完成内存泄漏验证。

## 8. 性能方法与结果

测试环境：2026-10-27，macOS / Darwin arm64，AppleClang 16.0.0（clang-1600.0.26.6），CMake Release（优化构建），本机 loopback IPv4 TCP。所有被测角色为独立进程，共用同一机器；无绑定 CPU 或排除系统背景负载。

| 场景 | 样本与计时范围 |
| --- | --- |
| 串行 new/cancel | 同一持久客户端连接；先预热 100 对下单/撤单，排除其 200 请求；正式测 2000 对，共 4000 个独立请求；轮流访问两个分片 |
| 16 客户端 pipeline | 16 个已登录活跃客户端，各自最多 64 个在途请求，另有 64 个已登录空闲客户端；16 批 × 64 下单及对应撤单 × 16 客户端，共 32768 请求；barrier 后开始计时，排除登录 |
| 重复与校验 | direct/router 交错运行，各独立启动三轮；共 220608 个计时请求；核验所有响应 ID/状态、订单所属分片、完成数和分片连接计数 |

单请求 latency 从客户端提交该请求前开始，到客户端收到并核验对应完整响应结束，包含客户端、内核、网关、模拟引擎与排队；pipeline 包含批量在途等待。耗时使用 `steady_clock`，p50/p95/p99 使用 nearest-rank，单位微秒；吞吐为完成请求数 / 场景墙钟时间，一次下单和一次撤单算两个请求。每个正常基准轮次，每个分片只接受一条引擎连接，验证没有按订单反复建连。

串行场景：每行 4000 请求，运行时间单位 ms，延迟单位 µs。

| 拓扑 | 轮次 | 运行 ms | 请求/s | p50 µs | p95 µs | p99 µs | max µs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| direct | 0 | 722.632 | 5535.32 | 166.292 | 234.875 | 297.333 | 5798.42 |
| direct | 1 | 727.521 | 5498.12 | 166.042 | 233.209 | 363.917 | 6147.33 |
| direct | 2 | 677.393 | 5904.99 | 164.541 | 216.292 | 256.500 | 5807.38 |
| router | 0 | 1059.670 | 3774.75 | 249.959 | 345.083 | 417.792 | 5998.46 |
| router | 1 | 1038.950 | 3850.03 | 248.542 | 321.084 | 381.334 | 5859.46 |
| router | 2 | 1036.940 | 3857.49 | 247.291 | 319.333 | 387.375 | 7115.58 |

并发 pipeline 场景：每行 32768 请求，16 活跃 + 64 空闲登录连接，每活跃连接最多 64 在途；延迟包含排队，所以明显高于串行。

| 拓扑 | 轮次 | 运行 ms | 请求/s | p50 µs | p95 µs | p99 µs | max µs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| direct | 0 | 576.499 | 56839.6 | 14370.2 | 20510.2 | 22777.2 | 25482.0 |
| direct | 1 | 571.377 | 57349.2 | 14713.2 | 21325.7 | 24960.1 | 27198.4 |
| direct | 2 | 580.934 | 56405.7 | 13839.1 | 21331.5 | 25604.4 | 30891.4 |
| router | 0 | 639.062 | 51275.1 | 15937.8 | 21803.1 | 23541.7 | 29192.1 |
| router | 1 | 653.057 | 50176.3 | 16696.0 | 23105.4 | 25134.2 | 28174.2 |
| router | 2 | 666.688 | 49150.4 | 17803.6 | 23258.9 | 24226.1 | 26718.7 |

以三轮各项指标的中位数比较（不是合并样本后的全局分位数），direct 串行吞吐为 5535.32 请求/s，router 为 3850.03，请求吞吐提高约 **43.8%**；串行 p50 从 248.542 降至 166.042 µs，降低约 **33.2%**。并发吞吐从 50176.3 增至 56839.6 请求/s，提高约 **13.3%**，但并发 p99 并未全面改善：direct 三轮 p99 中位数 24960.1 µs，高于 router 的 24226.1 µs。此次结果支持当前负载下直连的吞吐和典型延迟优势，不能据此承诺尾延迟全面更好。

原始数据为 `build/gateway/results/performance/latency.csv`，每轮子目录含配置、日志和计数。结果衡量当前实现和模拟引擎在该负载下的表现；不等同于纯网关服务时间、撮合吞吐上限或跨机器网络性能，也不代表可保证固定延迟。README 没有硬吞吐/延迟指标；真实远端部署的网络 RTT、CPU 和订单分布需要另外测量。
