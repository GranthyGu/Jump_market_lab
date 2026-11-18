# Task 3：代码构造与低延迟数据结构

实现日期：2026-11-17；组件目录更新于 2026-11-18。交易语义以 [01](01_peer_updates_and_durable_publication.md)、[03](03_restart_shutdown_recovery_and_trade_records.md)、[04](04_matching_priority.md) 为准；原实测数据见 [06 测试报告](06_validation_results.md)，运行时检查、意外中断处理及恢复限制见 [07 工程补充](07_implementation_guards_and_interruptions.md)，目录整理后的回归结果见 [08](08_component_layout_and_validation.md)。

## 1. 在原引擎上增加的部分

原 `MatchingEngine` 仍是唯一真实订单簿实现。价格树、价档聚合、订单池、档内 FIFO、活动订单索引和用户订单链继续复用。`DurableEngine` 仍管理 Result、Batch、订单日志线程、成交日志线程、检查点及恢复。Shard 1—4 保持普通处理路径；Shard 5—8 在同一引擎外增加 Session 和网络适配。

| 分组 | 文件 | 职责 |
| --- | --- | --- |
| 核心复用接口 | `core/group_execution.cpp`、`core/engine.hpp` | 在原簿上接收主动单、取真实 FIFO 前缀、记录本片成交和最终剩余挂单 |
| 来源与推导 | `spreads/depth.*` | 固定三档、12 条 route、依赖位图、价格限制与共享成员映射；不依赖持久化或组网络编解码 |
| 选择与配对 | `spreads/execution.*` | 价格/直接 FIFO/AB 优先级，固定选择计划，两来源 FIFO 片段切分 |
| 单命令状态 | `spreads/session.*` | 多轮命令、P2P 执行、来源同步与明细并行、一条完整 Result、短路径 |
| Coordinator | `spreads/coordinator/`：`coordinator.*`、`server.*`、`main.cpp` | 分别承载纯状态机、TCP 服务及接入维护、独立进程入口；确认来源/视图/内存完成 |
| 组通信 | `spreads/transport/`：`protocol.*`、`links.*`、`join.*` | 组报文、三档 delta/snapshot 与持久化结果分块编解码、实例和 epoch 校验、握手与接入数据 |
| shard 运行时适配 | `runtime/spread_worker.*` | 将组业务/peer 连接、恢复及就绪状态接入原 shard 服务；持有 Session 和行情候选 |
| Router 与发布 | `cluster/router_spreads.*`、`cluster/router_spreads_publication.*` | 主 Router 内部的关联组模块；有界组请求 FIFO、MEM_DONE 放行、实际参与者持久化汇总、查询和行情可见边界 |
| 行情 | `market_data/spread_market.*`、`shared_data/market_data/`、`publisher/` | 合并真实与 implied 五档、保存不可变候选并应用发布许可；复用原行情传输，支持 V2 编码、快照/增量和显示 |
| 持久化与历史 | `persistence/`、`trade_history/`、`shared_data/orders/protocol.h` | 混合类型成交帧、原样复制、B+ 树记录位置、类型化客户端查询 |
| 维护 | `cluster/router.cpp`、`cluster/run.py` | 整组排空、单片替换、standby 准备、激活、恢复及干净关闭 |

`spreads=enabled` 才启用关联组。默认配置有 A、B、C、SP_AB、SP_AC 五个关联品种，S/A/B/C 四个成员；BF_ABC 使用 S 的调度和持久化，但仍为独立普通订单簿，不执行蝶式腿联动。

### 1.1 按组件整理后的目录

2026-11-18 按代码实际所有者整理；下面省略原有未移动文件，`.*` 表示头文件和源文件。

```text
matching_engine/
├── core/
│   └── group_execution.cpp
├── cluster/
│   ├── router.*
│   ├── router_spreads.*
│   └── router_spreads_publication.*
├── runtime/
│   ├── server.*
│   └── spread_worker.*
├── market_data/
│   ├── snapshot_mailbox.*
│   └── spread_market.*
└── spreads/
    ├── depth.*
    ├── execution.*
    ├── session.*
    ├── transport/
    │   ├── protocol.*
    │   ├── links.*
    │   └── join.*
    └── coordinator/
        ├── coordinator.*
        ├── server.*
        └── main.cpp
```

`Publication` 只由 Router 持有，其实现编入 `market_engine_router`，发布测试单独编译同一源文件；不再作为撮合核心库的一部分。`MarketPublication` 与真实/implied 合并放在 `market_data/spread_market.*`。`Worker` 继续编入运行时库，只把源文件放到与实际用途一致的 `runtime/`。类名和 `marketlab::orders::spreads` 命名空间保持，调用接口及对象所有权不变。

共享的 `member_targets/member_sources` 位于 `depth.hpp`，Session 直接依赖组协议，不再通过 Coordinator 类头文件取得这些映射。三档线格式和结果分块集中在 `transport/protocol.*`；`command_window=128` 也作为组协议共用约束声明，行情代码无需包含 Router 类。`join.*` 保留恢复报告、端点及边界比较；`links.*` 保留连接握手与心跳，真正的接入状态推进仍由 Coordinator 服务和 Worker 完成。

### 1.2 普通 shard 行情与原 Batch 编码的复用

Shard 1—4 在默认持久化模式下，双日志完成后由 [DurableEngine::poll](../../src/matching_engine/persistence/durable_engine.cpp) 更新 `CommittedState`；`DurableEngine::copy_market` 调用 [CommittedState::market](../../src/matching_engine/recovery/state.cpp) 从已提交聚合价档提取买卖各五档。[MatchingEngine::copy_market](../../src/matching_engine/core/engine.cpp) 则是 working 簿的快照接口。组行情沿用该接口复制基础视图，再合并 implied 并保存按命令序号关联的候选，等待 Router 许可后公开。

两类 shard 最终都通过 [SnapshotMailbox](../../src/matching_engine/market_data/snapshot_mailbox.cpp)、已有行情 TCP/共享内存传输交给 Publisher。`spread_market.*` 负责引擎侧公开视图；[publisher.cpp](../../src/publisher/publisher.cpp) 继续负责汇总采样、snapshot/delta 编码选择及对外发送。

磁盘 Batch 编码仍由 `DurableEngine::writer()` 和 [persistence/codec.*](../../src/matching_engine/persistence/codec.hpp) 完成：组命令 `seal_group()` 后进入同一套订单/成交双日志队列，复用订单 delta、普通成交记录和日志帧，扩展类型化腿成交/组合成交。`transport/protocol.cpp` 中的 `send_result()` 只负责把**已经本地提交的 Boundary 和客户端事件**分块返回 Router；事件正文继续复用原协议，只增加组序号、分块和完整性元信息，不重复传输全部订单日志。

这次整理没有新增线程、消息往返、转发队列或每笔持久化步骤；固定容量结构、候选缓存、执行计划与提交顺序保持原实现。

## 2. Coordinator 的三档存储

来源索引是 `2 * slot + side`。Slot 固定为 A/B/C/AB/AC，side 固定为 Bid/Ask；直接通过数组下标访问，无哈希查找。

```cpp
struct DepthSide {
  uint64_t version;           // 8 bytes
  uint64_t quantity[3];      // 24 bytes
  int32_t price[3];          // 12 bytes
  uint8_t count;             // 1 byte
  bool more;                 // 1 byte; with alignment: sizeof == 48
};
using Sources = std::array<DepthSide, 10>;  // 480 bytes
```

分开存 price 和 quantity，避免每个 `{int32_t,uint64_t}` 元素因对齐占 16 字节。价格和数量在同一小对象内，遍历只需最多三步。`count` 确定有效区间；`more` 表示第三档之后仍有真实价档，不能用最后一档数量来代替。版本仅用于本来源连续更新校验，组 epoch/seq/round 在外层报文。

一条 implied route 最多有 `3 + 3 - 1 = 5` 个配对段，使用 `quantity[5] + price[5] + count + more`，`sizeof(Implied)==64`，12 条共 **768 字节**。来源和 route 价量缓存合计 **1248 字节**，不是整个 Coordinator 进程的总内存；控制状态、连接收发缓冲、临时输出和标准库仍另占内存。

来源到 route 的依赖是启动前生成的 10 个 `uint16_t` 位图，共 20 字节。来源变化时 OR 入 dirty mask；本轮来源齐备后，仅重算对应 route。比较价量、count 和 more，内容相同便不发视图替换；变空也必须发送。输出 `Bytes` 保留容量复用。

Coordinator 不存订单号、用户名、成交片段、Result、Batch 或交易日志。真实成交身份只在实际参与的 shard 之间传递。

## 3. 传输怎样保持紧凑

组报文使用显式 little-endian 编码，不直接发送带 padding 的 C++ 结构体。固定头为 **26 字节**：length(4)、kind(1)、flags(1)、epoch(8)、seq(8)、round(4)，单帧不超过 64 KiB。

三档价量正文每档是 price(4)+quantity(8)，满三档 **36 字节**。正文外仍有来源、版本、count/more 等字段，不能把 36 字节当成完整 TCP 帧长度。编码器计算 delta 和 snapshot 的实际长度，选择较小者；换档很多时直接发完整快照，避免“删除三档再新增三档”更贵。两者都校验来源版本和轮次。

`RESULT_DETAILS` 每个必要 FIFO 片段传 orderId(8)、userId(4)、price(4)、quantity(4)，**20 字节**；不重复发送用户名字符串和整个 Order。配对分配项为 **37 字节**，其批次/轮次由外层携带。分块携带连续偏移和 final 标志，接收端检查连续性、数量及来源。

持久化返回沿用原客户端事件字节。无请求关联的通知省略 token/session，只保留 5 字节事件元信息；有请求关联的事件再携带这两个标识。Boundary 只出现在首块，分块号连续，全部收到后才允许发布。网络正常接收时直接消费 `span`；仅因就绪或持久化容量暂不能接纳时复制一条 deferred 报文。

## 4. 选择、FIFO 与多轮

选择器同时观察本地真实最优价和最多两条 implied 路径：价格优先，同价真实订单 FIFO 优先，之后 AB 优先 AC。真实价格树仍使用原实现，不按价差可能很大的价格区间分配数组。

选择结果是固定 **21 个 Slice**：最多 10 个 implied 段，其间最多 11 个直接前缀。相邻直接价档合并为一段，实际成交价保留在原簿取出的 FIFO 片段里。每条 route 还有一个固定前缀数量，无动态 map。

选中的 route 耗尽且 `more=true` 时立即结束本轮选择，刷新来源后再比较。不能先吃掉当前视图里更差的另一条路径，否则新暴露的深层价格可能本应优先。

真实来源执行复用 `take_prefix`，按价格和 FIFO 修改原节点，生成紧凑片段，不复制整个订单簿。`PieceCursor` 对两个来源各保存索引和片段已用数量；每次分配 `min(两侧剩余)`。这使 1/2 与 2/1 的来源切分仍形成正确的三个 1 份成交，不依赖网络回包先后。

一条命令跨所有轮次保留同一个 Result 指针，每个实际参与 shard 最后只 seal 一次 Batch。仅更新 implied 的 shard 不创建 Batch。临时 FIFO/明细/分配 vector 保留容量复用，数量受命令工作区预算约束；Result 自身另有原命令预算。默认各 8 MiB，二者不能误算成合计只有 8 MiB；达到容量上限会拒绝/暂停，不能宣称任意大小扫单都可接受。

## 5. 来源刷新与成交明细并行

发起方保留两份很小的主动订单进度：`incoming_` 是已确认执行的真实数量，`pairing_` 是按选择顺序构造通知时的累计状态。

1. 各来源完成真实前缀修改后即可发送 SOURCE_DONE，并独立返回 EXECUTION_DONE、必要 FIFO 片段。
2. 发起方收齐 EXECUTION_DONE，扣减确认成交数量后报告自己的来源完成。此时不用等待订单身份片段齐备。
3. Coordinator 推导/安装 implied，与 RESULT_DETAILS 传输、FIFO 配对并行。
4. 发起方按本地 Slice 顺序分配跨轮递增 fillNo 和共同时间，发送对应分配。直接成交也占该命令的 fillNo 顺序。
5. 必要分配已进入有序发送队列时 `detailsReady` 成立；同一 peer 连接上的下一轮 EXEC 或 COMMAND_END 排在分配之后，不增加 allocation ACK。
6. ROUND_READY 与 detailsReady 同时成立才继续下一轮；最终剩余挂单若改变来源，还要完成最后的来源同步。

这就是流程图中“明细传输与配对”的作用：补齐每个实际 shard 的成交记录和增量 OrderUpdate。它会写进当前尚未封闭的 Result，最后进入同一个 Batch，不是每收到一个片段就提交一批。

本地短路径要求整条命令结束、没有任何远端执行或先前同步轮次，且所有本片关联来源的三档和 more 都不变。此时 seal Result 后发 LOCAL_COMPLETE，Coordinator 核对本地 Batch 序号返回 MEM_DONE。深层挂撤单可能命中；仅仅没有 implied 成交不足以命中。

## 6. 内存完成与公开可见性

MEM_DONE 释放下一条组命令的执行名额，不等待 fsync。Task 2 的 `Batch.committed` 和本地连续序号仍负责每片提交顺序。Router 只有一个按组 seq 的有界结果 deque，最多 128 条；四个成员只有四个历史游标，**没有四个待发布命令队列**。

一条命令收齐 MEM_DONE 声明的实际参与者持久化结果后，先发发起方事件，再发其余本地事件。因此主动单 Accepted 在该命令成交之前。按设计，后续完整命令的通知可先于仍缺远端持久化结果的旧命令；历史可见边界仍逐片连续，组行情发布仍推进完整组前缀。

行情候选使用启动时分配的 129 个槽位（128 窗口加初始态），按 seq 保存不可变合并五档；同一命令的多轮覆盖同一个候选。旧 Publish 只能取它允许的候选，不能读取更新后的 working book。活动订单捕获在完整内存边界进行，响应等待该组边界公开；历史查询显式携带本片 `visible_trade_end`，**0 表示空前缀**，不会被旧 API 的“0=最新”语义放大。

对外合并价档分别报告 D=真实 quantity、I=impliedQuantity。`numOfOrders` 等于该价位真实订单数，加该价位数量为正的 implied route 数；不是远端真实订单数，也不是配对段数。纯 I 档位有效；D/I 都为零才删除。

## 7. 落盘、价格与历史

保留原 DirectTrade 正文 136 字节；ImpliedLegTrade 正文 148 字节，在真实腿记录上追加公共 `(aggressorOrderId, fillNo)`；SpreadExecution 正文 197 字节，保存本片真实价差订单与两腿引用。新增帧逐条带类型/长度，旧帧仍可恢复。

两真实腿和价差各记录自身真实参与，价差数量只计一次；不创建虚假价差对手订单。成交历史按记录类型决定可查询账户，分页位置仍是 trades.log 中的实际字节位置，复制不重编码已有记录。B+ 树位置值包含长度和校验信息，旧索引可重建。

价差上下限使用两腿已有区间推导：`[A.lower-X.upper, A.upper-X.lower]`。中间计算使用 int64；SP_AB、SP_AC 允许区间内负价和零价，普通品种/BF 仍沿用原价格规则。默认 A=[108,132]、B=[99,121]、C=[94,115]，因此 AB=[-13,33]、AC=[-7,38]。这些值由配置计算，不在撮合中写死。

## 8. 维护与故障边界

`restart 5/6/7/8`：复制期间旧实例继续服务；最终停发整个关联组，等待所有命令 MEM_DONE、参与者落盘结果和查询排空。Coordinator Fence 后旧实例 Drain，standby 追齐五字段 Boundary 和索引。随后先切换业务/peer 接入、重建新实例来源与 implied，收到本轮准备就绪才激活并 resume。

只替换涉及新实例的 peer 连接，健康成员之间继续使用现有连接；Coordinator 和 Router 不重启。激活前失败可重新准备旧实例；发送 activate 后结果不确定则保留 transition，绝不自动同时激活两边。

默认启动器发现关联成员进程退出后，只从原数据目录重启该成员，保留 Router/Coordinator；日志依次为 GROUP_MEMBER_LOST、GROUP_MEMBER_RECOVERED、核对通过后的 GROUP_MEMBER_RESUMED。单成员在完整公开边界崩溃后可核对并重接入。尚未执行的组请求留在原队列，其他四片仍可通过现有 Gateway 连接交易。部分真实腿已改内存、或部分参与者持久化结果缺失时，关联组保持暂停；不增加跨片回滚日志、结果副本或自动分布式修复。

默认启动器在运行前持久化 `group_clean_shutdown=false`，只有整组排空并停止后写 true。这是进程生命周期标记，不进入每笔交易路径。未确认关闭后启动器拒绝用全新的 Router/Coordinator 猜测丢失的发布进度；需检查参与者边界。手工直接启动二进制应保留同样的运维约束。干净关闭后的整组恢复已经测试。

低延迟主要来自固定小数组、依赖位图、有界计划、原 FIFO/池的复用、少量字节传输、来源与明细重叠、MEM_DONE 与 fsync 分离。网络、文件同步、OS 调度、缓冲高水位仍会影响尾延迟；实际边界和数值见测试报告。
