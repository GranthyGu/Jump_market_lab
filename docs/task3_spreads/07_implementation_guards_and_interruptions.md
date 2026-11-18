# Task 3：工程补充、运行时检查与意外中断处理

日期：2026-11-17。行为按代码基线 `aca0517` 核对，文件位置已随 2026-11-18 的组件整理更新。本文说明正常交易流程之外的工程处理，以及这些处理的实际保证范围。架构与交易语义仍以 [01](01_peer_updates_and_durable_publication.md)、[03](03_restart_shutdown_recovery_and_trade_records.md)、[04](04_matching_priority.md) 为准；模块与数据结构见 [05](05_implementation_and_data_structures.md)，原验证结果和耗时见 [06](06_validation_results.md)。

**当前能够自动恢复的是：控制进程仍保留进度、各片能确认回到完整公开边界的成员故障。故障若留下未完成的跨片执行，或无法确认齐全的参与者持久化结果，代码会暂停整个关联组的新执行，保留不确定状态，不自动回滚、补造回执或重放命令。** 后一种处理是防止继续扩大不一致，并不代表已经修复了这笔交易。正常异步落盘期间的短暂结果不齐不属于故障，仍沿用 MEM_DONE 后继续内存执行的设计。

## 1. 哪些来自设计，哪些是实现时补充的工作

“额外工作”主要是把设计的前置条件变成运行时检查，补齐启动器、连接、查询与故障测试，并没有另起一套撮合架构。

| 类别 | 已有内容或实现补充 |
| --- | --- |
| 原设计明确要求 | 单条组内存命令、实际参与者各一个 Batch、MEM_DONE 与落盘分离、完整结果后公开、恢复前核对边界、部分跨片尾部保持暂停 |
| 原设计中的优化 | 三档固定数组、脏 route 位图、delta/snapshot 按大小选择、本地短路径、来源刷新与明细配对并行；这些不是实现阶段另加的架构 |
| 复用 Task 1/2 | 原订单簿、价格/FIFO、订单索引和池；双日志提交、快照及连续前缀恢复；原基线复制、结果追赶、激活与退役 |
| Task 3 的具体检查 | 消息身份、epoch/join/seq/round、来源所有权、分块顺序、数量守恒、参与者集合、本地 Batch 序号与公开边界逐项核对 |
| 接入与调度补充 | 同一实例的业务/控制双通道报告比对；READY 跨连接先后差异的有限暂存；有界命令窗口、内存预算与进度超时 |
| 可见性适配 | 显式历史边界 0、查询快照等待公开许可、不可变行情候选、Direct/Leg/Spread 混合历史及复制适配 |
| 启动维护补充 | 保留 Router/Coordinator 的单成员重启、关联组准备完成后激活、干净关闭标记、故障日志与独立运行目录 |
| 验证工作 | 状态机非法输入、参考模型对照、延迟明细、分块压力、真实进程中断、替换失败回退、旧路径回归及 UBSan |

Router 的关联组功能现在位于 [cluster/router_spreads.cpp](../../src/matching_engine/cluster/router_spreads.cpp)，使用主 Router 的进程、reactor 和业务连接；Coordinator 的状态机和服务集中在 [spreads/coordinator/](../../src/matching_engine/spreads/coordinator/)。没有增加四套 shard 待发布队列，也没有重写 MatchingEngine。

## 2. 先区分中断发生在哪个阶段

同样是“连接断了”，发生位置不同，能恢复的程度也不同。

| 阶段 | 当前事实 | 能否据此认为交易完整 |
| --- | --- | --- |
| Router 已排队，尚未发送 RUN | 请求只在 Router 内存中，尚未执行真实订单修改 | 不能；仍是未执行请求 |
| 已发送 RUN，Session 活跃 | 可能已经取走某条真实腿，但明细、分配或其他腿尚未完成 | 不能；可能存在部分内存修改 |
| Coordinator 已确认 MEM_DONE | 整条命令及各片 Batch 封闭完成，下一条内存命令可以开始 | 不能；参与者的日志可能尚未全部同步 |
| Router 收齐该命令全部实际参与者的持久化结果 | 校验分块、参与者及 localBatchSeq 后可以发送该命令的客户端事件 | 可确认组内执行结果完整；客户端是否实际收到仍是另一件事 |
| 连续公开前缀推进 | 行情发布许可推进，每片历史按其参与命令推进可见游标 | 可以按这些边界提供公开视图；不等于客户端确认接收 |

因此，“TCP 已连接”“本片 fsync 成功”“MEM_DONE 已到”都不能单独作为恢复放行依据。更晚的完整命令可以先发自身通知，但每片历史和组行情仍按各自的连续边界推进，这是原发布设计的具体实现。

## 3. 运行时补了哪些检查

### 3.1 连接身份与消息归属

- 握手核对协议版本、连接角色、逻辑 shard、实例代次和业务配置 fingerprint。peer 必须属于本次目录中的实例及允许的连接关系；standby/fenced 实例拒绝真实执行。
- 正常命令核对 epoch、seq、round 与当前 Session；恢复消息使用单独的 joinId。不能把旧命令或旧实例的报文计入当前命令。
- Coordinator 检查来源是否属于发送者、是否本轮参与、是否重复完成、视图 ACK 是否来自目标成员、结束时 localBatchSeq 是否连续。
- 帧检查长度、类型、标志、正文是否完整消费；单帧上限 64 KiB。握手不能重复，心跳不能夹带命令字段。
- 显式维护取消一个 join 后，Coordinator 会忽略属于已退役 join 的接入回包。这是处理不同连接之间的迟到回复；普通交易报文没有获得“重复就忽略”的通用重试语义。

入口见 [protocol.cpp](../../src/matching_engine/spreads/transport/protocol.cpp)、[links.cpp](../../src/matching_engine/spreads/transport/links.cpp)、[spread_worker.cpp](../../src/matching_engine/runtime/spread_worker.cpp) 和 [Coordinator 状态机](../../src/matching_engine/spreads/coordinator/coordinator.cpp)。fingerprint 用来发现配置不一致，不是加密认证或网络安全签名。

### 3.2 来源、执行前缀与成交明细

| 检查对象 | 具体检查与作用 |
| --- | --- |
| 三档更新 | 来源归属、版本连续、delta 基准版本、重复价格、删除不存在价位、正数量、档数与 more 合法性；在临时摘要上解析检查后更新缓存 |
| implied 替换 | route 和目标所有者、完整 route 集合、档数、价格顺序、数量及空视图；避免把另一片视图装进本片 |
| EXECUTE_PREFIX | 主动订单身份及上下文、来源/方向、重复前缀、执行总量、当前已广告三档是否足够；下一轮前必须完成上一轮有序分配 |
| RESULT_DETAILS | 分块 offset 连续、来源属于选中 route、数量与价格范围、FIFO/价格次序、final 时总量完整；不按网络回包顺序分配 fillNo |
| 分配报文 | 发送者、fillNo 顺序、当前 FIFO 片段剩余量、对应订单与方向、成交价格关系；final 时不能留下未配对的真实前缀 |
| 数值边界 | implied 价格用较宽整数计算后检查 int32 范围；数量求和、来源 version、round、fillNo、命令序号等在对应入口检查溢出 |

原设计允许“视图先更新、明细稍后到”。实现另外检查配对后的剩余数量与已确认真实执行数量一致；只有 `ROUND_READY` 和 `detailsReady` 同时满足才继续。`COMMAND_END` 不能越过尚未完成的分配。

代码见 [depth.cpp](../../src/matching_engine/spreads/depth.cpp)、[来源与组协议编解码](../../src/matching_engine/spreads/transport/protocol.cpp)、[execution.cpp](../../src/matching_engine/spreads/execution.cpp)、[session.cpp](../../src/matching_engine/spreads/session.cpp)。这些检查用于发现状态不一致；如果异常发生前已经修改真实簿，抛异常不会自动撤销之前的修改。

### 3.3 持久化结果不能靠“收到一包”判定完成

Router 对每个命令记录实际参与者和各自 localBatchSeq。持久化结果必须满足：

1. epoch/seq 在当前未完成窗口内，返回者是实际参与者；view-only 成员不能提交成交 Batch。
2. 同一 shard 的 Batch 序号连续；同一结果分块号连续，不接受重复 final、跳块或交叉拼接两个结果。
3. 事件类型、长度和请求关联元信息合法，全部正文解析完毕。
4. MEM_DONE 指定的 localBatchSeq 与持久化 Boundary.seq 相同；所有实际参与者完整后才发该命令的通知。

这沿用 Task 2 每片 Batch 的提交顺序。实现用一份命令记录和四个历史游标，不再维护四份待发布命令队列。代码见 [router_spreads_publication.cpp](../../src/matching_engine/cluster/router_spreads_publication.cpp)。

## 4. 意外中断的处理矩阵

| 中断或异常 | 当前处理 | 恢复范围与限制 |
| --- | --- | --- |
| 完整公开边界上的单个组成员退出 | 暂停组内 RUN；启动器在原数据目录、原活动槽位重启该成员；Router/Coordinator 保持运行 | 通过第 5 节所有核对后自动放行；其余健康成员无需重启 |
| 必需 peer、业务或控制 TCP 断开 | 连接回调使组暂停，停止新 RUN；连接恢复后重新核对接入条件 | 重连本身不等于恢复。无未完成状态且未进入失败状态时才可能自动重接入 |
| 真实腿已修改，但明细、分配或结束流程未完成 | 保留 unresolved Session；Coordinator 不确认组完成；Router 不公开该命令的片面结果 | 当前没有自动补齐或回滚协议；重启故障片后仍保持暂停 |
| MEM_DONE 已到，但部分参与者未落盘，或持久化回复缺失 | Router 保留未完成结果记录，不能通过 settled 检查；无进展时暂停 | 不会把已执行命令放回未执行队列，也不凭某片文件单独推断整组成功；没有结果重传/补查恢复协议 |
| 明细、落盘结果或 join 长期无进展 | 执行/发布/join 进度超时，记录原因并进入失败状态 | 心跳正常也不能替代命令进展；协议错误和进度失败不会仅因 socket 重连被自动清除 |
| Gateway 与 Router 的业务上游断开 | 清除该连接的 pending 关联、未发送组队列及缓存查询结果 | 已派发命令可能继续完成；不能自动撤销，也不承诺客户端重发只执行一次 |
| Router 或 Coordinator 进程退出 | 当前启动器检测到控制进程退出后进入整体关闭流程；各片失去必要连接后暂停 | 未实现控制进程状态复制或自动选主；默认编排下可能影响整个受管集群，不能保证 shard 1—4 此时一直运行 |
| 日志写入/同步异常 | 沿用 DurableEngine 的失败传播，不将失败 Batch 当作提交完成 | 本片有效前缀恢复不等于跨片修复；没有磁盘永久损坏自动恢复能力 |
| 命令/结果/连接缓冲达到预算 | 能在准入处等待的先停止派发或暂存；无法容纳或校验失败时按相应错误路径暂停/断链 | 不是所有超限都返回一个普通业务 Reject；执行中超限可能已经发生部分修改 |
| 可控正常关闭 | 先停发并排空命令、结果和查询，执行 Fence/Drain，再停止进程 | 只有排空确认成功才保存干净关闭标记 |
| supervisor 被 SIGKILL、掉电或未能排空 | 无法依赖正常退出处理；未确认关闭状态阻止默认启动器直接重新开放整组 | 不保证 SIGKILL 后子进程已全部退出，也没有跨片冷启动自动修复；需保留并检查现场 |

这里“队列保留”的条件非常具体：**只有组成员故障、原 Gateway–Router 连接还在时，尚未执行请求才留在 Router 队列。** `Router::closed(front)` 会清空队列。它是有界内存队列，没有独立落盘，也没有对客户端提供跨断线的 exactly-once 承诺。

超时只表示无法在时限内确认进展，不等于订单未执行。即使各片文件事实上已经完整，只要丢失必要回执且控制状态无法确认，也可能继续暂停；当前没有从磁盘重建并补发这些在途组结果的流程。

组成员业务连接丢失时，Router 特意保留上游连接，因而无关 shard 1—4 可以继续交易；这一行为有真实进程测试。不能把它扩展成“任何控制进程故障都不影响 1—4”。实现入口为 [Router 连接处理](../../src/matching_engine/cluster/router.cpp)、[Worker](../../src/matching_engine/runtime/spread_worker.cpp)、[启动器](../../src/matching_engine/cluster/run.py)。

## 5. 自动重接入为什么不能只看“重启成功”

以 A 退出为例，启动器重新启动进程后只记录 `GROUP_MEMBER_RECOVERED`，并将其设为 `rejoining`；此时还没有获得交易许可。实际检查为：

1. **本地状态完整。** Worker 等待 DurableEngine idle，且无活跃 Session 或 deferred 命令，再报告 RECOVERED。报告包含实例代次、Boundary 和索引覆盖位置。
2. **同一份状态从两条连接确认。** Router 业务连接与 Coordinator 控制连接分别收到报告。`same_recovery` 比对实例、索引位置及 Boundary 五项：`seq / next_order / next_trade / trade_end / trade_count`；索引位置必须等于 trade_end。
3. **与仍存活的控制状态核对。** Router 必须没有未完成发布或待确认 capture；重新接入各片的 Boundary 必须等于 Router 保存的公开边界。Coordinator 还核对本地 Batch 序号与其记住的内存完成进度。各片批次号不要求彼此相等。
4. **只接受当前实例的 peer。** 本次 join 分配新的接入轮次，建立所需连接。A 需要 S/B/C，B/C 各需要 S/A；健康实例之间的有效连接可以保留。
5. **确认真实来源及派生视图。** 更换实例上报完整真实三档，核对与已确认边界的缓存价量和 more 一致；为新实例安装完整 implied，空视图也安装。恢复来源和视图不创建新的成交 Batch。
6. **整体就绪后放行。** 收齐 peer/视图确认，Coordinator 才发送 READY；Router 再核对双方选择的实例与完成序号。启动器观察 Router 的 READY 后才记录 `GROUP_MEMBER_RESUMED`。

首次干净启动没有存活的旧 Router/Coordinator 可作比较，依赖本地恢复和启动器的生命周期检查；它不能凭新生成的 epoch 重建丢失的在途命令事实。

网络还有一个正常竞争：Router 收到 READY 后发出的 RUN，可能比另一个连接上的 Worker READY 更早到达。Worker 对尚未就绪或暂缺持久化容量的工作只保留 **一条** owned deferred 报文，条件满足后处理；重复占用该槽会报错，不额外堆积执行队列。

代码见 [join.cpp](../../src/matching_engine/spreads/transport/join.cpp)、[Coordinator 接入服务](../../src/matching_engine/spreads/coordinator/server.cpp)、[RouterGroup](../../src/matching_engine/cluster/router_spreads.cpp)。自动重启也不是无限重试：启动失败或重接入期间进程再次退出会进入 failed；边界不满足时不能靠反复重启把它变成 READY。

## 6. 计划替换和整组关闭的额外保护

### 6.1 激活前后采用不同失败规则

沿用 Task 2 的复制和最终 Boundary 核对，Task 3 在最终窗口增加候选业务连接、peer、真实来源及 implied 准备。候选达到 `prepared` 后才发送 `activate`，随后恢复组派发。

- **activate 发出之前失败：** 在能核实旧实例状态的条件下，撤回候选路由、恢复旧端点和完整接入，解除旧实例维护状态。恢复成功后记录 `RESTART_ABORTED_SOURCE_ACTIVE`；恢复本身失败则记录 `RESTART_NEEDS_ATTENTION`，不会谎报完成。
- **activate 已发出、结果不确定：** 代码在调用 activate 前就设置 `activated = True`。即使只丢了回复，也不自动重新激活旧实例；保留两份目录和 transition 记录，避免两个实例同时写同一逻辑 shard。

前一种失败有不可达候选 peer 的真实测试；后一种是沿用并保留的激活不确定性处理规则，目前没有 Task 3 专门的“丢失 activate ACK”故障注入测试。

### 6.2 生命周期文件与正常退出

启动器沿用 `cluster.lock` 排他锁和 `cluster-state.json` 原子写入：临时文件写完并 fsync，rename 替换，再 fsync 父目录。已有未结束 transition 会阻止直接启动。Task 3 增加 `group_clean_shutdown`：

1. 启动任何交易进程前先保存 `false`。
2. 正常退出先执行组 cut/barrier，等待完整排空，再退出进程。
3. 确认排空成功后才保存 `true`；失败打印 `GROUP_SHUTDOWN_UNRESOLVED` 并保持未确认状态。
4. 下次启动看到 `false` 直接拒绝开放，提示检查参与者边界。

这只在启动/关闭时写入，不给每条交易增加一次文件同步。它是生命周期门禁，不是事务日志：旧状态文件缺少该字段时当前代码默认允许，绕过启动器直接运行二进制也不会自动获得这层保护。不能把手工改成 true 当成修复跨片尾部的办法。

SIGINT/SIGTERM 设置停止标志并进入关闭流程；SIGKILL 无法执行这套逻辑。Worker 退出时如果还有未结束组工作，会打印 `GROUP_UNRESOLVED_SHUTDOWN`；原 `book.drain()` 处理已提交给持久化层的工作，不会把未完成 Session 强行补成完整跨片命令。

## 7. 查询和行情侧补齐的边界处理

- **历史边界 0 是空。** 原普通历史接口存在“未指定边界则取最新”的语义；组公开查询使用显式边界，0 不能被解释为最新。即使本片已经有落盘成交，组尚未公开时也不能泄露。
- **订单快照先捕获、后放行。** 在完整内存边界捕获订单快照，并记录对应组序号；如果公开前缀尚未到达，Router 缓存结果，等允许后发送。返回 capture ACK 的成员、token、seq 都要匹配。
- **行情许可只选择已有候选。** Worker 在对应命令边界捕获候选；较晚收到 Publish 时不能重新读取已经进入下一条命令的 working book。同一命令多轮更新其候选，最终对外选择已许可的版本。
- **暂停不等于回滚已公开事实。** 已形成的不可变候选和迟到的合法结果仍需按其序号处理；不能用它们推断组已经可以接受新命令。恢复的初始行情许可还要等 Worker READY。
- **历史与复制保持原事实。** 三种成交记录保留类型、原 ID、时间和文件边界进行读取/复制；索引是可重建派生数据，恢复不重新撮合，也不生成历史成交的新通知。

入口见 [DurableEngine](../../src/matching_engine/persistence/durable_engine.cpp)、[Router 发布控制](../../src/matching_engine/cluster/router_spreads_publication.cpp)、[行情候选](../../src/matching_engine/market_data/spread_market.cpp)、[Router 查询处理](../../src/matching_engine/cluster/router.cpp)。

## 8. 这些检查的存储与延迟代价

| 位置 | 当前约束 | 对正常路径的影响 |
| --- | --- | --- |
| 来源和 route 校验 | 10 个三档来源、12 条 route，固定数组和位图 | 只扫描小集合；校验摘要用小型临时副本，不复制订单簿 |
| 组头与状态机 | 26 字节头；整数、标志、序号和所有权检查 | 在已有报文处理时完成，不增加一次网络 ACK |
| Router 请求 FIFO | 同时限制 `max_pending` 和 `max_buffer_bytes`；默认 65536 条、4 MiB 正文 | 暂停期间也有上限；队列节点等对象开销另计，不代表总内存只有 4 MiB |
| 未完成发布 | 最多 128 条命令；结果缓冲 capacity 总预算为 `max(64 MiB, max_buffer_bytes)` | 窗口满时停止新 RUN；没有无限囤积结果 |
| Session 临时工作区和 Result | 默认各 8 MiB，分别核算；保留的 vector capacity 也计入工作区 | 复用已分配空间，但不能承诺任意大扫单均可接受 |
| 行情候选 | 启动时分配 129 个槽位，每槽包含本片行情快照 | 避免逐笔候选分配；总大小随本片商品数量变化，不是整个行情模块仅 129 字节 |
| 待公开订单查询 | Router 的捕获结果正文累计上限 64 MiB | 不会因公开边界未推进而无限缓存查询 |
| 完整恢复/维护核对 | 五项 Boundary、索引位置、实例、peer 和完整视图 | 放在接入或维护阶段，不逐笔传完整拓扑/订单簿 |

默认配置的心跳间隔为 500 ms，连接静默超时和请求进度超时均为 5000 ms，见 [exchange.conf](../../src/config/exchange.conf)。心跳在 Links 内消费，不作为业务完成进展；进度计时是组件最近进展检查，并非每个分块都有独立精确定时器。检测还依赖 reactor 获得执行机会，不能把 5000 ms 当作任何阻塞下的严格故障发现上限。生命周期测试将请求超时调整为 30000 ms，不能混同默认值。

没有为这些检查增加跨片事务日志、每条命令的独立线程或 allocation ACK。正常路径仍受真实 FIFO 片段数、日志同步、消息量和已有分配影响；上述结构控制了开销，不等于已经证明所有路径零分配或具有固定最坏延迟。`--trace` 逐帧日志也会增加开销。

## 9. 哪些情况确实测过

下表区分测试覆盖与代码规则。详细耗时、性能口径及原始结果入口沿用 [06](06_validation_results.md)，不把本次文档核对当成一次新的运行时测试。

| 场景 | 证据 | 已验证内容与边界 |
| --- | --- | --- |
| 正常算法及 corner cases | [depth](../../tests/spreads/depth.cpp)、[execution](../../tests/spreads/execution.cpp)、[session](../../tests/spreads/session.cpp) | 12 路、价格/真实 FIFO/AB 优先、more 补档、负价/零价、不同片段切分、40 个固定种子参考模型场景 |
| 视图先到、明细延迟 | [session](../../tests/spreads/session.cpp) | 故意延迟明细，确认双条件汇合；不能因为 ROUND_READY 已到就提前结束 |
| 报文与状态错误 | [network](../../tests/spreads/network.cpp)、[coordinator](../../tests/spreads/coordinator.cpp)、[publication](../../tests/spreads/publication.cpp) | 身份/版本/分块/参与者/窗口等非法输入和乱序；不是覆盖所有网络分区时序的模型检查 |
| 本片尚未封闭与公开 0 边界 | [persistence](../../tests/spreads/persistence.cpp) | 未封闭 Result 不落盘；已有本片成交但公开边界 0 时历史仍为空；混合类型复制恢复保持事实 |
| 完整边界上的 A 退出 | [native process](../../tests/spreads/process.cpp) | 恢复同一目录，核对订单/历史/行情；原 Gateway 连接上排队的请求恢复后执行一次 |
| 启动器自动单成员恢复 | [lifecycle](../../tests/spreads/lifecycle.py) | SIGKILL 空闲 A，监督重启；Router/Coordinator 与健康片 PID 保持，状态不变并继续交易 |
| 部分内存执行中断 | [native process](../../tests/spreads/process.cpp) | 先停止 A，发起跨片命令，观察 B 的 SOURCE_DONE，再终止并重启 A；确认仍 PAUSED、完成/公开边界不前进、没有该命令 Accepted/Trade；普通 shard 仍可挂撤 |
| S/A/B/C 计划替换 | [lifecycle](../../tests/spreads/lifecycle.py) | 替换 5/6/7/8/5，覆盖 S 的 A→B→A；健康进程、订单、历史字节与公开视图保持一致，替换后能成交 |
| 候选 peer 不可达 | [lifecycle](../../tests/spreads/lifecycle.py) | 制造候选端点错误，激活前准备失败；恢复旧实例和路由后可以继续交易 |
| 正常整组关闭及再启动 | [lifecycle](../../tests/spreads/lifecycle.py) | 实际排空、保存关闭标记、重新启动，订单/历史/视图一致 |
| 未确认关闭的启动门禁 | [lifecycle](../../tests/spreads/lifecycle.py) | 在测试进程全部停止后将隔离状态文件标记为 false，验证启动拒绝；这不是一次真实 supervisor SIGKILL 或掉电实验 |
| 本地同步失败 | 原 [recovery suite](../../tests/recovery/suite.cpp)、[optimizations](../../tests/recovery/optimizations.cpp) | 复用的 Task 2 注入检查覆盖失败 Batch 不公开和查询目标同步失败；不等于验证了跨片部分 fsync 后的自动恢复 |
| 压力与端到端 | [测试说明](../../tests/spreads/README.md) | 百万次核心循环、5000 小单明细、5000 请求突发、15000 本地成交通知，以及真实 Publisher/CLI 适配 |

历史完整 Release 验证为 **26/26 个 CTest 通过，总耗时 159.79 秒**；其中 Task 3 的 9 个 C++ suite 共 52 个用例，另有 Python 生命周期场景。8 项核心/协议 UBSan 测试通过。ASan 在进入测试 main 前初始化失败，不能标为通过，详情已记录在 06。上述完整验证对应 `50c0c6e`；本文最初的行为核对使用 `aca0517`。2026-11-18 组件整理后的完整回归另见 [08](08_component_layout_and_validation.md)，不覆盖这里的历史测量。

目前未有 Task 3 专项故障注入覆盖：逐个磁盘写入/fsync 切点、真实掉电、supervisor 强杀后的残留进程、activate 回复丢失、所有半连接/分区组合。对于这些情况，本文只能说明代码处理规则及限制，不能宣称全部验证成功。

## 10. 日志里怎么看到这些行为

每次启动有独立运行目录，子进程重启日志使用独立名称；配置、统计和维护结果也保留在该次目录。启动器的 `CLUSTER_READY logs=... state=...` 给出位置。

| 日志/状态 | 含义 |
| --- | --- |
| `GROUP_PAUSED component=... reason=...` | 哪个组件停止放行及直接原因；Router 还打印 dispatched/memory/published，Worker/Coordinator 打印 unresolved |
| `GROUP_LINK_REJECT` | 报文解析、身份或状态处理失败，随后关闭该连接；reason 指向具体检查 |
| `GROUP_JOIN_BEGIN`、`GROUP_WORKER_READY`、`GROUP_READY`、`ROUTER_GROUP_READY` | 接入轮次、实例/视图准备及 Router 最终核对进度 |
| `GROUP_MEMBER_LOST / RECOVERED / RESUMED / RECOVERY_FAILED` | 进程丢失、本地重启、获准恢复交易、自动恢复失败；RECOVERED 不等于 RESUMED |
| `GROUP_REPLACEMENT_PREPARED` | 替换候选的组连接和视图已准备，可进入激活阶段 |
| `RESTART_FAILED promotion_sent=...`、`RESTART_NEEDS_ATTENTION` | 区分尚可尝试撤回的准备失败与激活结果可能不确定 |
| `GROUP_SHUTDOWN_DRAINED / UNRESOLVED`、`GROUP_UNRESOLVED_SHUTDOWN` | 关闭是否确认到完整边界、某片是否仍有未结束组工作 |
| `GROUP_TX / GROUP_RX` | trace 模式下按消息类型、epoch、seq、round 和 bytes 跟踪通信 |

Router 管理接口的 `group-status` 字段顺序为：`READY或PAUSED epoch join dispatched memory published queued`。例如 dispatched 大于 memory 表示仍有内存命令未结束；memory 大于 published 表示尚未形成完整的连续公开前缀。`queued` 只统计尚未派发队列，不包含已经执行但等待持久化的命令。

排查时应将同一 epoch/seq 的 Router、Coordinator、实际参与者日志与 Boundary 放在一起看。只看某个 shard 的最新 trades.log、只看到进程重新启动、或只看到一条 READY 前的连接日志，都不足以判定组已恢复。
