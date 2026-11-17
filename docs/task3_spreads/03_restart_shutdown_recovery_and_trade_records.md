# Task 3：沿用 Task 2 的恢复、计划重启与组合成交记录

日期：2026-11-17。状态：已实现；代码构造见 [05](05_implementation_and_data_structures.md)，验证结果见 [06](06_validation_results.md)。正常交易与三档推导以 [01 文档](01_peer_updates_and_durable_publication.md) 为准；本文补充 shard 中断恢复、计划重启和成交历史。

**真实订单和成交事实继续由各 shard 的 Task 2 日志恢复；implied 不落盘，由 Coordinator 根据恢复后的真实三档重建。计划重启沿用基线复制、结果追赶和最终交接：Router 负责关联组停发、业务连接切换和所有参与者的落盘汇总，Coordinator 负责 peer/三档/implied 就绪确认；最终窗口补齐视图后再放行。**

## 1. 范围与继续沿用的部分

只考虑 SP_AB＝A−B、SP_AC＝A−C。S＝shard5，A/B/C＝shard6/7/8。Router、GroupCoordinator 和维护进程保持运行；不设计它们自身中断、磁盘永久损坏或网络分区后的自动选主。

| 内容 | 本方案 |
| --- | --- |
| 文件与后台线程 | 继续使用订单 delta、成交日志、订单簿快照，以及原双日志线程、快照线程、历史索引/查询线程 |
| 本地恢复 | 沿用快照＋连续配对的双日志结果恢复、尾部校验、FIFO 和 ID 计数器恢复 |
| 计划重启 | 沿用当前主实例 → 替换实例的文件基线、结果复制、最终边界校验、激活与退役 |
| implied | 不进入订单 delta、订单簿快照或成交历史；恢复后重新生成 |
| 三档 | 属于真实簿的派生摘要，恢复后从真实簿提取；Coordinator 不保存远程订单副本 |
| 成交历史 | 继续以 trades.log 为事实来源，B+ 树定位记录；为组合成交增加记录类型与读取适配 |

“订单恢复”指 Task 2 已有的真实活动订单、数量、FIFO 和查询能力；不新增所有历史订单事件的永久归档。恢复与复制均应用已经确定的结果，不重新撮合，也不把日志中的旧成交当成新成交通知。

GroupCoordinator 是负责三档、implied 和内存完成确认的 C++ 控制组件；Task 2 的维护编排进程负责启动/替换实例和复制文件，两者不能混称。下文用 Old/New 表示同一逻辑 shard 的旧/新实例，避免与商品 A/B 混淆。

## 2. 恢复就绪：真实状态、连接、三档、implied 四项一起检查

恢复期间，Router 暂停整个 S/A/B/C 关联组的 RUN，在自己的有界队列保留尚未执行的请求；Coordinator 保持组未就绪并停止发出新的内存放行确认。因为各片共享来源，仅停止向故障 shard 派发新单不够，其他片也可能通过 implied 向它发送 EXECUTE_PREFIX。

恢复顺序为：

```text
本地日志恢复完成
    → 恢复 Router 业务连接、Coordinator 控制连接，报告恢复边界
    → 建立需要的 peer 连接，但暂不允许交易
    → 上报恢复后的真实三档，Coordinator 确认来源齐全
    → Coordinator 计算 IN/OUT，并安装完整 implied 视图
    → Coordinator 确认连接及视图就绪，通知 Router 允许恢复 RUN / P2P 成交
```

控制连接可以提前建立以报告进度，“已连上”不等于“可以成交”。Coordinator 给本次接入分配 joinId，并登记逻辑 shard、实例代次和端点；恢复消息用 joinId 关联，正常命令仍使用 01 的 groupEpoch/seq/round。Router 同时登记新实例身份，组恢复就绪与逐命令 MEM_DONE 分开判断；旧实例或旧接入轮次的消息不能覆盖新来源、新视图或计入新命令的落盘回执。

| 接入信息 | 内容 |
| --- | --- |
| RECOVERED | shardId、实例代次、Task 2 的 Boundary、索引已覆盖的 trade_end |
| 连接清单与 PEERS_READY | 本次 joinId、需要连接的逻辑 shard 及当前实例端点；确认握手身份和角色 |
| DEPTH_SNAPSHOT | 沿用 01 的合约、侧、version、hasMore、price/quantity；接入时完整覆盖对应来源 |
| IMPLIED_REPLACE / VIEW_APPLIED | 沿用完整替换和安装确认，接入消息关联 joinId；空视图也明确安装 |

接入控制只在恢复或切换时发生，不逐笔交易携带整份拓扑。implied 内容仍按 route 分组：routeId/more 发送一次，档位只带 price、quantity，不增加源订单 ID。日常命令沿用 01 的结束流程：每轮先同步来源及 implied，发起方收到 ROUND_READY 后判断继续或结束；整条命令及挂簿收尾完成后才封闭 Batch。Coordinator 收齐累计实际参与者的 COMMAND_END 确认后直接 MEM_DONE；恢复就绪不重放这条命令或重新封闭旧 Batch。

### 2.1 需要恢复哪些连接

| 恢复的逻辑 shard | Router 业务连接及 Coordinator 控制连接之外，需要的 peer |
| --- | --- |
| S | A、B、C |
| A | S、B、C |
| B | S、A |
| C | S、A |

仍是五条 peer 长连接 S–A、S–B、S–C、A–B、A–C，无需 B–C。沿用 01 的连接发起规则，由较小 shardId 发起；重启后以新实例端点重新握手。健康片之间的连接可以保留，只替换涉及旧实例的连接。

Router 保留唯一业务上游角色，Coordinator 控制连接和 peer 使用独立连接角色，不能接入原唯一业务上游入口而替换 Router。Router–Coordinator 仍保留一条轻量控制连接，只交换内存完成和维护信息，不传成交结果。standby 可以提前建立候选连接，但不能接收 EXECUTE_PREFIX。旧实例 fenced 后，即使旧 TCP 尚未关闭，也必须拒绝新的真实腿执行。

## 3. shutdown 后恢复

### 3.1 单个 shard 中断，例如 A

1. Router 或 Coordinator 发现 A 不可用后，Router 停止整个关联组的 RUN，Coordinator 将 A 来源标记为不可使用，并暂停相关新行情快照的发布。保留旧缓存用于诊断也可以，但不能拿它继续成交；未执行的请求留在 Router 队列。
2. A 在原数据目录启动，先按 Task 2 恢复真实活动订单、FIFO、成交日志有效前缀及 next_order/next_trade；恢复或重建 B+ 树并追到恢复后的成交边界。此时不生成历史订单的新通知。
3. A 以新实例代次恢复 Router 业务连接和 Coordinator 控制连接，报告 RECOVERED。双方依据保留的命令进度确认真实状态满足第 3.4 节的接入条件，再建立 A–S、A–B、A–C；其余 peer 无需重连。
4. A 发送自己的 bid/ask **完整三档快照**，最多六个价位，空侧也发送。Coordinator 用它整体替换旧 A 来源，不能在未知的旧版本上继续套 delta。S/B/C 若保持在已确认的同一完成边界，继续使用已封闭的缓存；只有内容改变才上报 delta，无需复制它们的订单簿给 A。
5. Coordinator 用五个合约的最新真实来源重算受影响的 IN/OUT。新 A 必须收到完整的 A implied 视图，即使与停机前相同；健康 S/B/C 只接收内容变化的视图，变空时明确清空。S 的 SP_AB、SP_AC 是两个独立目标视图。
6. Router 业务连接和所需 peer 已就绪、来源已确认、相关 VIEW_APPLIED 已收齐后，Coordinator 向 Router 确认本次 joinId 就绪；Router 核对恢复边界后才恢复派发及新行情发布。恢复只安装派生视图，不为此创建日志 Batch。

恢复 B/C 同理。恢复 S 时，S 上报 SP_AB、SP_AC 两个合约的完整三档，最多十二个源价位，并接收两个合约的 implied IN；不能只恢复其中一个。

### 3.2 所有业务 shard 都中断

Router、Coordinator 仍在运行，由 Router 保持组内停发。S/A/B/C 分别恢复本地文件和历史索引，再恢复 Router 业务连接及 Coordinator 控制连接；可以并行做本地恢复，不必等一片完成才启动下一片。

Coordinator 将所有旧来源标为待重建，等待五个合约的完整 bid/ask 快照，共最多三十个源价位。四条 Router 业务连接、五条 peer 连接和所有真实恢复边界确认后，统一计算十二条 IN/OUT 规则，向四个 shard 安装五个合约的完整 implied 视图，包括空视图。Coordinator 向 Router 确认全部就绪后才开放关联组，不能因为 A 已先恢复就让它使用尚未恢复的 S/B/C 来源。

### 3.3 主动正常 shutdown

可控关机先由 Router 停发关联组，完成当前整条命令及所有轮次；Router 收到 MEM_DONE，并收齐此前命令实际参与者直返的 DURABLE_RESULT，排空业务输出和查询，再调用 Task 2 原有关闭流程。仅退出指定 shard；其他 shard 等它重新接入后继续工作。整组正常关闭同样先排空，再分别退出各片。

正常排空会等待落盘，这是维护边界；不改变日常“Router 收到 MEM_DONE 后可执行下一条，同一命令实际参与者的 DURABLE_RESULT 齐全后通知用户”的规则。

### 3.4 紧急中断的接入条件

Task 2 的有效日志前缀只证明**本片**的订单/成交日志配对完成，不自动证明 S/A/B 的同一跨片命令都完整。Router 保留的 group seq → 实际参与者、各片 localBatchSeq 和已发布边界，以及 Coordinator 尚未结束的内存执行进度，共同用于检查接入边界；不同 shard 的本地批次号不要求相等，也不能简单取它们的最小值。

本方案自动开放的条件是：恢复后的真实状态已确认位于完整的关联命令边界，已获准发布的结果仍存在，并且没有尚未解决的 P2P 修改或缺失的参与者结果。恢复前的旧 working 三档可能领先于磁盘，不能据此覆盖恢复簿或宣称追平。

若中断恰好落在“三片只有部分完成持久化”或“真实腿已改内存但整条 Result 尚未封闭”的窗口，Coordinator 不确认恢复就绪，Router 保持组暂停，不能仅重算 implied 就开放。本文按约定不新增跨片回滚日志、结果副本或分布式提交协议；这类在途尾部的自动补齐/取消不属于 Task 2 现有代码已经解决的能力。尤其不能只删除 trades.log 的记录：订单 delta、快照和后续命令可能也受影响。

## 4. restart：保留 Task 2 的复制，只扩展最终交接

仍从现有维护命令指定逻辑 shard，例如 restart 6；restart all 继续逐片维护。Router 保留维护入口、Gateway 暂停/恢复机制以及业务连接切换，改为按整个关联组停发；GroupCoordinator 确认内存排空并负责控制/peer 连接和 implied 交接。无关 shard 仍走 Task 2 原流程。

### 4.1 准备与在线追赶

Old 冻结检查点、等待已有检查点完成，再导出固定订单簿快照与成交日志前缀；New 在新目录恢复并追赶 Old 的已提交结果。继续使用 Task 2 的 tail/apply 和原双日志线程；复制保留原订单/成交 ID、时间、FIFO、文件位置，不重新撮合。

此阶段 Old 仍正常参加 P2P 成交、上报三档、安装 implied。New 只回放真实结果，不向 Coordinator 的活动来源槽位上报三档，也不执行 implied；无需持续复制 Old 的 implied 或远程摘要。New 的候选 Router 业务连接、Coordinator 控制连接及 peer 连接可以提前准备，但不能提前接收 RUN 或 P2P 写入。

### 4.2 最终交接顺序

| 步骤 | 要做什么 |
| --- | --- |
| 1. 停发关联组 | Router 停止新 RUN，将最后派发的 seq 随维护标记发给 Coordinator；未派发请求留在 Router FIFO，Gateway 沿用 Task 2 背压 |
| 2. 完成旧工作 | 最后派发命令的全部轮次及收尾视图同步完成；发起方判定结束，各实际参与者封闭 Batch 并确认 COMMAND_END。Coordinator 收齐后直接 MEM_DONE；Router 再收齐此前各命令直返的持久化结果，排空业务/查询输出 |
| 3. 排空并隔离 Old | Router 在直连 Old 的有序业务连接尾部发送原 Drain，并接收 Drained；除原 idle 条件外，Old 还需没有 P2P 执行、未封闭 Result 或未完成视图安装，随后禁止 RUN 和 peer 写入 |
| 4. 最终结果同步 | New 追到 Old 最终 Boundary；seq、next_order、next_trade、trade_end、trade_count 必须逐项相等，B+ 树追到同一 trade_end |
| 5. 补齐 Task 3 状态 | New 上报真实三档，与 Old 固定边界对应的来源核对；准备 Router 业务连接及指向 New 的控制/peer 连接，废止旧实例身份；Coordinator 向 New 下发其全部 implied，其他片有变化才更新 |
| 6. 激活并切换 | New 的真实边界、索引、peer 和视图全部确认后，沿用 activate；Router 切换该片业务连接和稳定行情入口的内部来源，Coordinator 确认新控制/来源实例已就绪 |
| 7. 恢复与退役 | Router 核对新连接与 Coordinator 的本次接入就绪确认后，按原顺序派发排队请求；维护流程解除 Gateway 暂停、退役 Old，并按 Task 2 保留旧目录和当前槽位记录 |

只向 Old 的订单 TCP 写 Drain 不足以阻止其他 shard 的 P2P 请求，因此第 2 步必须先完成。维护控制与 shard 回执跨连接，按最后派发 seq 核对完成，不能把先收到维护标记当作旧命令已结束。停发的是整个关联组，真正替换的仍然只有指定 shard。

Router–Coordinator 的轻量控制连接保持，原 Router → Old 业务连接切换为 Router → New；命令队列始终在 Router，Coordinator 不保存另一份待派发队列。已执行但尚未通知的请求不能当作未执行请求重放。沿用 Task 2 的失败规则：激活前可安全中止准备，激活请求已发出而结果不确定时，不自动恢复 Old 接单。

New 与 Old 的真实状态完全相同时，理论上其他健康 shard 的 implied 不变；只需给 New 安装完整视图，无需四片全部重发。S 的替换必须同时接管两个价差合约的来源与 IN 视图。维护中的行情保留原稳定入口，完整视图安装前不发布 New 的半成品状态。

## 5. 成交记录：ABC 保留双边语义，S 保存一条组合执行事实

### 5.1 为什么 S 需要扩展

普通成交仍用现有 TradeRecord。ABC 的 implied 腿成交也有明确的经济买卖双方，可以保留 symbol、price、quantity、buy_order/sell_order、buyer/seller；其中一个订单可能是 S 的价差订单。买卖方向是**相对于该条记录的合约**：买 SP_AB 的订单在 A 记录中是买方，在 B 记录中是卖方。

S 的 implied 成交没有一张真实的“对手价差单”：对手是分别位于 A 和 B/C 的两张真实订单。因此不能随意把其中一张填成 SP_AB 的卖单，也不生成虚构的价差 orderId。

本方案区分三类记录，仍写入各片原 trades.log：

| 类型 | 所属片与内容 |
| --- | --- |
| DirectTrade | 原双边 TradeRecord；包括 SP_AB/SP_AC 本簿真实订单之间直接成交 |
| ImpliedLegTrade | A/B/C 上的双边记录，保留原字段，增加下述 executionKey 关联组合成交 |
| SpreadExecution | S 上的一条组合执行记录，保存价差订单、两条真实对手腿及 executionKey |

统一关联键取 executionKey＝(aggressorOrderId, fillNo)。aggressorOrderId 是当前主动单的真实订单 ID，fillNo 在该命令的所有轮次内递增；共享一组实际订单和执行价格的配对段使用同一键。它复用已有真实订单身份，不新增全局 ID 分配器，也不是持久化的 implied ID。各片仍分别生成自己拥有的 tradeId，继续沿用本片 next_trade。

SpreadExecution 的必要信息为：

```text
tradeId、timestamp、executionKey
spreadSymbol、spreadPrice、quantity
spreadOrderId、spreadUser、spreadSide
legA：counterpartyOrderId、counterpartyUser、executionPrice
legX：counterpartyOrderId、counterpartyUser、executionPrice
```

legX 是 B 或 C，由 spreadSymbol 确定；腿方向由 spreadSide 确定，不重复存 implied 路径、源盘口或整个源订单。每条记录的 quantity 同时适用于价差和两条 1:1 腿，spreadPrice＝legA.price−legX.price。两条腿的对手订单都是真实订单，既可能是挂单，也可能是本次主动单。

### 5.2 一个三方成交怎样记

假设 Alice 有 A 卖单 A1，价格 100、数量 10；Bob 有 SP_AB 买单 S1，价格 6、数量 10；Carol 的 B 买单 B1 以 94 成交 10 手。共同 executionKey 为 (B1,1)：

| 记录所属 shard | 合约与成交 | 买卖双方或组合关系 |
| --- | --- | --- |
| A | A，100 × 10 | 买方 Bob/S1；卖方 Alice/A1 |
| B | B，94 × 10 | 买方 Carol/B1；卖方 Bob/S1 |
| S | SP_AB，6 × 10，买入 | Bob/S1；A 腿对手 Alice/A1、价格 100；B 腿对手 Carol/B1、价格 94 |

S1 只减少 10 手，不因出现两条腿记录而减少 20 手。每片只修改自己拥有的真实订单；远端订单 ID 只是成交事实中的引用，不能插入本片活动订单索引。实时订单成交和 OrderUpdate 仍由订单所属 shard 生成，A/B 不再替 S 重复发送一次 S1 的订单扣量通知。

implied IN 同样适用：若主动单是 SP_AB 买单，对手分别是 A 卖单与 B 买单，仍生成上述三种合约的记录。SP_AB 真实买卖单直接成交则沿用普通双边记录，不虚构 A/B 执行价格，也不要求 A/B 写日志。

### 5.3 价位成交后，什么时候取得真实订单身份

维护 implied 仍不需要 orderId；但写成交事实时必须知道真正成交的用户和订单。01 中 EXECUTE_PREFIX(routeId,q) 不变，RESULT_DETAILS 在跨片成交记录构造中具体承担以下工作：

1. 两条来源腿执行 q 后，各自按实际 FIFO 消耗顺序给发起 shard 返回必要片段：(orderId、userId、price、quantity)，按 route 分组；方向和首次主动单上下文不重复传。
2. 发起 shard 对两份片段用双游标对齐，以当前两段数量的 min 切出共同成交段，分配 fillNo 和共同时间，并按 route 公式取得第三个合约的实际成交价。例如 B 的成交价为 A 的执行价减 SP_AB 的执行价，不能直接用主动单限价代替。价位相同但订单不同仍要区分；三档计算规模固定，不代表成交明细规模固定。
3. 发起 shard 将必要的配对分配一次打包给真实参与者，各片据此构造本地记录。已有明细用片段序号引用，不重复传完整订单；发起方已有的信息不要求源再回传。明细可以逐轮收齐，但 COMMAND_END 必须等最终 ROUND_READY、判定不再继续且挂簿收尾完成后才发，不能因明细提前齐全就封闭本单。
4. 多轮结果先累计；所有撮合及收尾 implied 更新完成后，每个实际参与 shard 收到发起方的 COMMAND_END，检查明细完整，将本条成交记录和订单变化封闭为一个 Batch 交原双日志队列，再向 Coordinator 确认 COMMAND_END。Coordinator 收齐后直接 MEM_DONE，不再下发封闭指令、不等 fsync。恢复和复制直接回放记录，不再次联络其他 shard 查历史订单。

这一段是成交记录必需的通信成本，不能将“执行项只有 5 字节”理解为整个成交只传 5 字节。它不增加 PREPARE/投票阶段，也不建立长期的 implied→源订单索引；额外明细的工作量随实际成交订单数增长。

## 6. 文件格式、B+ 树与查询的最小适配

### 6.1 继续同一套文件写入和恢复框架

新增 TradesV2 帧类型，沿用原帧头、CRC、批次配对和帧尾，帧内记录用“类型＋长度＋正文”区分第 5 节的三类成交。旧 Trades 帧继续按 136 字节记录读取，不原地改写历史文件；新记录仍由原成交日志线程写入，不增加组合成交专用日志或第三个写线程。

一个本地 Batch 仍对应一个订单帧及至多一个成交帧。S 同一命令包含直接成交和 implied 成交时，可以在同一 TradesV2 帧保存两种记录；订单帧中的预期成交数计本地记录条数，S 的一条 SpreadExecution 算一条，不把两条腿算成两个 S 成交。

需要调整的现有假设只有明确涉及新记录的部分：

- 当前 Storage::recover() 要求 tradeId、buy_order、sell_order 都属于本 shard。tradeId 仍严格按本片检查；成交中的远端订单引用按其真实所属 shard 和品种角色校验，不能拿本片 next_order 验证远端计数。订单 PUT/DELETE 和快照中的活动订单仍必须全部属于本片。
- 当前价格解码要求 price>0。价差真实订单、DirectTrade 和 SpreadExecution 需要允许合法的零/负价差，包含快照、delta、成交日志和复制解码；单腿价格继续遵守其原规则。
- Result、编码/解码、复制、历史读取与预算计算支持三类记录；记录数、ID 计数、最大帧/命令大小仍按原机制校验。仅真实结果参与者创建 Batch，implied 替换不创建。
- tail/apply 保留原始成交帧的字节布局和边界；New 不得把旧帧重新编码成另一格式，否则已有历史游标会失效。支持新格式的程序读取旧格式；不要求旧程序能够读取 TradesV2。

### 6.2 B+ 树继续定位本地文件，无需跨片查询拼装

现有索引定位采用 frame＋ordinal，Get() 内写死 136 字节步长。加入不同长度记录后，改为 TradePosition＝(recordOffset:uint64、typeAndLength:uint32、checksum:uint32)，仍为 16 字节；typeAndLength 打包记录类型与正文长度，区分旧记录和三种新记录。树结构、缓存、索引线程和查询线程继续使用原实现。索引快照升级版本，不兼容的旧索引按 Task 2 的机制从成交日志重建。

recordOffset 指向记录正文。旧记录计算出原字节位置，新记录在扫描帧时记录实际位置、类型和长度；查询由 B+ 树取得这些信息后，按长度读取、校验并选择对应解码器。afterOffset/throughOffset 继续使用文件位置，复制和计划重启不移动它们。

索引归属规则：DirectTrade 和 ImpliedLegTrade 继续为买卖双方建索引，自成交同一账户只插一次；SpreadExecution 为 spreadUser 建一条索引。A/B 对手用户在自己的腿记录里查询成交，不把 S 的组合汇总再次当作自己一笔价差成交。

因此例子中 Alice 查询到 A 的记录，Carol 查询到 B 的记录；Bob 可以查到 SP_AB 汇总及 A/B 两条腿记录，通过 executionKey 识别同一组合执行。这三条表示不同合约的事实，统计 SP_AB 成交量时只数 S 汇总。

原历史请求、登录权限、分页结束和 Router 扇出继续沿用。普通记录保留原字段；ImpliedLegTrade 和 SpreadExecution 增加带类型的历史响应，返回关联键/组合字段，客户端据类型展示。不能把 S 的三方事实硬塞进旧的 buyer/seller 两个槽位；每条组合记录计一条分页结果，续页仍指定原逻辑 shard。

### 6.3 查询不能早于跨片发布条件

B+ 树可以照旧追赶本地已持久化日志，但组内查询的可见上限不能仅看本片双日志完成。Router 根据 MEM_DONE 给出的最终实际参与者及各片直返的 DURABLE_RESULT，维护每片已获准公开的连续 trade_end；结果附带对应本地边界。Router 直接向 shard 转发历史请求并携带该可见上限，shard 再用它限制原 B+ 树查询，Coordinator 不转发查询或成交结果。

可见上限为零要明确表示“空范围”，不能误用原请求 throughOffset=0 的“捕获本地最新边界”语义。翻页保持首次捕获的截止位置；后一条已获准通知的命令若位于本片尚未公开的旧记录之后，历史查询可以暂时等前缀推进，不因此修改 01 的逐命令通知放行规则。

计划重启在最终排空时完成这些旧结果的发布并追平索引；New 继承同一可见文件边界。shutdown 恢复则保留 Router 的发布进度，先核对真实恢复边界，再恢复查询；不通过重发历史 Trade 来“补”实时通知。活动订单查询和行情仍在 01 规定的完整命令边界捕获并等待相应发布条件。

## 7. 实现入口与验证重点

| 现有位置 | Task 3 需要补充 |
| --- | --- |
| [维护编排](../../src/matching_engine/cluster/run.py)、[Router](../../src/matching_engine/cluster/router.cpp) | 保留指定 shard 重启、业务连接切换和稳定行情入口；Router 按组停发、汇总落盘结果，并核对 Coordinator 的内存/视图就绪确认 |
| [引擎入口](../../src/matching_engine/runtime/server.cpp)、[WorkerControl](../../src/matching_engine/cluster/worker.cpp) | peer 身份/角色、恢复接入、视图就绪，以及扩展后的 Drain/activate 条件 |
| [Result/TradeRecord](../../src/matching_engine/persistence/model.hpp)、[codec](../../src/matching_engine/persistence/codec.cpp) | 新记录类型、组合配对关联、TradesV2 编解码与预算统计 |
| [Storage](../../src/matching_engine/recovery/storage.cpp)、[DurableEngine](../../src/matching_engine/persistence/durable_engine.cpp) | 兼容新记录和远端成交引用；继续原本地恢复、双线程写入和结果复制 |
| [HistoryStore](../../src/matching_engine/trade_history/history_store.cpp)、[TradeIndex](../../src/matching_engine/trade_history/index/trade_index.hpp) | 变长记录定位、组合用户索引、查询可见边界与历史响应适配 |

实现后至少验证：单片/全片正常恢复后真实簿和 IN/OUT 正确；空来源清掉旧 implied；新实例旧消息隔离；五条 peer 恢复；单片 restart 在 P2P 大单多轮期间正确排空；S 同时恢复两个价差合约；负/零价差及远端订单 ID 可恢复；三方成交记录、订单扣量和用户索引一致；复制保留历史字节位置且旧游标续页有效；只更新视图不写日志；无法确认完整跨片边界时不提前开放。

本文的正常恢复、计划替换和历史记录路径已有对应实现与新增测试，具体证据见 [06](06_validation_results.md)。部分跨片尾部仍按第 2 节保持暂停，不新增自动修复协议。默认启动器另保存整组干净关闭标记，阻止不确定关闭后用全新 Router/Coordinator 猜测旧发布状态。Task 2 依据：[计划重启设计](../task2_recovery/09_planned_restart_router_design.md)、[双日志与快照](../task2_recovery/11_orderbook_delta_snapshot_consistency.md)、[历史查询 API](../task2_recovery/bpt_trade_history/02_history_api_and_client.md)。
