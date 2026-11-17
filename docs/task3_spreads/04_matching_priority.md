# Task 3：撮合优先级与 AB／AC 路径选择

日期：2026-11-17。状态：已按本文规则实现，选择器、完整 Session 和真实 peer 网络均有对应测试；见 [实现说明](05_implementation_and_data_structures.md) 与 [测试报告](06_validation_results.md)。

**先比较成交价格；同价时，直接挂单按原 FIFO 全部优先于 implied；两条 implied 路径同价时，按到期先后选择。本文约定 A 为近月、B 为较近的远月、C 为更远月，因此同价时 AB 优先于 AC。** 更优价的 AC 仍然优先于较差价的 AB。

本文补齐 [01 的正常撮合流程](01_peer_updates_and_durable_publication.md) 中的“既定优先级”。范围仍为 SP_AB＝A−B、SP_AC＝A−C，比例均为 1:1，支持现有设计的 IN 和 OUT。Router、Coordinator、三档、P2P、整单 Batch 和 [03 的恢复流程](03_restart_shutdown_recovery_and_trade_records.md) 继续沿用。

## 1. CME 规则在本项目中的取舍

CME 的 [Supported Matching Algorithms：FIFO for Implied Orders](https://cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/457218479/Supported%2BMatching%2BAlgorithms) 明确说明：在同一价格，先匹配直接挂单，之后匹配 implied；跨期路径按到期先后排列，并直接举了 A−B 先于 A−C 的例子。这里的前提是 B 比 C 更早到期。

[Futures Implied Order Matching Priority](https://cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/457096650/Futures%2BImplied%2BOrder%2BMatching%2BPriority) 进一步规定：同价来源逐项比较；同类跨期价差先比较前腿最后交易日，相同再比较后续腿，仍无法区分时最终以较小的 Security ID 决胜。

本项目两条路径都是同产品、同类型的 1:1 跨期价差，前腿同为 A。策略类型等共同条件不用变成动态排序机制，只需比较 B/C 的到期顺序。这里的“到期顺序”按 CME 的最后交易日理解。首版固定合约定义为 A 早于 B、B 早于 C；这是本设计的静态约定，现有 Product 配置尚无到期字段，不能声称程序已能自动识别到期日。

[CME Implied Orders](https://cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/457095346/Implied%2BOrders) 还规定 implied 没有时间优先权，且 implied 买卖不能互相成交。因此无需保存 implied 的 arrivalKey，也不按生成时间、刷新时间或网络到达时间给 AB/AC 排队。

## 2. 唯一的比较顺序

以下规则只比较**同一目标合约、同一对手侧**的可执行候选。候选先通过现有订单合法性检查和主动单限价检查；价差允许零/负价格时仍按有符号数值比较。

| 比较层次 | 规则 |
| --- | --- |
| 1. 价格 | 主动买单取最低 Ask，主动卖单取最高 Bid；价格始终优先于来源 |
| 2. 同价的直接／implied | 先直接挂单，再 implied；即使直接单比源订单晚到，也先执行直接单 |
| 3. 同价的直接订单之间 | 沿用真实价格档的 FIFO；部分成交不改变剩余订单的队列位置 |
| 4. 同价的 implied 路径之间 | 本版 AB 在前、AC 在后；不比较路径数量、源订单年龄或回包先后 |
| 5. 已选路径的真实来源分配 | 两个来源各自从最优价开始取量，同价位仍按各自的 FIFO |

买卖方向只影响价格的升降序，**不会反转 AB／AC 的同价顺序**。买单只能执行 price≤limit 的卖侧候选；卖单只能执行 price≥limit 的买侧候选。数量仅决定可以成交多少，不决定谁优先。

实现可把静态来源顺序预先折叠为 `routePriority`：A 的 AB OUT＝0、AC OUT＝1，买卖两侧相同。该值由共享的 route 表取得，不加进每笔执行请求；`routeId` 负责识别路径，不默认以其数值大小表示优先级。

如果以后配置了实际到期日，就按 `(前腿最后交易日, 后腿最后交易日, spreadSymbolId)` 升序生成顺序；最后的 symbolId 是项目对 CME Security ID 决胜规则的映射，必须稳定。当前映射为 SP_AB＝1004、SP_AC＝1005。配置顺序在运行及 restart/recovery 前后保持一致，并纳入已有配置标识校验；不能因为重新生成 implied 而变化。

### 各个订单簿实际比较什么

| 主动单所在簿 | 同价时的顺序 |
| --- | --- |
| A | A 直接挂单 FIFO → AB 的 OUT → AC 的 OUT |
| B | B 直接挂单 FIFO → AB 的 OUT |
| C | C 直接挂单 FIFO → AC 的 OUT |
| SP_AB | SP_AB 直接挂单 FIFO → A/B 生成的 IN |
| SP_AC | SP_AC 直接挂单 FIFO → A/C 生成的 IN |

只有 A 同时面对 AB、AC 两条 OUT。SP_AB 和 SP_AC 虽然同在 S shard，仍是两个不同合约，不能互相撮合；也不额外规定“所有 AB 外部命令先执行”。外部命令继续按 Router 的组内 FIFO 执行。

## 3. 怎样接入原撮合循环

原引擎继续负责限价检查、真实档内 FIFO、订单扣量、部分成交、剩余挂簿、撤单及结果生成。需要扩展的是“选下一个对手”的入口：不能先调用原 `match()` 扫完整个直接盘，再补做 implied，否则可能先成交较差的直接价。

对 A，只需合并三条有序流：直接盘、AB implied、AC implied；其他簿最多两条。每次比较这些流的首个候选，按第 2 节选出一个，取主动单剩余量与候选量的 min，然后推进相应游标。无需给所有真实订单重新排序。

```text
用当前完整视图建立本轮临时游标；不修改 Coordinator 的来源缓存
while 主动单还有未分配量:
    candidate = 按价格、直接优先、routePriority 选最优头部
    若没有候选，或最优价格不满足限价：停止本轮选择
    分配 q = min(未分配量, candidate 剩余量)，记下选择顺序
    推进该候选；同 route 始终取连续最优前缀
    若某 route 的窗口耗尽、more=true，且主动单还有剩余：停止选择，准备补档
执行本轮选中的本地量及各 route 前缀
按 01 完成来源更新、implied 安装及 ROUND_READY
发起 shard 按最新视图决定继续、挂簿收尾或 COMMAND_END
```

三条流只用于选择本轮数量。实际执行仍是现有本地订单操作和 `EXECUTE_PREFIX(routeId,q)`：每条 route 的选中数量可合并发送，两腿各执行相同 q。A 的两条 OUT 分别消耗 SP_AB/B 和 SP_AC/C，真实来源互不重叠，允许沿用同轮批量 P2P 执行。

**合并网络请求不能丢掉本地成交顺序。** 发起 shard 保留本轮选择的有序片段，例如 `AC@119 → direct@120 → AB@120 → AC@120`；远端明细回来后，按这份顺序和源 FIFO 片段构造成交结果及 03 的 fillNo。不能按 EXECUTION_DONE 的到达先后排列成交，也不能把 AC 所有价位统一排在 AB 前面。这份临时计划在命令结束后释放，不是长期 implied→订单索引，不增加热路径执行项字段。

### 来源的 FIFO 与成交价格

路径已选定后，源 shard 执行的是指定真实侧的数量前缀，不重新与自己的 implied 竞争。两个来源各自按价格/FIFO 取得真实订单，再按数量对齐成共同成交段；不在不同源簿之间比较谁的订单更早。

- 直接成交继续使用对手挂单价。
- implied 成交使用所选路径的实际组合价，不能用主动单限价代替。A OUT 为 SP_AX＋X；X OUT 为 A−SP_AX；SP_AX IN 为 A−X，X 为 B 或 C。
- 每个配对段满足 `price(SP_AX)=price(A)−price(X)`，三者数量相同；各所属 shard 只扣减自己的真实订单。
- implied 只是候选流；不得插入真实 FIFO，不占用真实 orderId，也不触发 implied 对 implied 的撮合。替换视图本身不会制造一张新主动单。

## 4. 三档补齐不能改变优先级

三档只限制每轮可见的来源窗口，不能把窗口耗尽误认为整条路径已无流动性。

首版采用简单规则：**选中并耗尽某条 route 的窗口后，如果 more=true 且主动单还有剩余，就结束本轮选择，先执行并刷新，再比较所有来源。** 即使另一条 route 或直接盘还有报价，也不在这次补档之前继续选择。这样无需发送更深价位或额外优先级字段。

例如 AB 已显示的前缀全部消耗，AC 下一档为 123；补档后 AB 出现 122，则必须先成交 AB 的 122。如果补出的 AB 也是 123，则该价先直接单、再 AB、再 AC。不能因为 AC 一直留在内存里就让它抢先。

若 more=false，才可将该 route 视为当前已耗尽，继续选择其他来源。若某 route 仍有已知最优价，但它已不满足限价，较深价格也不会更优，不需要为了证明不能成交而空跑补档。

每轮刷新后，都从最新候选按同一比较规则继续；不重置原主动单，不给重建的 implied 增加时间优先级。整条订单和必要挂簿收尾结束后，才按 01 通知 COMMAND_END、封闭 Batch；Coordinator 收齐结束确认后 MEM_DONE，Router 才派发下一单。

## 5. 完整例子：更优价 AC、同价直接、同价 AB

假设 B 早于 C 到期，所有价格均合法，初始视图已同步，未列出的原生侧为空，无更深来源。

| 真实来源 | 卖盘 |
| --- | --- |
| A | 120 档：早到订单 2 手、后到订单 1 手；122 档：4 手 |
| SP_AB | 10 × 8、11 × 2 |
| B | 110 × 10 |
| SP_AC | 15 × 7 |
| C | 104 × 2、105 × 5 |

由此生成 A 的 AB asks＝120 × 8、121 × 2；AC asks＝119 × 2、120 × 5。A 新到一张**买 14 手、限价 121** 的订单：

| 次序 | 来源 | 成交价 | 数量 | 选择原因 |
| --- | --- | --- | --- | --- |
| 1 | AC implied | 119 | 2 | 全部来源中的最优价 |
| 2 | A 直接早到订单 | 120 | 2 | 同价直接优先，档内 FIFO |
| 3 | A 直接后到订单 | 120 | 1 | 直接 FIFO 尚未耗尽 |
| 4 | AB implied | 120 | 8 | 同价 implied 中 B 比 C 更早到期 |
| 5 | AC implied | 120 | 1 | AB 的 120 已空，主动单剩 1 手 |

合计 14 手。不能先成交 AB 的 121，因为 AC 仍有 120；也不能把 AB 和 AC 在 120 的数量按比例分配。

本轮可以合并成 AB 的 q=8、AC 的 q=3；S 同批处理两个 spread，B 执行 AB 的 8 手，C 执行 AC 的 3 手。A 的三手直接成交继续按本地 FIFO 处理，返回明细再按上表形成完整结果。

成交后 SP_AB 剩 11 × 2、B 剩 110 × 2；SP_AC 剩 15 × 4、C 剩 105 × 4。A 的 implied 更新为 AB 121 × 2、AC 120 × 4，直接卖盘剩 122 × 4。本例实际结果参与者为 S/A/B/C，沿用 Router 等待四片持久化结果的规则。

## 6. 实现后的验收重点

| 场景 | 必须得到的结果 |
| --- | --- |
| implied 价格优于直接盘 | 先 implied，不能先扫完直接盘 |
| 直接与 implied 同价 | 直接 FIFO 先全部执行；直接单到达较晚也不改变此规则 |
| AB、AC 同价 | 本版 AB 先；买卖两个方向一致，数量大小与回包速度不影响顺序 |
| AC 更优价 | AC 先；AB 的到期优势仅用于同价 |
| 同 route 源价位内有多张订单 | 各来源按本地 FIFO，部分成交保留位置，合计扣量相等 |
| route 窗口耗尽但 more=true | 先补档，再与直接盘及另一 route 重新比较 |
| implied 重建／restart | 来源优先级不变，真实订单 FIFO 不变 |
| implied 买卖交叉 | 不互相成交，不产生无真实主动单的撮合循环 |
| 价差单直接成交与 IN 竞争 | 同价先本簿直接 FIFO；更优价 IN 先成交 |
| 源撤单、部分成交、余量挂簿 | 沿用 01 更新全部受影响路径，再放行下一条外部命令 |

本文件限定撮合顺序，不新增 Pro-Rata、跨产品策略、订单类型或生命周期规则。价差限价区间采用两腿现有区间推导：`SP_AX ∈ [A.lower − X.upper, A.upper − X.lower]`；支持零价及负价。A/B/C 沿用原价格区间及整数取整规则。优先级比较不能替代价格合法性校验。
