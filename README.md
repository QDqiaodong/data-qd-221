# data-qd-221 · 持久任务投递

应用将任务写入 SQLite；后台 worker 按 FIFO 获取当前可领取的任务，凭当次投递凭据确认。未确认任务到期后可重投，旧 worker 无法用过期凭据删除它。多个 worker / `Broker` 实例可共用同一个数据库文件。

计时由调用方注入，便于确定性验证；所有调用方需使用同一时间基准。`now == expires` 即表示租约到期。当前提供单队列、资源键内保序、能力匹配批量领取和至少一次投递，不承诺消费端恰好一次、延迟重试或死信策略。

仅使用 Python 3.9+ 标准库和 SQLite，无外部服务或依赖。

## 公开接口

```python
job_id = broker.put_ordered(
    "generate-thumbnail",       # str 或 None；UTF-8 编码后至多 16 KiB
    key="asset-123",            # 1-64 个 [A-Za-z0-9_-]，区分大小写
    needs=frozenset({"gpu"}),   # frozenset[str] 或 None
)

batch = broker.take_batch(
    {
        "gpu-0": frozenset({"gpu", "codec"}),
        "plain-0": frozenset(),
    },
    now=100,
    lease=30,
)
# batch: {slot_id: Delivery(id=..., payload=..., receipt=...)}

broker.acknowledge(batch["gpu-0"], now=105)
```

可用能力标签为：`gpu`、`highmem`、`ssd`、`vector`、`codec`、`archive`、`arm`、`x86`。

`take_batch` 最多接受 24 个槽；`workers=None` 或 `{}` 表示本轮没有空闲槽。槽 ID 的命名规则与资源键相同，也区分大小写。槽 ID 只用于本轮返回结果，数据库不保存跨轮槽占用状态，确认仍以 `Delivery.receipt` 为准。

## 归档分区变更流

归档源事件可能跨分区重复、乱序到达或补传。不要在 `stage_event` 时直接调用 `put_ordered`；先把事件可靠暂存在 SQLite，等源给出时间水位和无缺号截点后，再由调度进程用 `publish_ready` 小批、全局有序地转入普通任务队列。

```python
broker.configure_feed(("A", "B"))          # 首次 True；同组任意顺序重登 False

broker.stage_event(
    "A", 2,                                # 分区内正整数 offset，可先传后面的号
    event_time=6,                          # 业务时间轴上的非负 int，不是租约 now
    key="asset-123",
    needs=frozenset({"gpu"}),              # 或 None；None 与 frozenset() 同义
    payload="archive-payload",             # 规则同 put_ordered
)

broker.seal_source("A", through=3, watermark=10)
broker.seal_source("B", through=2, watermark=10)
broker.source_checkpoints()
# {"A": (3, 10), "B": (2, 10)}

ready = broker.publish_ready(limit=16)
# [("A", 2, job_id), ("B", 1, job_id), ...]
```

### 源登记与幂等暂存

- `configure_feed(partitions)` 接收恰好一个元组，含 1–8 个互不相同的分区名；名称遵循资源键格式并区分大小写。每个数据库只能有一组源：首次登记返回 `True`，同一组名称以任意顺序重复登记返回 `False`；改成另一组或输入非法均抛 `ValueError`。源组运行期不增删。
- 未成功登记源组时，`stage_event`、`seal_source`、`source_checkpoints` 和 `publish_ready` 都抛 `ValueError`；带分区名的入口也拒绝源组之外的名称。
- 事件身份是 `(partition, offset)`。offset 是分区内从 1 开始的正整数；`offset`、`event_time` 都必须是不是 `bool` 的 SQLite 有符号 64 位整数，其中 `event_time >= 0`。`key`、`needs`、`payload` 的格式和上限与 `put_ordered` 完全相同。
- `stage_event` 只写暂存表，不创建 `jobs` 行、不消耗任务 ID。
  - `offset <= 当前 through`：只要参数本身合法，重传一律返回 `False`；不比较旧内容，也不会重建任务。即使该前缀的暂存数据已在发布后回收，也可仅凭截点幂等确认。
  - `offset > 当前 through`：同身份且 `event_time/key/needs/payload` 内容相同返回 `False`；同身份异内容抛 `ValueError`；新事件成功写入返回 `True`。`needs=None` 与 `needs=frozenset()` 视为相同内容。
  - 新 offset 最多为 `当前 through + 12_000`；新事件的 `event_time` 必须严格大于分区当前水位，否则抛 `ValueError`。

### 截点、水位与背压

`source_checkpoints()` 返回 `{partition: (through, watermark)}`；登记后、首次封存前均为 `(0, -1)`。

`seal_source(partition, *, through, watermark)` 的两个值都必须是不是 `bool` 的非负 SQLite 有符号 64 位整数：

- `through`、`watermark` 均不得倒退；新 `through <= 当前 through + 12_000`。
- `(through, watermark)` 与当前截点完全相同时，是幂等重传，返回 `False`；through 或 watermark 任一合法前进均返回 `True`。
- 推进 through 时，`(旧 through, 新 through]` 内每个 offset 都必须已暂存，不能缺号；这些消息的 `event_time` 必须 `<= 新 watermark`。
- 所有已暂存且 `offset > 新 through` 的消息，`event_time` 必须严格 `> 新 watermark`。
- through 不变时也可以只提高 watermark，用来证明该分区这段时间没有新事件；不需要为了推进水位而伪造 offset 或心跳事件。尚未有事件的新分区可用 `seal_source(name, through=0, watermark=...)` 把初始 `-1` 水位提升到非负值。
- 缺号、参数非法或事件时间与水位承诺矛盾时抛 `ValueError`，截点和暂存内容整体回滚。
- 每个分区至多有一段已封但尚未全部转入 `jobs` 的前缀，以及至多 12,000 个 offset 宽度的未封消息。若上一段已封消息仍有暂存行，合法请求也不能推进 through，应抛内置 `BufferError`；但 through 不变、只提升 watermark 仍允许。这个背压只看“是否已经写入正式任务表并删除暂存行”，与 worker 是否可领取、是否持有租约或是否已经 ack 无关。
- 已发布事件的暂存行会在同一事务中删除；之后重传靠 through 识别，不保留无限增长的逐事件历史。

### 全局有序发布

调用方（通常是调度进程）应反复调用小批量：

```python
published = broker.publish_ready(limit=64)
# published: [(partition, offset, job_id), ...]，与正式 job ID 升序同序
```

- `limit` 必须是不是 `bool` 的整数，且 `1 <= limit <= 128`，否则抛 `ValueError`。
- 安全时间是**所有分区当前 watermark 的最小值**；暂时没有事件的分区也参与，因此任一分区仍为初始 `-1` 时不会发布任何事件。
- 候选事件必须已经封存、暂存行仍在（即尚未入队），且 `event_time <= safe_time`。
- 本批按 `(event_time, partition 的 ASCII/二进制字典序, offset)` 全局排序，取最早的至多 `limit` 条。同一时刻先按分区名排序；同一分区同一时刻再由 offset 决胜。不会按分区各切一批，也不会让同一分区的旧 offset 阻塞事件时刻更早的消息。
- 返回列表顺序就是上述全局顺序，也是本批正式 job ID 的递增分配顺序。

`publish_ready` 在一个 `BEGIN IMMEDIATE` 事务中完成安全时间计算、索引选择、正式 `jobs` 写入和对应暂存行删除：全部成功或全部回滚。多个 `Broker` 实例/进程并发调用同一 SQLite 文件时，写锁使各批串行提交，因此同一条暂存消息不会重复入队，一批内的 job ID 也不会被其他写入打乱。已提交响应丢失后直接再次调用即可：上一批的暂存行已删除，后续调用从下一条开始，不会补发同一任务。

发布出的行与 `put_ordered` 写入的任务完全同构，共用原 job ID 序列、能力掩码、资源键 FIFO、`Delivery`、租约、确认和到期重领。它们没有 `graph_nodes`、依赖或 sink 记录，因此不会产生工作流结论；普通任务、原有工作流和接收方行为保持不变。归档发布只保证已发布归档任务彼此按上述顺序获得 ID：发布前不会插到已有普通任务之前，两次 `publish_ready` 之间仍允许原入口写入新任务。

就绪选择使用 `sealed = 1` 部分索引 `(event_time, partition, source_offset)`，并用 `LIMIT` 提前终止；查询不会为了找到极短的就绪前缀而扫描、排序或装载整个未来积压，payload 也只读取本批中签的行。

## 工作流依赖图

使用 `submit_graph(workflow_id, nodes)` 在同一个 SQLite 事务内提交整组节点。`workflow_id`、节点 `name` 和节点 `key` 都使用上述 1–64 字符命名规则；同一工作流内节点名不能重复，工作流名一旦使用便不能再次提交（即使所有节点都已结束）。

```python
ids = broker.submit_graph(
    "release-2026-11",
    [
        {
            "name": "publish",
            "key": "asset-123-publish",
            "payload": "publish-config",  # str/None，<=16 KiB UTF-8
            "needs": None,                # 同 put_ordered 的能力要求
            "requires": ["transcode"],    # 这些节点必须 succeeded
            "after": ["audit"],           # 这些节点进入任意终态即可
        },
        {
            "name": "transcode",
            "key": "asset-123-transcode",
            "payload": None,
            "needs": frozenset({"codec"}),
            "requires": [],
            "after": [],
        },
        {
            "name": "audit",
            "key": "audit-pool",
            "payload": "audit-payload",
            "needs": frozenset(),
            "requires": ["transcode"],
            "after": [],
        },
    ],
)
# ids == {"publish": 1, "transcode": 2, "audit": 3}
```

节点按列表顺序取得与普通任务共享的整数 job ID，依赖可以引用后面才声明的节点。单次最多 12,000 个节点、48,000 条显式 `requires`/`after` 依赖。

提交时会一起检查三类等待关系是否成环：

- `requires`：前置必须成功；
- `after`：前置成功、失败或跳过均可；
- 同资源键 FIFO：同键中 ID 较小的节点排在前面。

因此，若较早声明的发布节点与稍后声明的转码节点同键，同时发布又 `requires` 转码，即使显式依赖本身只有一条边，也会和键内顺序形成环并被拒绝。非法参数、未知/重复/相交引用、环或重复 `workflow_id` 都抛 `ValueError`，事务回滚，不留下部分任务或依赖。

### 依赖、跳过和结束

`take` 和 `take_batch` 会先排除依赖尚未满足的节点，再执行原有键内队首、租约、能力和最大批量分配逻辑。未满足依赖的节点仍是同键队首阻塞者，不会让同键后继绕过它。

- `requires` 的每个前置都必须是 `succeeded`。
- `after` 的每个前置进入 `succeeded`、`failed` 或 `skipped` 任一终态即可。
- 任一 `requires` 前置 `failed` 或 `skipped`，该节点在同一次失败结束事务中立即变为 `skipped`，并继续沿 `requires` 反向传播；不会沿 `after` 传播跳过。
- 终态图节点会从活动 `jobs` 队列删除，但终态和节点名保留在图状态表中；如果它前面仍有未结束的同键任务，键内 FIFO 仍由该较早任务阻挡后继。

成功仍使用原入口：

```python
broker.acknowledge(delivery, now=105)
```

工作流节点的不可重试失败使用：

```python
broker.fail(delivery, now=105)
```

`fail` 的 `now` 必须是非负、不超过 SQLite 有符号 64 位上限的整数，且不能是 `bool`，否则抛 `ValueError`。对普通任务、过期/过时凭据或已结束节点，`fail` 返回 `False` 且不改变状态；只有用当前有效凭据结束待处理工作流节点时返回 `True`。成功和失败通过 `BEGIN IMMEDIATE` 与条件删除竞争，最多一个生效；凭据失效、终态写入、级联跳过和活动队列删除在同一事务提交。

```python
broker.graph_status("release-2026-11")
# {
#   "publish": "pending" | "succeeded" | "failed" | "skipped",
#   "transcode": "...",
#   "audit": "...",
# }
```

`pending` 同时表示尚不可领、当前可领和已租约但未结束。非法工作流名抛 `ValueError`；名称合法但未登记抛 `KeyError`。重开数据库后状态和调度资格立即恢复，旧凭据不能改写终态。

## 结论投递（下游接收方）

索引、清理等下游不必轮询工作流，也不占用 worker 的领取/租约。它们先登记一个带容量的接收方，提交工作流时声明该工作流要把每个节点的结论投递给哪些接收方。

```python
broker.register_sink("index", capacity=3)     # 首次 True；同名同容量 False
broker.register_sink("cleanup", capacity=3)

broker.submit_graph(
    "release-2026-12",
    nodes,
    sinks=("index", "cleanup"),               # 仅关键字；默认为空
)
```

- 每个库至多 32 个接收方，名称与资源键同规则且区分大小写。`capacity` 是 1–12,000 的整数（不含 `bool`）。首次登记返回 `True`，同名同容量再次登记返回 `False`；同名不同容量、达到上限后新增或非法参数抛 `ValueError`。没有改容量或注销接口。
- `sinks` 是由不同已登记名称组成的元组（最多 32 个）。工作流每个节点最终向每个接收方各交付一条结论。提交时按**节点总数**向每个接收方同时预留名额；任一接收方余额不足抛 `BufferError`。
- 名额计数的是“该接收方尚未确认的节点”：等待依赖、等待领取、持有租约以及已进入终态尚未确认的节点都计入；确认后释放。普通任务、未指定 `sinks` 的历史/新工作流都不占名额、不产生结论。
- 输入非法（未知接收方、重复名称、非法图、重复 `workflow_id`）抛 `ValueError`，即使同时余额不足也先报 `ValueError`。失败不写部分工作流、不占用工作流名、任务 ID 或任何接收方名额；预留与入队在同一 `BEGIN IMMEDIATE` 事务提交，两个实例争最后一格时不可能都成功。

### 结论与确认

节点在有效凭据下 `succeeded`/`failed`，或按 `requires` 规则被传播为 `skipped`，即形成结论；原队列移除、终态、依赖放行与该结论可读在同一事务生效。租约到期、失效回执、重复结束都不产生结论。**终态路径不再申请名额**，所以接收方名额已满也不会阻塞已准入工作流确认、失败或一次失败触发的全部跳过。

```python
outcomes = broker.read_outcomes("index", limit=50)
# [{"seq": 12, "workflow_id": "...", "name": "publish",
#   "job_id": 7, "status": "failed"}, ...]

broker.ack_outcomes("index", through=outcomes[-1]["seq"])  # True
```

- 每条结论含 `seq`、`workflow_id`、`name`、`job_id`、`status`，不含 payload 或租约凭据。同一节点只有一个逻辑结论，多接收方看到相同 `seq`；全部结论的 `seq` 按产生顺序严格递增（允许间隔，不因确认或重开而复用）。一次失败先排失败节点，再排同事务新跳过的节点（按任务 ID 升序）；不同事务按提交先后排序，不按任务 ID。
- `read_outcomes(name, *, limit)` 只读该接收方尚未确认的最早结论，按 `seq` 升序，至多 `limit` 条（1–128 的整数，不含 `bool`）。读取不占租约、不推进进度；重开数据库后未确认部分仍可重读。
- 调用方先把已处理前缀持久化到下游，再调用 `ack_outcomes(name, *, through)`。`through` 须为正的 SQLite 有符号 64 位整数（不含 `bool`）。不大于该接收方水位（初始 0）时返回 `False` 且无副作用；高于水位时必须命中该接收方当前可读的一条结论，否则抛 `ValueError`（不接受别的接收方的序号或未来序号）。成功返回 `True`，一次确认该接收方截至该序号的全部未确认结论，按实际条数释放名额（不按序号差），不影响其他接收方，并发/重复确认不会多释放。
- 结论一旦被所有目标接收方确认即回收其流存储，不会随已确认数量无限增长；已有 `graph_status` 的终态历史不受影响。

## 保序与调度规则

- 每个资源键只有 ID 最小且尚未确认的队首任务可以参与分配。
- 队首即使租约未到期、已到期但本轮无兼容槽，也会阻止同一资源键的后继任务被领取。
- 租约到期后只重新投递同一条队首任务并生成新凭据，不会推进资源键。
- 成功确认/删除队首后，后继任务才可能被领取；不同资源键互不阻塞。
- 旧 `put` 写入的任务 `resource_key IS NULL`，每条都是独立的无键任务，保留原有“前一条在租约中时仍可领取后一条”的竞争语义。
- 旧 `take` 等价于一个无能力标签的槽：它与 `take_batch` 竞争，并遵守相同的资源键队首限制，但不会领取要求非空能力的任务。

批量选择先最大化领取数量；数量相同时选择升序任务 ID 列表字典序最小的分配；任务集合仍相同时，按任务 ID 升序排列对应槽 ID，选择该槽 ID 列表字典序最小的分配。结果不依赖输入字典的插入顺序。

整批选择和租约更新在同一个 `BEGIN IMMEDIATE` 事务中提交或回滚，因此两个打开同一 SQLite 文件的实例不会让同一条未到期任务同时产生两个生效凭据。

## 参数与兼容

以下参数不符合约定时抛出 `ValueError`，且不会写入任务或租约：

- 资源键、槽 ID 必须是 1–64 个 ASCII 字母、数字、下划线或连字符。
- `needs` 必须是 `frozenset[str]` 或 `None`；槽能力值必须是 `frozenset[str]`（无能力用空 `frozenset()`），标签必须来自上表。`workers` 本身可为 `None` 或 `{}`。
- 新入口 payload 必须是字符串或 `None`，UTF-8 编码后不超过 16 KiB。
- `now`、`lease` 必须是整数且不能是 `bool`；`now >= 0`、`lease > 0`、`now + lease <= 2**63 - 1`。
- `sinks` 必须是元组，元素为已登记名称；`capacity`、`limit`、`through` 的取值规则见上面的结论投递一节。
- 归档源的 `partitions` 必须是含 1–8 个不同合法名称的元组；归档 `offset` 为正 int64，`event_time`、封存 `through`/`watermark` 为非负 int64，均不能是 `bool`。`publish_ready(limit=...)` 的 `limit` 为 1–128；新 offset 相对当前 through 的超前宽度、以及单次 through 推进宽度均不得超过 12,000。

现有数据库可直接由新版 `Broker(path)` 打开。初始化会在事务内补齐 `resource_key`、`needs_mask` 列，并额外创建 `graph_workflows`、`graph_nodes`、`graph_dependencies` 表；若旧表的 `payload TEXT NOT NULL` 阻碍空 payload，会重建 `jobs` 表并原样复制 ID、payload、receipt、expires，同时保留 AUTOINCREMENT 序列高水位和早期库中的非整数到期值。归档源功能额外幂等创建 `archive_feed_sources`、`archive_events` 及就绪顺序部分索引，不改变或迁移任何旧任务。结论投递相关表（`sink_registry`、`sink_watermark`、`graph_node_sinks`、`outcome_stream`、`outcome_deliveries`）以 `IF NOT EXISTS` 补齐；旧版 `graph_nodes` 会新增 `outcomes` 列并以 `DEFAULT 1` 标记既有节点，因此升级不会补发旧终态结论、也不改写活跃租约。不需要清空或另建队列。

## 本地测试与演示

```sh
python3 -B -m unittest -v
python3 -B demo.py
```

测试全部可离线运行，覆盖：

- 最大领取数和两级字典序决胜；
- 资源键队首阻塞、到期边界重领、重开数据库后的新旧凭据；
- 原 `take`、无键任务和旧数据库结构兼容；
- 非法参数及失败批次无部分写入；
- 图提交校验、前向引用、显式/同键混合环、原子事务和工作流名复用限制；
- `requires`/`after` 满足条件、失败级联跳过、跳过不经过 `after` 传播、重开库恢复；
- 成功与失败凭据竞争、子进程失败退出后新进程继续投递清理节点；
- 两个文件型 `Broker` 实例的并发批量竞争；
- 旧库非整数到期值、AUTOINCREMENT 高水位迁移及图任务实际领取；
- 12,000 条未确认任务/24 槽规模，以及 12,000 节点图前置完成后的首批 24 条；
- 1,536 条 16 KiB payload 下，小批次领取和失败批量跳过都不会把大批 payload 装入 Python 内存（`tracemalloc` 峰值增量不超过 8 MiB）。
- 接收方登记、容量预留、`BufferError`/`ValueError` 优先级、竞赛最后一格和失败后工作流名/任务 ID 复用；
- 失败结论顺序、级联跳过的结论、失败不传播 `after`、多接收方相同序号、按提交顺序而非任务 ID 排序；
- 确认边界校验（跨接收方/未来序号拒绝）、按实际条数释放、并发重复确认、确认后流存储回收；
- 旧图库升级后不补发旧结论、原凭据与原依赖继续工作；
- 12,000 节点、32 接收方、失败触发全量跳过时的分页读取与 `tracemalloc` 内存上限，以及只发给乙的结论在大量甲流之后的低成本单页读取；
- 归档源登记幂等/改组拒绝、未登记分区拒绝、参数和 int64/布尔校验；
- 归档事件乱序暂存、未封重传/冲突、封内不比较重传、缺号和水位矛盾、重开恢复；
- 全分区最小水位、`(event_time, ASCII 分区名, offset)` 全局排序、同分区同时刻 offset 决胜；
- 已封前缀未全部发布时的 `BufferError`、只提升水位、worker 租约/确认与封存背压解耦、暂存回收；
- 两个独立 `Broker` 实例并发发布时无重复、无遗漏且 job ID/全局顺序一致；
- 归档任务沿用资源键 FIFO、GPU 能力阻塞、旧凭据/到期重领，并与同键工作流和 sink 结论共存；
- 8 分区、84,000 条积压（含 1,024 条 16 KiB payload）下，重开后三次 `limit=1` 发布只命中时刻 0、时刻 10 和空列表；每次调用的 `tracemalloc` 峰值增量不超过 6 MiB，每 1,000 条 VM 指令采样的 progress handler 回调不超过 150 次。

演示依次显示任务领取、重投、`stale ack: False`、`current ack: True` 和 `empty: True`。
