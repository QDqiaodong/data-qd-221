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

现有数据库可直接由新版 `Broker(path)` 打开。初始化会在事务内补齐 `resource_key`、`needs_mask` 列，并额外创建 `graph_workflows`、`graph_nodes`、`graph_dependencies` 表；若旧表的 `payload TEXT NOT NULL` 阻碍空 payload，会重建 `jobs` 表并原样复制 ID、payload、receipt、expires，同时保留 AUTOINCREMENT 序列高水位和早期库中的非整数到期值。不需要清空或另建队列。

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

演示依次显示任务领取、重投、`stale ack: False`、`current ack: True` 和 `empty: True`。
