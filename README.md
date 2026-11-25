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

现有数据库可直接由新版 `Broker(path)` 打开。初始化会在事务内补齐 `resource_key`、`needs_mask` 列；若旧表的 `payload TEXT NOT NULL` 阻碍空 payload，会重建表并原样复制 ID、payload、receipt、expires，同时保留 AUTOINCREMENT 序列高水位。不需要清空或另建队列。

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
- 两个文件型 `Broker` 实例的并发批量竞争；
- 12,000 条未确认任务/24 槽规模；
- 1,536 条 16 KiB payload 下，小批次不会把大批 payload 装入 Python 内存（`tracemalloc` 峰值增量不超过 8 MiB）。

演示依次显示任务领取、重投、`stale ack: False`、`current ack: True` 和 `empty: True`。
