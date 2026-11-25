# data-qd-221 · 持久任务投递

应用将待处理任务写入 SQLite；后台 worker 按 FIFO 获取当前可领取的任务，凭当次投递凭据确认。未确认任务到期后可重投，旧 worker 无法用过期凭据删除它。多个 worker 共用同一个数据库。计时由调用方注入，便于确定性验证；使用者需提供同一时间基准。当前提供单队列和至少一次投递，不承诺消费端恰好一次、延迟重试或死信策略。

Python 3.9+（兼容验收镜像的 3.11）；仅标准库。示例使用本仓合成输入和临时数据库。

```sh
python3 -B -m unittest -v
python3 -B demo.py
```

演示依次显示任务领取、重投、`stale ack: False`、`current ack: True` 和 `empty: True`。
