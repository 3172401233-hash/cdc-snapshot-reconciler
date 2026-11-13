# cdc-snapshot-reconciler
Python 全量与增量数据对账库，处理版本覆盖、删除墓碑、乱序变更和断点恢复

## 输入格式

snapshot JSONL，每行一条全量记录：

```json
{"key": {"id": 1, "region": "cn"}, "version": 12, "data": {"name": "alice"}}
```

change log JSONL，每行一条变更（`delete` 可省略 `data`）：

```json
{"op": "insert|update|delete", "key": {"id": 1, "region": "cn"}, "version": 13, "data": {...}}
```

## 用法

```python
from reconciler import reconcile

written, quarantined = reconcile(
    "snapshot.jsonl", "changelog.jsonl", "final.jsonl", "staging.db",
    min_version=0, max_version=10_000_000,  # 允许版本窗口，超窗进 quarantine
)
```

或使用 `Reconciler` 类分步执行（`load_snapshot` / `apply_changelog` /
`finalize`），任意时刻进程退出后重新运行即可从 checkpoint 续跑。

## 对账优先级

同一复合主键（key 字段按排序后 JSON 规范化，字段顺序无关）：

1. **版本最大者胜出**，与到达顺序无关 —— 窗口内乱序、重复投递均可容忍。
2. **delete 生成版本 V 的墓碑**：任何 `version <= V` 的记录（包括晚到的
   snapshot 旧版本）都不能复活该 key。
3. **显式重建允许恢复**：`version > V` 的 insert/update 可让被删 key 复活。
4. **版本相同视为重复**，先到者保留，后到者忽略（幂等）。
5. **异常数据进 quarantine**：版本超出允许窗口、JSON 损坏、缺字段、
   未知 op 的记录隔离存放，不影响其余数据处理。

## 暂存与恢复

所有中间状态存放在 SQLite（`staging` / `checkpoint` / `quarantine`
三张表）。每批数据写入与 checkpoint 推进在**同一个事务**里提交，
崩溃时未提交的批整体回滚，checkpoint 不会超前，因此续跑既不丢数据
也不会产生重复。

## 测试

```bash
python3 -m unittest -v
```

覆盖：snapshot 晚到、update 重复、delete 后重建、复合键冲突、
坏 change log、事务中断恢复、窗口内乱序。
