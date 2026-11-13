"""CDC snapshot + change log 离线对账库。

输入
----
snapshot JSONL，每行::

    {"key": {...复合主键...}, "version": 12, "data": {...}}

change log JSONL，每行::

    {"op": "insert"|"update"|"delete",
     "key": {...}, "version": 13, "data": {...}}   # delete 可省略 data

对账优先级（同一复合主键）
--------------------------
1. 版本号最大者胜出，与到达顺序无关（窗口内乱序、重复均可容忍）。
2. delete 生成版本为 V 的墓碑：任何 version <= V 的记录（包括晚到的
   snapshot 旧版本）都不能复活该 key。
3. version > V 的显式 insert/update 允许重建该 key。
4. 版本相同视为重复记录，先到的保留，后到的忽略。
5. 版本超出允许窗口 [min_version, max_version]、JSON 损坏、缺字段、
   未知 op 的记录进入 quarantine，不影响其余数据处理。

暂存与恢复
----------
所有中间状态存放在 SQLite。每批数据与 checkpoint（已处理的行偏移）
在同一个事务里提交；进程在任意时刻退出后，重新运行会从上次提交的
checkpoint 继续，不会产生重复或丢失。
"""

import json
import os
import sqlite3

_VALID_OPS = ("insert", "update", "delete")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS staging (
    k        TEXT PRIMARY KEY,   -- 复合主键的规范化 JSON
    version  INTEGER NOT NULL,
    op       TEXT NOT NULL,      -- 当前胜出记录的操作类型
    data     TEXT                -- 记录负载 JSON；墓碑为 NULL
);
CREATE TABLE IF NOT EXISTS checkpoint (
    source   TEXT PRIMARY KEY,   -- 'snapshot' / 'changelog'
    offset   INTEGER NOT NULL    -- 已提交处理的行数
);
CREATE TABLE IF NOT EXISTS quarantine (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    source   TEXT NOT NULL,
    line_no  INTEGER NOT NULL,
    raw      TEXT NOT NULL,
    reason   TEXT NOT NULL
);
"""

_UPSERT = """
INSERT INTO staging (k, version, op, data) VALUES (?, ?, ?, ?)
ON CONFLICT(k) DO UPDATE SET
    version = excluded.version,
    op      = excluded.op,
    data    = excluded.data
WHERE excluded.version > staging.version
"""


def canonical_key(key):
    """把复合主键规范化为稳定字符串（字段序无关）。"""
    if not isinstance(key, dict) or not key:
        raise ValueError("key must be a non-empty object")
    return json.dumps(key, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


class Reconciler:
    """把 snapshot 与 change log 合并为最终 JSONL。"""

    def __init__(self, db_path, min_version=None, max_version=None,
                 batch_size=500):
        self.db_path = db_path
        self.min_version = min_version
        self.max_version = max_version
        self.batch_size = batch_size
        self.conn = sqlite3.connect(db_path)
        self.conn.executescript(_SCHEMA)
        self.conn.commit()

    # -- 记录校验 ---------------------------------------------------------

    def _validate(self, obj, require_data):
        if not isinstance(obj, dict):
            raise ValueError("record is not a JSON object")
        key = canonical_key(obj.get("key"))
        version = obj.get("version")
        if not isinstance(version, int) or isinstance(version, bool):
            raise ValueError("version must be an integer")
        if self.min_version is not None and version < self.min_version:
            raise ValueError("version %d below min_version %d"
                             % (version, self.min_version))
        if self.max_version is not None and version > self.max_version:
            raise ValueError("version %d above max_version %d"
                             % (version, self.max_version))
        data = obj.get("data")
        if require_data and data is None:
            raise ValueError("missing data")
        return key, version, data

    # -- 通用流式处理：分批，每批与 checkpoint 同事务提交 ------------------

    def _process_file(self, path, source, parse):
        cur = self.conn.execute(
            "SELECT offset FROM checkpoint WHERE source = ?", (source,))
        row = cur.fetchone()
        offset = row[0] if row else 0

        with open(path, "r", encoding="utf-8") as fh:
            for _ in range(offset):
                if not fh.readline():
                    break  # 文件比 checkpoint 短，没有可读内容
            pending = []   # (kind, payload)，kind: 'rec' | 'quar'
            line_no = offset
            for line in fh:
                line_no += 1
                raw = line.rstrip("\n")
                if not raw.strip():
                    self._commit_batch(pending, source, line_no)
                    pending = []
                    continue
                try:
                    pending.append(("rec", parse(json.loads(raw))))
                except (ValueError, KeyError, TypeError) as exc:
                    pending.append(("quar", (source, line_no, raw, str(exc))))
                if len(pending) >= self.batch_size:
                    self._commit_batch(pending, source, line_no)
                    pending = []
            if pending or line_no > offset:
                self._commit_batch(pending, source, line_no)

    def _commit_batch(self, pending, source, line_no):
        """一个事务里：写 staging/quarantine + 推进 checkpoint。"""
        with self.conn:  # 异常时整体回滚，checkpoint 不会超前
            for kind, payload in pending:
                if kind == "rec":
                    self.conn.execute(_UPSERT, payload)
                else:
                    self.conn.execute(
                        "INSERT INTO quarantine (source, line_no, raw, reason)"
                        " VALUES (?, ?, ?, ?)", payload)
            self.conn.execute(
                "INSERT INTO checkpoint (source, offset) VALUES (?, ?)"
                " ON CONFLICT(source) DO UPDATE SET offset = excluded.offset",
                (source, line_no))

    # -- 公开 API ---------------------------------------------------------

    def load_snapshot(self, path):
        """加载全量快照（幂等，可断点续跑）。"""
        def parse(obj):
            key, version, data = self._validate(obj, require_data=True)
            return (key, version, "snapshot",
                    json.dumps(data, ensure_ascii=False))
        self._process_file(path, "snapshot", parse)

    def apply_changelog(self, path):
        """应用增量变更日志（幂等，可断点续跑）。"""
        def parse(obj):
            if not isinstance(obj, dict):
                raise ValueError("record is not a JSON object")
            op = obj.get("op")
            if op not in _VALID_OPS:
                raise ValueError("unknown op %r" % (op,))
            key, version, data = self._validate(
                obj, require_data=(op != "delete"))
            payload = None if op == "delete" else json.dumps(
                data, ensure_ascii=False)
            return (key, version, op, payload)
        self._process_file(path, "changelog", parse)

    def finalize(self, out_path):
        """导出最终 JSONL（墓碑不输出），按主键排序，返回存活行数。"""
        count = 0
        tmp = out_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as out:
            for k, version, data in self.conn.execute(
                    "SELECT k, version, data FROM staging"
                    " WHERE op != 'delete' ORDER BY k"):
                out.write(json.dumps(
                    {"key": json.loads(k), "version": version,
                     "data": json.loads(data)},
                    ensure_ascii=False, sort_keys=True) + "\n")
                count += 1
        os.replace(tmp, out_path)
        return count

    def quarantine(self):
        """返回隔离区记录列表 [(source, line_no, raw, reason), ...]。"""
        return list(self.conn.execute(
            "SELECT source, line_no, raw, reason FROM quarantine"
            " ORDER BY id"))

    def checkpoint(self, source):
        row = self.conn.execute(
            "SELECT offset FROM checkpoint WHERE source = ?",
            (source,)).fetchone()
        return row[0] if row else 0

    def close(self):
        self.conn.close()


def reconcile(snapshot_path, changelog_path, out_path, db_path,
              min_version=None, max_version=None, batch_size=500):
    """一步到位：建库 -> 加载快照 -> 应用变更 -> 导出最终结果。"""
    rec = Reconciler(db_path, min_version=min_version,
                     max_version=max_version, batch_size=batch_size)
    try:
        if snapshot_path:
            rec.load_snapshot(snapshot_path)
        if changelog_path:
            rec.apply_changelog(changelog_path)
        written = rec.finalize(out_path)
        return written, rec.quarantine()
    finally:
        rec.close()
