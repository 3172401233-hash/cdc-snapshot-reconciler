import json
import os
import shutil
import tempfile
import unittest

from reconciler import Reconciler


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            if isinstance(row, str):
                fh.write(row + "\n")
            else:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def read_jsonl(path):
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def snap(key, version, **data):
    return {"key": key, "version": version, "data": data}


def change(op, key, version, **data):
    rec = {"op": op, "key": key, "version": version}
    if op != "delete":
        rec["data"] = data
    return rec


class ReconcilerTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="recon-test-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.db = os.path.join(self.dir, "staging.db")
        self.snap_path = os.path.join(self.dir, "snapshot.jsonl")
        self.log_path = os.path.join(self.dir, "changelog.jsonl")
        self.out_path = os.path.join(self.dir, "final.jsonl")

    def run_reconcile(self, **kw):
        rec = Reconciler(self.db, **kw)
        try:
            if os.path.exists(self.snap_path):
                rec.load_snapshot(self.snap_path)
            if os.path.exists(self.log_path):
                rec.apply_changelog(self.log_path)
            rec.finalize(self.out_path)
            return rec.quarantine()
        finally:
            rec.close()

    def final_by_key(self):
        rows = read_jsonl(self.out_path)
        return {json.dumps(r["key"], sort_keys=True): r for r in rows}

    # 1. snapshot 晚到：旧版本不能覆盖已收到的更新版本
    def test_late_snapshot_does_not_overwrite_newer_update(self):
        write_jsonl(self.log_path, [
            change("update", {"id": 1, "region": "cn"}, 10, name="new"),
        ])
        self.run_reconcile()  # 先应用增量
        # snapshot 之后才导出/到达，携带更旧的版本
        write_jsonl(self.snap_path, [
            snap({"id": 1, "region": "cn"}, 5, name="old"),
            snap({"id": 2, "region": "cn"}, 5, name="only-in-snap"),
        ])
        self.run_reconcile()
        final = self.final_by_key()
        k1 = json.dumps({"id": 1, "region": "cn"}, sort_keys=True)
        k2 = json.dumps({"id": 2, "region": "cn"}, sort_keys=True)
        self.assertEqual(final[k1]["version"], 10)
        self.assertEqual(final[k1]["data"], {"name": "new"})
        self.assertEqual(final[k2]["version"], 5)

    # 2. update 重复投递：结果保持单条且版本不变
    def test_duplicate_updates_are_idempotent(self):
        write_jsonl(self.log_path, [
            change("update", {"id": 1}, 7, v="a"),
            change("update", {"id": 1}, 7, v="a"),
            change("update", {"id": 1}, 7, v="a"),
        ])
        self.run_reconcile()
        rows = read_jsonl(self.out_path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["version"], 7)

    # 3. delete 后显式重建允许恢复；旧 snapshot 不能复活墓碑
    def test_delete_tombstone_and_recreate(self):
        write_jsonl(self.log_path, [
            change("insert", {"id": 1}, 3, v="born"),
            change("delete", {"id": 1}, 8),
            change("insert", {"id": 1}, 9, v="reborn"),   # 显式重建
            change("insert", {"id": 2}, 2, v="born"),
            change("delete", {"id": 2}, 6),
        ])
        write_jsonl(self.snap_path, [
            snap({"id": 2}, 4, v="zombie"),  # 晚到的旧版本，不得复活
        ])
        self.run_reconcile()
        final = self.final_by_key()
        self.assertEqual(final['{"id": 1}']["data"], {"v": "reborn"})
        self.assertNotIn('{"id": 2}', final)

    # 4. 复合主键：字段顺序无关；不同键值互不干扰
    def test_composite_key_canonicalization_and_isolation(self):
        write_jsonl(self.log_path, [
            change("insert", {"a": 1, "b": 2}, 5, v="x"),
            # 同一逻辑主键，字段顺序不同，版本更高 -> 覆盖
            change("update", {"b": 2, "a": 1}, 6, v="y"),
            # 键值不同 -> 独立记录
            change("insert", {"a": 1, "b": 3}, 4, v="z"),
        ])
        self.run_reconcile()
        final = self.final_by_key()
        self.assertEqual(len(final), 2)
        k12 = json.dumps({"a": 1, "b": 2}, sort_keys=True)
        self.assertEqual(final[k12]["version"], 6)
        self.assertEqual(final[k12]["data"], {"v": "y"})

    # 5. 坏 change log：异常行进入 quarantine，其余照常处理
    def test_bad_changelog_records_go_to_quarantine(self):
        write_jsonl(self.log_path, [
            "{not valid json",
            {"op": "merge", "key": {"id": 1}, "version": 3, "data": {}},
            {"op": "update", "key": {"id": 1}},              # 缺 version
            {"op": "update", "key": {}, "version": 3, "data": {}},
            {"op": "update", "key": {"id": 9}, "version": 999,
             "data": {"v": "too-new"}},                      # 超出允许版本
            change("update", {"id": 1}, 4, v="good"),
        ])
        quarantine = self.run_reconcile(max_version=100)
        self.assertEqual(len(quarantine), 5)
        self.assertTrue(all(q[0] == "changelog" for q in quarantine))
        rows = read_jsonl(self.out_path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["data"], {"v": "good"})

    # 6. 事务中断：崩溃后从 checkpoint 续跑，结果与一次性跑完一致
    def test_crash_resume_from_checkpoint(self):
        log_rows = [change("update", {"id": i}, i + 1, v="v%d" % i)
                    for i in range(10)]
        log_rows.append(change("delete", {"id": 9}, 100))
        write_jsonl(self.log_path, log_rows)

        class CrashingReconciler(Reconciler):
            def __init__(self, *args, crash_after, **kwargs):
                super().__init__(*args, **kwargs)
                self._commits = 0
                self._crash_after = crash_after

            def _commit_batch(self, pending, source, line_no):
                super()._commit_batch(pending, source, line_no)
                self._commits += 1
                if self._commits >= self._crash_after:
                    raise RuntimeError("simulated crash")

        rec = CrashingReconciler(self.db, batch_size=1, crash_after=4)
        with self.assertRaises(RuntimeError):
            rec.apply_changelog(self.log_path)
        rec.close()
        # 崩溃时只提交了 4 批，checkpoint 必须停在 4
        probe = Reconciler(self.db, batch_size=1)
        self.assertEqual(probe.checkpoint("changelog"), 4)
        probe.close()

        # 重新运行：从 checkpoint 续跑直到完成
        rec = Reconciler(self.db, batch_size=1)
        rec.apply_changelog(self.log_path)
        rec.finalize(self.out_path)
        self.assertEqual(rec.checkpoint("changelog"), len(log_rows))
        rec.close()

        rows = read_jsonl(self.out_path)
        self.assertEqual(len(rows), 9)          # id=9 被删除
        self.assertNotIn({"id": 9}, [r["key"] for r in rows])
        versions = {r["key"]["id"]: r["version"] for r in rows}
        self.assertEqual(versions[0], 1)
        self.assertEqual(versions[8], 9)

        # 续跑幂等：再跑一遍结果不变
        rec = Reconciler(self.db, batch_size=1)
        rec.apply_changelog(self.log_path)
        rec.finalize(self.out_path)
        rec.close()
        self.assertEqual(len(read_jsonl(self.out_path)), 9)

    # 补充：窗口内乱序被最大版本规则纠正
    def test_out_of_order_within_window(self):
        write_jsonl(self.log_path, [
            change("update", {"id": 1}, 9, v="newest"),
            change("update", {"id": 1}, 3, v="stale"),
            change("update", {"id": 1}, 6, v="middle"),
        ])
        self.run_reconcile()
        rows = read_jsonl(self.out_path)
        self.assertEqual(rows[0]["version"], 9)
        self.assertEqual(rows[0]["data"], {"v": "newest"})


if __name__ == "__main__":
    unittest.main()
