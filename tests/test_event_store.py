"""
游戏内事件溯源与状态回放系统 - 验收测试
运行方式：python -m pytest tests/test_event_store.py -q
共 15 个测试用例
"""
import json
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from event_sourcing.event_store import EventStore, ConcurrencyError, ProjectionError


class TestEventStore:
    def setup_method(self):
        self.system = EventStore()

    # ---- 事件追加：记录历史 ----
    def test_case_01(self):
        self.system.append_event("player_move", {"x": 1, "y": 2})
        events = self.system.event_storage()
        assert len(events) == 1
        assert events[0]["type"] == "player_move"
        assert events[0]["data"] == {"x": 1, "y": 2}

    # ---- 事件追加：版本号递增 ----
    def test_case_02(self):
        e1 = self.system.append_event("a", {})
        e2 = self.system.append_event("b", {})
        assert e1["version"] == 1
        assert e2["version"] == 2
        assert self.system.version == 2

    # ---- 事件追加：乐观并发控制 ----
    def test_case_03(self):
        self.system.append_event("a", {})
        with pytest.raises(ConcurrencyError):
            self.system.append_event("b", {}, expected_version=99)
        self.system.append_event("b", {}, expected_version=1)
        assert self.system.version == 2

    # ---- 事件追加：事务，批量要么全成功要么全失败 ----
    def test_case_04(self):
        with pytest.raises(ValueError):
            self.system.append_events([
                {"type": "ok", "data": {"a": 1}},
                {"type": None, "data": {}},
            ])
        assert self.system.version == 0
        assert self.system.event_storage() == []
        result = self.system.append_events([
            {"type": "e1", "data": {}},
            {"type": "e2", "data": {}},
        ])
        assert [e["version"] for e in result] == [1, 2]

    # ---- 事件存储：完整历史而非只有当前状态 ----
    def test_case_05(self):
        for i in range(10):
            self.system.append_event("tick", {"n": i})
        events = self.system.event_storage()
        assert len(events) == 10
        assert [e["data"]["n"] for e in events] == list(range(10))

    # ---- 事件存储：类型索引，查询无需遍历全部 ----
    def test_case_06(self):
        for i in range(20):
            self.system.append_event("move", {"x": i})
            self.system.append_event("attack", {"dmg": i})
        moves = self.system.event_storage(event_type="move")
        assert len(moves) == 20
        assert all(e["type"] == "move" for e in moves)
        assert "attack" in self.system._index_by_type
        assert len(self.system._index_by_type["attack"]) == 20

    # ---- 事件存储：范围与限量查询 ----
    def test_case_07(self):
        for i in range(10):
            self.system.append_event("tick", {"n": i})
        assert len(self.system.event_storage(from_version=4, to_version=6)) == 3
        assert len(self.system.event_storage(limit=5)) == 5

    # ---- 事件存储：压缩节省空间 ----
    def test_case_08(self):
        big = {"log": "player moved to tile " * 20}
        for i in range(50):
            self.system.append_event("move", dict(big, x=i))
        raw = sum(len(json.dumps(dict(big, x=i))) for i in range(50))
        assert self.system.storage_size() < raw // 2
        # 压缩后读取结果仍正确
        assert self.system.event_storage(event_type="move")[0]["data"]["x"] == 0

    # ---- 状态回放：还原任意历史版本 ----
    def test_case_09(self):
        for i in range(10):
            self.system.append_event("set", {"score": i})
        assert self.system.state_replay() == {"score": 9}
        assert self.system.state_replay(up_to_version=5) == {"score": 4}
        assert self.system.state_replay(up_to_version=1) == {"score": 0}

    # ---- 状态回放：基于快照加速 ----
    def test_case_10(self):
        store = EventStore({"snapshot_interval": 10})
        for i in range(25):
            store.append_event("set", {"score": i})
        assert len(store.snapshots()) == 2  # 每 10 个事件自动快照
        assert store.state_replay() == {"score": 24}
        # 从快照回放与从头回放结果一致
        assert store.state_replay(23) == {"score": 22}

    # ---- 投影：注册并随事件自动更新 ----
    def test_case_11(self):
        self.system.projection(
            "kills", lambda acc, ev: (acc or 0) + ev["data"].get("n", 0), 0)
        self.system.append_event("kill", {"n": 1})
        self.system.append_event("kill", {"n": 2})
        assert self.system.projection("kills") == 3

    # ---- 投影：查询已注册投影无需遍历事件 ----
    def test_case_12(self):
        self.system.append_event("kill", {"n": 5})
        value = self.system.projection(
            "kills", lambda acc, ev: (acc or 0) + ev["data"].get("n", 0), 0)
        assert value == 5
        with pytest.raises(ProjectionError):
            self.system.projection("not_registered")

    # ---- 投影：损坏后可重建 ----
    def test_case_13(self):
        self.system.projection(
            "kills", lambda acc, ev: (acc or 0) + ev["data"].get("n", 0), 0)
        self.system.append_event("kill", {"n": 3})
        self.system._projections["kills"]["value"] = -999  # 模拟损坏
        assert self.system.rebuild_projection("kills") == 3

    # ---- 快照：手动创建 + 定时自动创建 ----
    def test_case_14(self):
        store = EventStore({"snapshot_interval": 0, "snapshot_interval_seconds": 60})
        store.append_event("a", {"v": 1})
        snap = store.snapshot()
        assert snap["version"] == 1
        assert snap["state"] == {"v": 1}
        store.append_event("a", {"v": 2})
        store.update(60)  # 累计时间到达间隔，自动快照
        assert store.snapshots()[-1]["version"] == 2
        store.update(120)  # 无新事件，不重复快照
        assert len(store.snapshots()) == 2

    # ---- 快照：超出上限自动清理最旧 ----
    def test_case_15(self):
        store = EventStore({"snapshot_interval": 0, "max_snapshots": 3})
        for i in range(5):
            store.append_event("a", {"v": i})
            store.snapshot()
        snaps = store.snapshots()
        assert len(snaps) == 3
        assert [s["version"] for s in snaps] == [3, 4, 5]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
