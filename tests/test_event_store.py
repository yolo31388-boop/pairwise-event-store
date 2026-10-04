"""
游戏内事件溯源与状态回放系统 - 验收测试
运行方式：python -m pytest tests/test_event_store.py -q
共 15 个测试用例
"""
import pytest
import sys
import os
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from event_sourcing.event_store import EventStore, ConcurrencyError


def apply_score(state, event):
    state["score"] = state.get("score", 0) + event.payload.get("points", 0)


class TestEventStore:
    def setup_method(self):
        self.system = EventStore()
        self.system.register_event_type("ScoreGained", apply_score)

    # 1. 事件追加：追加后可查询，事件带版本号
    def test_case_01(self):
        event = self.system.append_event("player-1", "ScoreGained", {"points": 10})
        assert event.version == 1
        events = self.system.event_storage("player-1")
        assert len(events) == 1
        assert events[0].payload["points"] == 10

    # 2. 版本号：按聚合单调递增
    def test_case_02(self):
        e1 = self.system.append_event("player-1", "ScoreGained", {"points": 1})
        e2 = self.system.append_event("player-1", "ScoreGained", {"points": 2})
        e3 = self.system.append_event("player-2", "ScoreGained", {"points": 3})
        assert (e1.version, e2.version) == (1, 2)
        assert e3.version == 1

    # 3. 乐观并发：期望版本不匹配时拒绝写入
    def test_case_03(self):
        self.system.append_event("player-1", "ScoreGained", {"points": 1})
        with pytest.raises(ConcurrencyError):
            self.system.append_event("player-1", "ScoreGained", {"points": 1}, expected_version=0)
        assert self.system.version("player-1") == 1
        ok = self.system.append_event("player-1", "ScoreGained", {"points": 1}, expected_version=1)
        assert ok.version == 2

    # 4. 事务：批量追加要么全成功要么全失败
    def test_case_04(self):
        def bad_projection(state, event):
            if event.payload.get("points", 0) < 0:
                raise ValueError("negative points not allowed")
            state["total"] = state.get("total", 0) + event.payload["points"]

        self.system.create_projection("score", bad_projection)
        with pytest.raises(ValueError):
            self.system.append_events("player-1", [
                ("ScoreGained", {"points": 5}),
                ("ScoreGained", {"points": -1}),
            ])
        assert self.system.event_storage("player-1") == []
        assert self.system.version("player-1") == 0
        assert self.system.projection("score").get("total", 0) == 0

    # 5. 并发写入：两个线程用相同期望版本竞争，只有一个成功
    def test_case_05(self):
        self.system.append_event("player-1", "ScoreGained", {"points": 1})
        results = []

        def writer():
            try:
                self.system.append_event("player-1", "ScoreGained", {"points": 1}, expected_version=1)
                results.append("ok")
            except ConcurrencyError:
                results.append("conflict")

        threads = [threading.Thread(target=writer) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert results.count("ok") == 1
        assert results.count("conflict") == 1
        assert self.system.version("player-1") == 2

    # 6. 存储索引：按聚合查询，不需要遍历其他聚合的事件
    def test_case_06(self):
        for i in range(50):
            self.system.append_event("player-1", "ScoreGained", {"points": 1})
        for i in range(3):
            self.system.append_event("player-2", "ScoreGained", {"points": 1})
        assert len(self.system.event_storage("player-2")) == 3
        assert len(self.system.event_storage("player-1")) == 50
        assert self.system.storage_stats()["indexed_aggregates"] == 2

    # 7. 存储压缩：历史事件压缩后占用空间变小
    def test_case_07(self):
        big_payload = {"log": "x" * 2000}
        for _ in range(5):
            self.system.append_event("player-1", "ScoreGained", big_payload)
        before = self.system.storage_stats()["stored_bytes"]
        compressed = self.system.compress_events("player-1")
        after = self.system.storage_stats()["stored_bytes"]
        assert compressed == 5
        assert after < before
        assert self.system.storage_stats()["bytes_saved"] > 0

    # 8. 状态回放：回放结果与事件累积一致
    def test_case_08(self):
        for points in (10, 20, 30):
            self.system.append_event("player-1", "ScoreGained", {"points": points})
        state = self.system.state_replay("player-1")
        assert state["score"] == 60
        assert state["__version__"] == 3

    # 9. 回放支持指定历史版本（回到 1 小时前的状态）
    def test_case_09(self):
        for points in (10, 20, 30):
            self.system.append_event("player-1", "ScoreGained", {"points": points})
        state = self.system.state_replay("player-1", to_version=2)
        assert state["score"] == 30
        assert state["__version__"] == 2

    # 10. 快照：回放基于最近快照，不从头开始
    def test_case_10(self):
        for _ in range(10):
            self.system.append_event("player-1", "ScoreGained", {"points": 1})
        snap = self.system.snapshot("player-1")
        assert snap.version == 10
        assert snap.state["score"] == 10
        for _ in range(5):
            self.system.append_event("player-1", "ScoreGained", {"points": 1})
        state = self.system.state_replay("player-1")
        assert state["score"] == 15

    # 11. 定时快照：达到间隔自动创建
    def test_case_11(self):
        store = EventStore({"snapshot_interval": 3})
        store.register_event_type("ScoreGained", apply_score)
        for _ in range(7):
            store.append_event("player-1", "ScoreGained", {"points": 1})
        snaps = store.get_snapshots("player-1")
        assert len(snaps) >= 2
        assert snaps[-1].version == 6

    # 12. 快照清理：旧快照自动清理，只保留最近 N 个
    def test_case_12(self):
        store = EventStore({"snapshot_max_keep": 2})
        store.register_event_type("ScoreGained", apply_score)
        for i in range(5):
            store.append_event("player-1", "ScoreGained", {"points": 1})
            store.snapshot("player-1")
        snaps = store.get_snapshots("player-1")
        assert len(snaps) == 2
        assert snaps[-1].version == 5
        assert snaps[0].version == 4

    # 13. 投影：事件发生后投影自动更新
    def test_case_13(self):
        def total_score(state, event):
            state["total"] = state.get("total", 0) + event.payload.get("points", 0)

        self.system.create_projection("total_score", total_score)
        self.system.append_event("player-1", "ScoreGained", {"points": 10})
        self.system.append_event("player-2", "ScoreGained", {"points": 5})
        assert self.system.projection("total_score")["total"] == 15

    # 14. 投影重建：投影损坏后可以从事件流重建
    def test_case_14(self):
        def total_score(state, event):
            state["total"] = state.get("total", 0) + event.payload.get("points", 0)

        self.system.create_projection("total_score", total_score)
        self.system.append_event("player-1", "ScoreGained", {"points": 10})
        self.system.append_event("player-1", "ScoreGained", {"points": 20})
        # 人为破坏投影
        self.system.projection("total_score")["total"] = -999
        rebuilt = self.system.rebuild_projection("total_score")
        assert rebuilt["total"] == 30

    # 15. 压缩事件仍可正确回放与重建投影
    def test_case_15(self):
        for points in (10, 20, 30):
            self.system.append_event("player-1", "ScoreGained", {"points": points, "note": "y" * 500})
        self.system.compress_events("player-1")
        state = self.system.state_replay("player-1")
        assert state["score"] == 60


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
