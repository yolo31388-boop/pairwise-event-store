"""
游戏内事件溯源与状态回放系统 - 核心模块

功能：
- append_event(): 事件追加，支持事务（批量追加要么全成功要么全失败）与乐观并发版本号
- event_storage(): 事件存储，带类型索引与 zlib 压缩，查询无需遍历全部事件
- state_replay(): 状态回放，基于最近快照加速，无需从开头回放
- projection(): 投影，事件追加时自动更新，支持损坏后重建
- snapshot(): 快照，支持按事件数/时间间隔自动创建与旧快照清理
"""
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
import copy
import json
import time
import zlib


class ConcurrencyError(Exception):
    """expected_version 与当前版本不一致时抛出（乐观并发控制）。"""


class ProjectionError(Exception):
    """投影相关错误（未注册、reducer 非法等）。"""


@dataclass
class Config:
    snapshot_interval: int = 100          # 每追加多少事件自动创建一次快照
    snapshot_interval_seconds: float = 0  # >0 时按累计时间自动创建快照（由 update(dt) 驱动）
    max_snapshots: int = 5                # 保留的最大快照数，超出自动清理最旧的
    compress: bool = True                 # 是否压缩存储事件负载


class EventStore:
    def __init__(self, config: Optional[Dict] = None):
        self.config = dict(config or {})
        self._cfg = Config(**{k: v for k, v in self.config.items()
                              if k in Config.__dataclass_fields__})
        # 事件历史（修复：不再只保存当前状态，完整历史都保留）
        self._storage: List[Dict] = []            # 压缩存储的事件记录
        self._index_by_type: Dict[str, List[int]] = {}  # 类型索引：event_type -> [version, ...]
        self._version = 0
        # 快照：version -> {"version": v, "state": ..., "timestamp": ...}
        self._snapshots: Dict[int, Dict] = {}
        self._snapshot_order: List[int] = []
        self._events_since_snapshot = 0
        self._snapshot_timer = 0.0
        # 投影：name -> {"reducer": fn, "initial": value, "value": current}
        self._projections: Dict[str, Dict] = {}

    # ------------------------------------------------------------------
    # 事件追加（事务 + 版本号）
    # ------------------------------------------------------------------
    @property
    def version(self) -> int:
        return self._version

    def append_event(self, event_type: str, data: Optional[Dict] = None,
                     expected_version: Optional[int] = None) -> Dict:
        """追加单个事件。expected_version 用于乐观并发控制。"""
        return self.append_events([{"type": event_type, "data": data or {}}],
                                  expected_version=expected_version)[0]

    def append_events(self, events: List[Dict],
                      expected_version: Optional[int] = None) -> List[Dict]:
        """事务性批量追加：所有事件先暂存校验，全部成功才提交，任一失败则整体回滚。"""
        if expected_version is not None and expected_version != self._version:
            raise ConcurrencyError(
                f"版本冲突：期望 {expected_version}，当前 {self._version}")

        # 暂存阶段：构造全部事件，任何一步出错都不会污染存储
        staged = []
        version = self._version
        now = time.time()
        for item in events:
            if isinstance(item, dict):
                event_type = item.get("type")
                data = item.get("data", {})
            else:
                event_type, data = item
            if not event_type:
                raise ValueError("事件缺少 type")
            version += 1
            staged.append({
                "version": version,
                "type": event_type,
                "data": copy.deepcopy(data),
                "timestamp": now,
            })

        # 提交阶段
        for event in staged:
            self._commit(event)
        return copy.deepcopy(staged)

    def _commit(self, event: Dict):
        payload = json.dumps(event["data"], ensure_ascii=False).encode("utf-8")
        record = {
            "version": event["version"],
            "type": event["type"],
            "timestamp": event["timestamp"],
            "payload": zlib.compress(payload) if self._cfg.compress else payload,
        }
        self._storage.append(record)
        self._index_by_type.setdefault(event["type"], []).append(event["version"])
        self._version = event["version"]
        self._events_since_snapshot += 1

        # 投影随事件追加自动更新
        for proj in self._projections.values():
            proj["value"] = proj["reducer"](proj["value"], event)

        # 按事件数自动快照
        if (self._cfg.snapshot_interval > 0
                and self._events_since_snapshot >= self._cfg.snapshot_interval):
            self.snapshot()

    # ------------------------------------------------------------------
    # 事件存储（索引 + 压缩）
    # ------------------------------------------------------------------
    def event_storage(self, event_type: Optional[str] = None,
                      from_version: Optional[int] = None,
                      to_version: Optional[int] = None,
                      limit: Optional[int] = None) -> List[Dict]:
        """查询事件。指定 event_type 时走索引，无需遍历全部事件。"""
        if event_type is not None:
            versions = self._index_by_type.get(event_type, [])
        else:
            versions = range(1, self._version + 1)

        result = []
        for v in versions:
            if from_version is not None and v < from_version:
                continue
            if to_version is not None and v > to_version:
                continue
            result.append(self._load(v))
            if limit is not None and len(result) >= limit:
                break
        return result

    def _load(self, version: int) -> Dict:
        record = self._storage[version - 1]
        payload = record["payload"]
        if self._cfg.compress:
            payload = zlib.decompress(payload)
        return {
            "version": record["version"],
            "type": record["type"],
            "data": json.loads(payload.decode("utf-8")),
            "timestamp": record["timestamp"],
        }

    def storage_size(self) -> int:
        """压缩后存储占用的字节数（不含索引）。"""
        return sum(len(r["payload"]) for r in self._storage)

    # ------------------------------------------------------------------
    # 状态回放（快照加速）
    # ------------------------------------------------------------------
    def state_replay(self, up_to_version: Optional[int] = None) -> Dict:
        """回放到指定版本的状态。从最近的不晚于目标版本的快照开始，只回放增量事件。"""
        target = self._version if up_to_version is None else up_to_version
        if target < 0 or target > self._version:
            raise ValueError(f"无法回放到版本 {target}（当前版本 {self._version}）")

        # 找最近可用快照
        base_version = 0
        state: Dict = {}
        for v in reversed(self._snapshot_order):
            if v <= target:
                base_version = v
                state = copy.deepcopy(self._snapshots[v]["state"])
                break

        for v in range(base_version + 1, target + 1):
            self._apply(state, self._load(v))
        return state

    @staticmethod
    def _apply(state: Dict, event: Dict):
        data = event["data"]
        if isinstance(data, dict):
            for key, value in data.items():
                if value is None and key in state:
                    del state[key]
                else:
                    state[key] = value

    # ------------------------------------------------------------------
    # 投影（自动更新 + 可重建）
    # ------------------------------------------------------------------
    def projection(self, name: str,
                   reducer: Optional[Callable[[Any, Dict], Any]] = None,
                   initial: Any = None) -> Any:
        """注册或查询投影。reducer(state, event) -> new_state，追加事件时自动更新。"""
        if name not in self._projections:
            if reducer is None:
                raise ProjectionError(f"投影 '{name}' 未注册")
            self._projections[name] = {
                "reducer": reducer,
                "initial": initial,
                "value": self._replay_projection(reducer, initial),
            }
        return copy.deepcopy(self._projections[name]["value"])

    def rebuild_projection(self, name: str) -> Any:
        """投影损坏后从事件历史重建。"""
        if name not in self._projections:
            raise ProjectionError(f"投影 '{name}' 未注册")
        proj = self._projections[name]
        proj["value"] = self._replay_projection(proj["reducer"], proj["initial"])
        return copy.deepcopy(proj["value"])

    def _replay_projection(self, reducer, initial):
        value = copy.deepcopy(initial)
        for v in range(1, self._version + 1):
            value = reducer(value, self._load(v))
        return value

    # ------------------------------------------------------------------
    # 快照（定时创建 + 自动清理）
    # ------------------------------------------------------------------
    def snapshot(self) -> Dict:
        """在当前版本创建快照，并按 max_snapshots 清理最旧的快照。"""
        snap = {
            "version": self._version,
            "state": self.state_replay(self._version),
            "timestamp": time.time(),
        }
        if self._version in self._snapshots:
            self._snapshot_order.remove(self._version)
        self._snapshots[self._version] = snap
        self._snapshot_order.append(self._version)
        self._events_since_snapshot = 0
        self._snapshot_timer = 0.0

        # 清理旧快照
        while len(self._snapshot_order) > self._cfg.max_snapshots:
            oldest = self._snapshot_order.pop(0)
            del self._snapshots[oldest]
        return copy.deepcopy(snap)

    def snapshots(self) -> List[Dict]:
        return [copy.deepcopy(self._snapshots[v]) for v in self._snapshot_order]

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def update(self, dt: float):
        """按累计时间自动创建快照（snapshot_interval_seconds > 0 时生效）。"""
        interval = self._cfg.snapshot_interval_seconds
        if interval <= 0:
            return
        self._snapshot_timer += dt
        if self._snapshot_timer >= interval and self._events_since_snapshot > 0:
            self.snapshot()

    def reset(self):
        self._storage = []
        self._index_by_type = {}
        self._version = 0
        self._snapshots = {}
        self._snapshot_order = []
        self._events_since_snapshot = 0
        self._snapshot_timer = 0.0
        self._projections = {}
