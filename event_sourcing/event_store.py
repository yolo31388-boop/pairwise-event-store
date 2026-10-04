"""
游戏内事件溯源与状态回放系统 - 核心模块

提供：
- 事件追加：乐观并发版本号控制、事务性批量追加（全成功或全失败）
- 事件存储：按聚合 ID 索引、事件载荷压缩
- 状态回放：基于快照的增量回放，无需从头开始
- 投影：事件发生后自动更新，支持损坏后重建
- 快照：定时/计数自动创建、旧快照自动清理
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
import threading
import time
import zlib


class ConcurrencyError(Exception):
    """期望版本与当前版本不一致时抛出（并发写入冲突）。"""


@dataclass
class Event:
    aggregate_id: str
    event_type: str
    payload: Dict[str, Any]
    version: int
    timestamp: float = 0.0
    event_id: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "aggregate_id": self.aggregate_id,
            "event_type": self.event_type,
            "payload": dict(self.payload),
            "version": self.version,
            "timestamp": self.timestamp,
        }


@dataclass
class Snapshot:
    aggregate_id: str
    version: int
    state: Dict[str, Any]
    timestamp: float = 0.0


@dataclass
class Config:
    snapshot_interval: int = 0
    snapshot_max_age: float = 0.0
    snapshot_max_keep: int = 3
    compression_threshold: int = 0
    clock: Optional[Callable[[], float]] = None


class _Projection:
    def __init__(self, name: str, handler: Callable[[Dict[str, Any], Event], None],
                 initial: Optional[Dict[str, Any]] = None):
        self.name = name
        self.handler = handler
        self.state: Dict[str, Any] = dict(initial) if initial else {}

    def apply(self, event: Event) -> None:
        self.handler(self.state, event)


class EventStore:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        if config is None:
            config = {}
        self.config = config
        if isinstance(config, Config):
            cfg = config
        else:
            cfg = Config(**{k: v for k, v in config.items() if k in Config.__dataclass_fields__})
        self._cfg = cfg

        self._lock = threading.RLock()
        self._events: List[Event] = []
        self._index: Dict[str, List[int]] = {}
        self._versions: Dict[str, int] = {}
        self._snapshots: Dict[str, List[Snapshot]] = {}
        self._handlers: List[Callable[[Event], None]] = []
        self._projections: Dict[str, _Projection] = {}
        self._compressed: set = set()
        self._next_event_id = 1
        self._last_snapshot_time: Dict[str, float] = {}
        self.reset()

    def _now(self) -> float:
        if self._cfg.clock is not None:
            return self._cfg.clock()
        return time.time()

    def reset(self):
        with self._lock:
            self._events = []
            self._index = {}
            self._versions = {}
            self._snapshots = {}
            self._compressed = set()
            self._next_event_id = 1
            self._last_snapshot_time = {}
            for projection in self._projections.values():
                projection.state = {}

    def update(self, dt: float):
        """驱动定时快照：清理超过保留时长的旧快照。"""
        if self._cfg.snapshot_max_age <= 0:
            return
        with self._lock:
            now = self._now()
            for aggregate_id, snaps in list(self._snapshots.items()):
                fresh = [s for s in snaps if now - s.timestamp < self._cfg.snapshot_max_age]
                if len(fresh) != len(snaps):
                    self._snapshots[aggregate_id] = fresh

    # ---------- 事件处理器 ----------

    def register_handler(self, handler: Callable[[Event], None]) -> None:
        self._handlers.append(handler)

    # ---------- 事件追加（版本号 + 事务） ----------

    def append_event(self, aggregate_id: str, event_type: str, payload: Optional[Dict[str, Any]] = None,
                     expected_version: Optional[int] = None) -> Event:
        events = self.append_events(
            aggregate_id,
            [(event_type, payload or {})],
            expected_version=expected_version,
        )
        return events[0]

    def append_events(self, aggregate_id: str, events_data: List,
                      expected_version: Optional[int] = None) -> List[Event]:
        """事务性批量追加：版本校验或任一处理失败则全部不生效。"""
        with self._lock:
            current = self._versions.get(aggregate_id, 0)
            if expected_version is not None and expected_version != current:
                raise ConcurrencyError(
                    f"aggregate {aggregate_id}: expected version {expected_version}, actual {current}"
                )

            prepared: List[Event] = []
            now = self._now()
            for offset, item in enumerate(events_data):
                if isinstance(item, Event):
                    event_type, event_payload = item.event_type, item.payload
                else:
                    event_type, event_payload = item
                event = Event(
                    aggregate_id=aggregate_id,
                    event_type=event_type,
                    payload=dict(event_payload),
                    version=current + offset + 1,
                    timestamp=now,
                    event_id=self._next_event_id + offset,
                )
                prepared.append(event)

            # 在写入前先试跑投影/处理器，保证事务“要么全成功要么全失败”
            trial_projections: Dict[str, Dict[str, Any]] = {}
            try:
                for projection in self._projections.values():
                    trial_state = {k: _copy(v) for k, v in projection.state.items()}
                    for event in prepared:
                        projection.handler(trial_state, event)
                    trial_projections[projection.name] = trial_state
            except Exception as exc:
                raise ValueError(f"projection rejected event batch: {exc}") from exc

            trial_handler_results = []
            for handler in self._handlers:
                for event in prepared:
                    trial_handler_results.append((handler, event))

            self._next_event_id += len(prepared)
            for event in prepared:
                position = len(self._events)
                self._events.append(event)
                self._index.setdefault(aggregate_id, []).append(position)
            self._versions[aggregate_id] = current + len(prepared)

            for projection in self._projections.values():
                projection.state = trial_projections[projection.name]
                projection.state["__projection_version__"] = prepared[-1].event_id

            for handler, event in trial_handler_results:
                handler(event)

            if self._cfg.snapshot_interval > 0:
                new_version = self._versions[aggregate_id]
                last_version = self._snapshots.get(aggregate_id, [None])[-1]
                last_v = last_version.version if last_version else 0
                if new_version - last_v >= self._cfg.snapshot_interval:
                    self.snapshot(aggregate_id)

            return prepared

    # ---------- 事件存储（索引 + 压缩） ----------

    def event_storage(self, aggregate_id: str) -> List[Event]:
        with self._lock:
            return [self._events[pos] for pos in self._index.get(aggregate_id, [])]

    get_events = event_storage

    def all_events(self) -> List[Event]:
        with self._lock:
            return list(self._events)

    def compress_events(self, aggregate_id: Optional[str] = None) -> int:
        """压缩历史事件载荷（zlib），返回被压缩的事件数。"""
        compressed_count = 0
        with self._lock:
            targets = [aggregate_id] if aggregate_id else list(self._index.keys())
            for agg in targets:
                for pos in self._index.get(agg, []):
                    event = self._events[pos]
                    if event.event_id in self._compressed:
                        continue
                    raw = _encode_payload(event.payload)
                    if self._cfg.compression_threshold and len(raw) < self._cfg.compression_threshold:
                        continue
                    event.payload = {"__compressed__": zlib.compress(raw)}
                    self._compressed.add(event.event_id)
                    compressed_count += 1
        return compressed_count

    def storage_stats(self) -> Dict[str, Any]:
        with self._lock:
            total = len(self._events)
            compressed = len(self._compressed)
            raw_bytes = 0
            stored_bytes = 0
            for event in self._events:
                size = len(_encode_payload(event.payload))
                stored_bytes += size
                raw_bytes += size
            # 压缩前原始大小无法还原时，用压缩事件解压后的大小估算节省
            saved = 0
            for event in self._events:
                if event.event_id in self._compressed:
                    blob = event.payload["__compressed__"]
                    original = len(zlib.decompress(blob))
                    saved += original - len(blob)
            return {
                "total_events": total,
                "compressed_events": compressed,
                "stored_bytes": stored_bytes,
                "bytes_saved": saved,
                "indexed_aggregates": len(self._index),
            }

    # ---------- 状态回放（快照 + 优化） ----------

    def register_event_type(self, event_type: str,
                            apply_fn: Callable[[Dict[str, Any], Event], None]) -> None:
        if not hasattr(self, "_apply_fns"):
            self._apply_fns = {}
        self._apply_fns[event_type] = apply_fn

    def _apply_event(self, state: Dict[str, Any], event: Event) -> None:
        payload = event.payload
        if isinstance(payload, dict) and "__compressed__" in payload:
            payload = _decode_payload(zlib.decompress(payload["__compressed__"]))
            restored = Event(event.aggregate_id, event.event_type, payload,
                             event.version, event.timestamp, event.event_id)
        else:
            restored = event
        apply_fn = getattr(self, "_apply_fns", {}).get(event.event_type)
        if apply_fn is not None:
            apply_fn(state, restored)
        else:
            state.setdefault("__events__", []).append(restored.to_dict())

    def state_replay(self, aggregate_id: str,
                     to_version: Optional[int] = None) -> Dict[str, Any]:
        """基于最近快照回放；压缩事件自动解压。"""
        with self._lock:
            base_state: Dict[str, Any] = {}
            start_version = 0
            for snap in self._snapshots.get(aggregate_id, []):
                if to_version is None or snap.version <= to_version:
                    base_state = {k: _copy(v) for k, v in snap.state.items()}
                    start_version = snap.version

            for event in self.event_storage(aggregate_id):
                if event.version <= start_version:
                    continue
                if to_version is not None and event.version > to_version:
                    break
                self._apply_event(base_state, event)
            base_state["__version__"] = min(to_version, self._versions.get(aggregate_id, 0)) \
                if to_version is not None else self._versions.get(aggregate_id, 0)
            return base_state

    replay = state_replay

    # ---------- 快照（定时 + 清理） ----------

    def snapshot(self, aggregate_id: str) -> Optional[Snapshot]:
        with self._lock:
            if aggregate_id not in self._versions:
                return None
            state = self.state_replay(aggregate_id)
            state.pop("__version__", None)
            snap = Snapshot(
                aggregate_id=aggregate_id,
                version=self._versions[aggregate_id],
                state=state,
                timestamp=self._now(),
            )
            snaps = self._snapshots.setdefault(aggregate_id, [])
            snaps.append(snap)
            self._last_snapshot_time[aggregate_id] = snap.timestamp
            if self._cfg.snapshot_max_keep > 0:
                self._cleanup_snapshots(aggregate_id)
            return snap

    def create_snapshot(self, aggregate_id: str) -> Optional[Snapshot]:
        return self.snapshot(aggregate_id)

    def _cleanup_snapshots(self, aggregate_id: str) -> None:
        snaps = self._snapshots.get(aggregate_id, [])
        keep = self._cfg.snapshot_max_keep
        if keep > 0 and len(snaps) > keep:
            self._snapshots[aggregate_id] = snaps[-keep:]

    def cleanup_snapshots(self) -> int:
        """删除所有聚合的过期/超额快照，返回删除数量。"""
        removed = 0
        with self._lock:
            now = self._now()
            for aggregate_id in list(self._snapshots.keys()):
                snaps = self._snapshots[aggregate_id]
                before = len(snaps)
                if self._cfg.snapshot_max_age > 0:
                    snaps = [s for s in snaps if now - s.timestamp < self._cfg.snapshot_max_age]
                if self._cfg.snapshot_max_keep > 0 and len(snaps) > self._cfg.snapshot_max_keep:
                    snaps = snaps[-self._cfg.snapshot_max_keep:]
                removed += before - len(snaps)
                self._snapshots[aggregate_id] = snaps
        return removed

    def get_snapshots(self, aggregate_id: str) -> List[Snapshot]:
        with self._lock:
            return list(self._snapshots.get(aggregate_id, []))

    # ---------- 投影（更新 + 重建） ----------

    def create_projection(self, name: str,
                          handler: Callable[[Dict[str, Any], Event], None],
                          initial: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        with self._lock:
            projection = _Projection(name, handler, initial)
            self._projections[name] = projection
            return projection.state

    def projection(self, name: str) -> Dict[str, Any]:
        with self._lock:
            if name not in self._projections:
                raise KeyError(f"projection {name!r} does not exist")
            return self._projections[name].state

    def update_projection(self, name: str) -> Dict[str, Any]:
        """将投影增量追到最新版本（幂等）。"""
        with self._lock:
            projection = self._projections[name]
            applied_to = projection.state.get("__projection_version__", 0)
            for event in self._events:
                if event.event_id <= applied_to:
                    continue
                projection.handler(projection.state, event)
                projection.state["__projection_version__"] = event.event_id
            return projection.state

    def rebuild_projection(self, name: str) -> Dict[str, Any]:
        """投影损坏后清空并从全部事件重建。"""
        with self._lock:
            projection = self._projections[name]
            projection.state = {}
            for event in self._events:
                projection.handler(projection.state, event)
                projection.state["__projection_version__"] = event.event_id
            return projection.state

    rebuild = rebuild_projection

    def version(self, aggregate_id: str) -> int:
        with self._lock:
            return self._versions.get(aggregate_id, 0)


def _encode_payload(payload: Any) -> bytes:
    if isinstance(payload, dict) and "__compressed__" in payload:
        return payload["__compressed__"]
    import json
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")


def _decode_payload(raw: bytes) -> Any:
    import json
    return json.loads(raw.decode("utf-8"))


def _copy(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _copy(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_copy(v) for v in value]
    return value
