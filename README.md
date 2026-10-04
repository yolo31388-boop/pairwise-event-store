# 游戏内事件溯源与状态回放系统

## 项目描述

游戏内事件溯源与状态回放系统的bug修复项目。核心模块 `event_sourcing/event_store.py` 存在多个已知bug，需要修复后使测试全部通过。

## 已修复问题

- 事件追加：完整记录历史，批量追加事务化（要么全成功要么全失败），带乐观并发版本号（`expected_version` 冲突抛 `ConcurrencyError`）
- 事件存储：按类型建索引查询无需遍历全部，事件负载 zlib 压缩，支持版本范围/限量查询
- 状态回放：可回放到任意历史版本，基于最近快照只回放增量事件
- 投影：事件追加时自动更新，损坏后可用 `rebuild_projection()` 从历史重建
- 快照：按事件数或 `update(dt)` 累计时间自动创建，`max_snapshots` 自动清理旧快照

## 主要 API

```python
store = EventStore({"snapshot_interval": 100,
                    "snapshot_interval_seconds": 0,
                    "max_snapshots": 5})

store.append_event("player_move", {"x": 1, "y": 2})      # 单事件，可传 expected_version
store.append_events([{"type": "a", "data": {}}])        # 事务批量追加
store.event_storage(event_type="player_move",
                    from_version=1, to_version=10)      # 索引查询
store.state_replay(up_to_version=5)                     # 快照加速回放
store.projection("kills", lambda acc, ev: acc + 1, 0)  # 注册/查询投影
store.rebuild_projection("kills")                       # 重建投影
store.snapshot()                                        # 手动快照（定时由 update(dt) 驱动）
```

## 运行测试

```bash
python -m pytest tests/test_event_store.py -q
```

修复目标：所有 15 个测试全部通过。

## 技术要求

- 纯Python标准库，不引入第三方依赖
- 不修改测试文件
- 保持现有API接口不变
