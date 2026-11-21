"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

import threading
from typing import Any

from app.seed import SEED_ROWS


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self._table_locks: dict[str, threading.Lock] = {}
        self._table_locks_guard = threading.Lock()
        self._review_lock = threading.Lock()
        self._review_checklist: list[dict[str, Any]] = []

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def lock_for(self, module: str) -> threading.Lock:
        """按模块取表级锁：同一模块内的取号与写入串行化。"""
        with self._table_locks_guard:
            return self._table_locks.setdefault(module, threading.Lock())

    def current_max_id(self, module: str) -> int:
        """读取模块当前最大 id（抢号读）。"""
        with self.lock_for(module):
            return max((int(row.get("id", 0)) for row in self.rows(module)), default=0)

    def append_if_base(self, module: str, entry: dict[str, Any], expected_base: int) -> bool:
        """仅当当前最大号仍是 expected_base 时原子写入；否则返回 False（抢号冲突）。

        取号（读最大号）与提交分离后，提交时必须重新校验：若期间已有其他
        写入抢先落库，本次提交作废，由调用方重新取号重试，保证编号唯一。
        """
        with self.lock_for(module):
            rows = self.rows(module)
            current = max((int(row.get("id", 0)) for row in rows), default=0)
            if current != expected_base:
                return False
            rows.append(entry)
            return True

    def snapshot(self, module: str) -> list[dict[str, Any]]:
        """在表级锁下拷贝模块当前行，供报告等读路径拿到一致视图。"""
        with self.lock_for(module):
            return [dict(row) for row in self.rows(module)]

    def append_review_item(self, item: dict[str, Any]) -> None:
        """回写质量报告的当班复核清单。"""
        with self._review_lock:
            self._review_checklist.append(dict(item))

    def review_checklist(self) -> list[dict[str, Any]]:
        """读取当前当班复核清单（拷贝）。"""
        with self._review_lock:
            return [dict(item) for item in self._review_checklist]

    def drain_review_checklist(self) -> list[dict[str, Any]]:
        """取走当前当班复核清单并重新开始新的一班。"""
        with self._review_lock:
            items = self._review_checklist
            self._review_checklist = []
            return [dict(item) for item in items]

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
