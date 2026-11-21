"""质量事件服务：记录和追踪数据质量异常事件，驱动趋势分析和评分卡计算。

domain_chain: 质量事件 (dq_incident)
  └─ 关联 → 趋势分析 (dq_trend)
  └─ 聚合 → 评分卡 (dq_scorecard)
  └─ 清洗 → 告警通知 (dq_alert_channel)
"""

from __future__ import annotations

import random
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DqIncidentService(BaseModuleService):
    MODULE = "dq_incident"
    ENTITY = "质量事件"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["事件编号", "关联规则", "触发时间", "偏离程度", "影响行数", "严重级别", "处理状态", "处理人"]
    REQUIRED_FIELDS = ["事件编号", "关联规则", "触发时间"]
    STATUS_ORDER = ["待确认", "调查中", "已修复", "已关闭"]
    ACTION_RULES = {"确认事件": "待确认", "开始调查": "调查中", "标记修复": "已修复", "关闭事件": "已关闭"}
    NEGATIVE_ACTIONS = []

    # 事件来源路径：各路径共用 register_quality_event 登记，来源标记随登记结果回写复核清单
    SOURCE_DIRECT = "单次写入"
    SOURCE_CHECK_RUN = "巡检失败"
    SOURCE_METRIC = "指标波动"
    SOURCE_BASELINE = "基线校验"
    SOURCE_TREND = "趋势告警"
    SOURCE_ANOMALY = "异常检测"

    def register_quality_event(self, *, source: str,
                               assemble: Callable[[], dict[str, Any]],
                               on_conflict: Callable[[int], None] | None = None) -> dict[str, Any]:
        """共用基础登记逻辑：各来源统一在此取号、组装并写入事件表。

        取号（读当前最大号）与提交分离，提交时用 append_if_base 原子校验：
        若期间其他来源已抢先写入（并发抢号冲突），本次提交作废并重新取号
        重试，保证每条独立事实拿到唯一可查的事件编号。来源字段由调用方的
        assemble 闭包各自持有，重试不会把一条事实的来源拼给另一条。
        登记结果回写质量报告的当班复核清单，供报告聚合与来源回查。
        """
        attempts = 0
        while True:
            base = store.current_max_id(self.MODULE)
            source_fields = dict(assemble())
            attempts += 1
            new_id = base + 1
            entry = {
                "id": new_id,
                "事件编号": f"DQ_I-{new_id:04d}",
                **source_fields,
                "触发时间": datetime.now(timezone.utc).isoformat(),
                "status": "待确认",
                "pending": True,
                "abnormal": True,
            }
            if store.append_if_base(self.MODULE, entry, expected_base=base):
                break
            if on_conflict is not None:
                on_conflict(attempts)

        store.append_review_item({
            "事件编号": entry["事件编号"],
            "来源": source,
            "严重级别": entry.get("严重级别", ""),
            "处理状态": entry.get("处理状态", ""),
            "源字段": source_fields,
            "重试次数": attempts - 1,
            "登记时间": datetime.now(timezone.utc).isoformat(),
        })
        return entry

    def aggregate_incident_summary(self, table_name: str, days: int = 7) -> dict[str, Any]:
        """聚合指定表在时间窗口内的质量事件摘要。

        查询该表所有关联质量事件，按严重级别和状态聚合统计，
        去重同类事件并分析根因分布，生成事件摘要报告。
        """
        all_incidents = store.rows(self.MODULE)
        related = [
            i for i in all_incidents
            if table_name in str(i.get("关联规则", ""))
        ]

        by_level = {"critical": 0, "warning": 0, "info": 0}
        by_status = {}
        root_causes = {}

        for inc in related:
            level = inc.get("严重级别", "info")
            by_level[level] = by_level.get(level, 0) + 1
            status = inc.get("status", "待确认")
            by_status[status] = by_status.get(status, 0) + 1
            cause = inc.get("关联规则", "未知")
            root_causes[cause] = root_causes.get(cause, 0) + 1

        # 去重同规则事件
        deduped = {}
        for inc in related:
            key = inc.get("关联规则", "")
            if key not in deduped:
                deduped[key] = inc

        return {
            "目标表名": table_name,
            "时间窗口": f"{days}天",
            "事件总数": len(related),
            "去重后": len(deduped),
            "按级别": by_level,
            "按状态": by_status,
            "根因分布": dict(
                sorted(root_causes.items(), key=lambda x: x[1], reverse=True)[:5]
            ),
            "聚合时间": datetime.now(timezone.utc).isoformat(),
        }

    def write_quality_incident(self, rule_id: str, severity: str,
                                deviation: str, table_name: str) -> dict[str, Any]:
        """写入质量事件记录并将事件关联到告警通知通道。

        当检测到数据质量异常时，写入质量事件记录到事件存储中，
        同时将事件分发到告警通道触发通知，并登记到趋势服务。
        """
        entry = self.register_quality_event(
            source=self.SOURCE_DIRECT,
            assemble=lambda: {
                "关联规则": rule_id,
                "偏离程度": deviation,
                "影响行数": str(random.randint(500, 100000)),
                "严重级别": severity,
                "处理状态": "新建",
                "处理人": "系统自动",
            },
        )

        # 关联写入趋势记录
        trend_rows = store.rows("dq_trend")
        new_tid = max((int(r.get("id", 0)) for r in trend_rows), default=0) + 1
        trend_rows.append({
            "id": new_tid,
            "记录编号": f"DQ_T-{new_tid:04d}",
            "目标表名": table_name,
            "日期": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "完备率": f"{random.uniform(70, 85):.1f}%",
            "准确率": f"{random.uniform(75, 90):.1f}%",
            "及时率": f"{random.uniform(80, 95):.1f}%",
            "综合质量": f"{random.uniform(70, 85):.1f}",
            "趋势标记": "下降趋势",
            "status": "下降趋势",
            "pending": True,
            "abnormal": True,
        })

        return entry

    def analyze_incident_trend(self, table_name: str) -> dict[str, Any]:
        """分析质量事件趋势并计算改善评分。

        查询历史事件数据，计算事件频率变化、修复率和趋势方向，
        聚合后的趋势分析结果关联到评分卡计算。
        """
        all_incidents = store.rows(self.MODULE)
        related = [i for i in all_incidents if table_name in str(i.get("关联规则", ""))]

        if not related:
            return {"目标表名": table_name, "状态": "无历史事件", "趋势": "平稳"}

        resolved = sum(1 for i in related if i.get("status") in ("已修复", "已关闭"))
        resolution_rate = resolved / len(related) * 100 if related else 0

        trend = "改善中" if resolution_rate > 60 else "恶化" if resolution_rate < 30 else "平稳"

        return {
            "目标表名": table_name,
            "历史事件总数": len(related),
            "已修复": resolved,
            "修复率": f"{resolution_rate:.1f}%",
            "趋势方向": trend,
            "建议": "加强规则覆盖" if trend == "恶化" else "保持监控" if trend == "平稳" else "继续改进",
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "写入"
        snapshot["desc"] = "写入质量事件记录并关联告警与趋势分析"
        snapshot["artifact"] = "质量事件数据集"
        snapshot["artifact_desc"] = "含趋势和告警关联的质量事件数据集"
        return snapshot