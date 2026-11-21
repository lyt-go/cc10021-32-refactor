"""指标波动监控服务：查询指标当前值与基线对比，检测波动幅度并触发告警通知。

domain_chain: 指标波动检测 (metric_monitor)
  └─ 查询 → 质量事件 (dq_incident)
  └─ 关联 → 告警通知 (dq_alert_channel)
  └─ 校验 → 质量评分卡 (dq_scorecard)
"""

from __future__ import annotations

import random
import math
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.dq_incident import DqIncidentService
from app.store import store


class MetricMonitorService(BaseModuleService):
    MODULE = "metric_monitor"
    ENTITY = "监控指标"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["指标编号", "目标表名", "监控字段", "基线值", "当前值", "波动幅度", "告警阈值", "监控周期"]
    REQUIRED_FIELDS = ["指标编号", "目标表名", "监控字段"]
    STATUS_ORDER = ["草稿", "待校验", "监控中", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启动监控": "监控中", "停用监控": "已停用"}
    NEGATIVE_ACTIONS = ["停用监控"]

    def query_metric_values(self, table_name: str, metric_field: str,
                            baseline_value: float = 0) -> dict[str, Any]:
        """查询监控字段当前值并计算波动幅度。

        查询目标表中监控字段的当前统计值，与基线值对比计算波动幅度。
        若波动超过告警阈值，自动写入质量事件并触发告警通知。
        """
        # 查询当前值（模拟数据查询）
        current_value = baseline_value * random.uniform(0.5, 2.0) if baseline_value > 0 else random.uniform(100, 10000)
        if baseline_value == 0:
            baseline_value = current_value * 0.9

        volatility = abs(current_value - baseline_value) / baseline_value if baseline_value > 0 else 0
        volatility_pct = round(volatility * 100, 2)

        # 告警阈值判断
        thresholds = {
            "critical": 0.30,  # 30% 波动为严重
            "warning": 0.15,   # 15% 波动为告警
            "info": 0.05,      # 5% 波动为关注
        }
        alert_level = "normal"
        if volatility >= thresholds["critical"]:
            alert_level = "critical"
        elif volatility >= thresholds["warning"]:
            alert_level = "warning"
        elif volatility >= thresholds["info"]:
            alert_level = "info"

        result = {
            "目标表名": table_name,
            "监控字段": metric_field,
            "基线值": round(baseline_value, 4),
            "当前值": round(current_value, 4),
            "波动幅度": f"{volatility_pct}%",
            "告警级别": alert_level,
            "查询时间": datetime.now(timezone.utc).isoformat(),
        }

        # 若超过严重阈值：写入质量事件并触发告警
        if alert_level in ("critical", "warning"):
            self._create_quality_incident(table_name, metric_field, result)
            self._trigger_alert(table_name, metric_field, alert_level, volatility_pct)

        return result

    def _create_quality_incident(self, table_name: str, metric_field: str,
                                  metric_result: dict[str, Any]) -> None:
        """写入质量事件记录，将指标异常事件关联到质量事件管理。"""
        # 本条波动事实是独立事实，源字段闭包自持，不与其他来源串扰
        fact = {
            "关联规则": f"metric_monitor:{table_name}.{metric_field}",
            "偏离程度": metric_result["波动幅度"],
            "影响行数": str(random.randint(1000, 500000)),
            "严重级别": metric_result.get("告警级别", "warning"),
            "处理状态": "新建",
            "处理人": "auto",
        }
        DqIncidentService().register_quality_event(
            source=DqIncidentService.SOURCE_METRIC,
            assemble=lambda: dict(fact),
        )

    def _trigger_alert(self, table_name: str, field_name: str, level: str, volatility: float) -> None:
        """触发告警通知：将波动告警关联到告警通道配置，发起通知。"""
        alert_rows = store.rows("dq_alert_channel")
        new_aid = max((int(r.get("id", 0)) for r in alert_rows), default=0) + 1
        channel = "email" if level == "warning" else "phone+sms"
        alert_rows.append({
            "id": new_aid,
            "配置编号": f"DQ_A-{new_aid:04d}",
            "关联规则集": f"metric:{table_name}.{field_name}",
            "通知方式": channel,
            "接收人": "dq-admin",
            "告警级别": level,
            "静默时段": datetime.now(timezone.utc).isoformat(),
            "聚合策略": "立即",
            "通道状态": "已触发",
            "status": "已生效",
            "pending": False,
            "abnormal": level == "critical",
        })

    def aggregate_metric_trends(self, table_name: str, days: int = 7) -> list[dict[str, Any]]:
        """聚合多日指标查询结果，生成趋势分析数据。

        查询连续N天的监控指标，聚合波动趋势并关联到趋势看板。
        """
        trends = []
        base_value = random.uniform(1000, 50000)
        for day_offset in range(days, 0, -1):
            daily_value = base_value * random.uniform(0.7, 1.3)
            volatility = abs(daily_value - base_value) / base_value * 100
            trends.append({
                "日期": f"2026-11-{18 - day_offset:02d}",
                "表名": table_name,
                "指标值": round(daily_value, 2),
                "波动率": f"{volatility:.2f}%",
                "趋势": "上升" if daily_value > base_value else "下降",
            })

        # 聚合趋势写入趋势记录
        trend_rows = store.rows("dq_trend")
        for t in trends:
            new_tid = max((int(r.get("id", 0)) for r in trend_rows), default=0) + 1
            trend_rows.append({
                "id": new_tid,
                "记录编号": f"DQ_T-{new_tid:04d}",
                "目标表名": table_name,
                "日期": t["日期"],
                "完备率": f"{random.uniform(85, 100):.1f}%",
                "准确率": f"{random.uniform(80, 99):.1f}%",
                "及时率": f"{random.uniform(90, 100):.1f}%",
                "综合质量": f"{random.uniform(85, 98):.1f}",
                "趋势标记": t["趋势"],
                "status": "正常",
                "pending": False,
                "abnormal": False,
            })

        return trends

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "查询"
        snapshot["desc"] = "查询监控指标并对比基线检测波动"
        snapshot["artifact"] = "指标波动分析报告"
        snapshot["artifact_desc"] = "查询并聚合后的指标波动分析报告"
        return snapshot