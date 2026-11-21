"""质量报告服务：聚合巡检和质量事件数据，生成多维度质量分析报告。

domain_chain: 质量报告 (dq_report)
  └─ 聚合 → 质量趋势 (dq_trend)
  └─ 导出 → 告警通知 (dq_alert_channel)
"""

from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DqReportService(BaseModuleService):
    MODULE = "dq_report"
    ENTITY = "质量报告"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["报告编号", "报告周期", "覆盖表数", "通过规则数", "失败规则数", "趋势分析", "生成时间", "报告版本"]
    REQUIRED_FIELDS = ["报告编号", "报告周期", "覆盖表数"]
    STATUS_ORDER = ["草稿", "生成中", "已完成", "已归档"]
    ACTION_RULES = {"生成报告": "生成中", "确认完成": "已完成", "归档报告": "已归档"}
    NEGATIVE_ACTIONS = []

    def aggregate_quality_report(self, period: str = "daily") -> dict[str, Any]:
        """聚合全量巡检结果和质量事件，生成多维度质量分析报告。

        查询巡检执行记录、质量事件、评分卡和趋势数据，
        聚合各维度指标计算结果，写入质量报告记录并导出趋势快照。
        """
        # 查询各模块数据（事件表取一致快照：并发登记期间报告读到的不是中间态）
        incident_rows = store.snapshot("dq_incident")
        check_rows = store.rows("dq_check_run")
        scorecard_rows = store.rows("dq_scorecard")
        trend_rows = store.rows("dq_trend")

        # 聚合巡检结果
        total_checks = len(check_rows)
        passed_checks = sum(1 for r in check_rows if not r.get("abnormal"))
        failed_checks = total_checks - passed_checks

        # 聚合质量事件
        total_incidents = len(incident_rows)
        open_incidents = sum(1 for r in incident_rows if r.get("status") in ("待确认", "调查中"))
        resolved_incidents = sum(1 for r in incident_rows if r.get("status") in ("已修复", "已关闭"))

        # 计算质量趋势
        quality_trends = [t for t in trend_rows]
        avg_quality = 0
        quality_values = []
        for t in quality_trends:
            try:
                quality_values.append(float(t.get("综合质量", "0")))
            except (ValueError, TypeError):
                pass
        if quality_values:
            avg_quality = sum(quality_values) / len(quality_values)

        # 写入质量报告记录（事件登记结果随当班复核清单归档到报告，可按来源回查）
        checklist = store.drain_review_checklist()
        rows = store.rows(self.MODULE)
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        report = {
            "id": new_id,
            "报告编号": f"DQ_R-{new_id:04d}",
            "报告周期": period,
            "覆盖表数": str(len(scorecard_rows)),
            "通过规则数": str(passed_checks),
            "失败规则数": str(failed_checks),
            "趋势分析": "上升" if avg_quality >= 85 else "平稳" if avg_quality >= 70 else "下降",
            "生成时间": datetime.now(timezone.utc).isoformat(),
            "报告版本": "v1.0",
            "当班复核清单": checklist,
            "status": "已完成",
            "pending": False,
            "abnormal": failed_checks > passed_checks,
        }
        rows.append(report)

        # 计算质量维度得分汇总
        dim_scores = self._compute_dimension_scores(scorecard_rows)

        return {
            "报告编号": report["报告编号"],
            "报告周期": period,
            "巡检总次数": total_checks,
            "巡检通过率": f"{passed_checks / total_checks * 100:.1f}%" if total_checks > 0 else "N/A",
            "质量事件总数": total_incidents,
            "未处理事件": open_incidents,
            "已修复事件": resolved_incidents,
            "修复率": f"{resolved_incidents / total_incidents * 100:.1f}%" if total_incidents > 0 else "N/A",
            "平均综合质量": round(avg_quality, 2),
            "维度得分": dim_scores,
            "生成时间": report["生成时间"],
            "趋势": report["趋势分析"],
            "当班复核清单": checklist,
        }

    def lookup_event_source(self, event_no: str) -> dict[str, Any] | None:
        """按事件编号回查真实来源：先查当班复核清单，再查历史报告归档的清单。

        巡检失败、指标波动等任一路径登记的事件，都能按事件编号回查到
        登记时的真实来源、源字段快照和重试次数。
        """
        for item in store.review_checklist():
            if item.get("事件编号") == event_no:
                return item
        for row in store.rows(self.MODULE):
            for item in row.get("当班复核清单", []):
                if item.get("事件编号") == event_no:
                    return item
        return None

    def _compute_dimension_scores(self, scorecard_rows: list[dict]) -> dict[str, float]:
        """计算质量维度得分汇总：完整性、准确性、时效性、唯一性。"""
        dims = {"完整性": [], "准确性": [], "时效性": []}
        for s in scorecard_rows:
            for dim_key, field in [("完整性", "完整性得分"), ("准确性", "准确性得分"), ("时效性", "时效性得分")]:
                try:
                    val = float(s.get(field, "0"))
                    dims[dim_key].append(val)
                except (ValueError, TypeError):
                    pass
        return {
            k: round(sum(v) / len(v), 1) if v else 0.0
            for k, v in dims.items()
        }

    def export_report_csv(self, report_id: int) -> str | None:
        """导出质量报告为CSV格式，包含所有维度数据和趋势标记。"""
        report = store.find(self.MODULE, report_id)
        if not report:
            return None

        lines = [
            "报告编号,报告周期,覆盖表数,通过规则数,失败规则数,趋势,生成时间,版本",
            f"{report.get('报告编号')},{report.get('报告周期')},{report.get('覆盖表数')},{report.get('通过规则数')},{report.get('失败规则数')},{report.get('趋势分析')},{report.get('生成时间')},{report.get('报告版本')}",
        ]
        return "\n".join(lines)

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "聚合"
        snapshot["desc"] = "聚合巡检和质量事件数据生成质量分析报告"
        snapshot["artifact"] = "质量报告数据集"
        snapshot["artifact_desc"] = "聚合多维度质量指标的质量报告数据集"
        return snapshot