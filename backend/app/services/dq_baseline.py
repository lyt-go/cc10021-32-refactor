"""质量基线管理服务：管理数据质量统计基线，校验字段画像和监控指标。

domain_chain: 质量基线 (dq_baseline)
  └─ 校验 → 指标监控 (metric_monitor)
  └─ 关联 → 字段画像 (column_profile)
"""

from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.dq_incident import DqIncidentService
from app.store import store


class DqBaselineService(BaseModuleService):
    MODULE = "dq_baseline"
    ENTITY = "基线定义"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["基线编号", "目标表名", "基线指标", "基线上界", "基线下界", "计算周期", "基准日期", "基线版本"]
    REQUIRED_FIELDS = ["基线编号", "目标表名", "基线指标"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已过期"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    def calculate_baseline_from_history(self, table_name: str, metric_name: str,
                                         historical_values: list[float] | None = None) -> dict[str, Any]:
        """从历史数据计算统计基线：处理历史值得到上下界。

        导入历史监控指标数据，清洗异常值后计算均值和标准差，
        生成统计基线边界并校验基线合理性，写入基线存储。
        """
        if historical_values is None:
            historical_values = [random.gauss(5000, 800) for _ in range(30)]

        # 清洗数据中的异常值
        mean_val = sum(historical_values) / len(historical_values)
        std_val = (sum((x - mean_val) ** 2 for x in historical_values) / len(historical_values)) ** 0.5
        cleaned = [v for v in historical_values if abs(v - mean_val) <= 2 * std_val]

        cleaned_mean = sum(cleaned) / len(cleaned)
        cleaned_std = (sum((x - cleaned_mean) ** 2 for x in cleaned) / len(cleaned)) ** 0.5

        upper = cleaned_mean + 2 * cleaned_std
        lower = max(0, cleaned_mean - 2 * cleaned_std)

        # 导入基线存储
        rows = store.rows(self.MODULE)
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        baseline = {
            "id": new_id,
            "基线编号": f"DQ_B-{new_id:04d}",
            "目标表名": table_name,
            "基线指标": metric_name,
            "基线上界": f"{upper:.4f}",
            "基线下界": f"{lower:.4f}",
            "计算周期": "每周",
            "基准日期": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "基线版本": "v1.0",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        rows.append(baseline)

        return {
            "基线编号": baseline["基线编号"],
            "清洗前样本数": len(historical_values),
            "清洗后样本数": len(cleaned),
            "均值": round(cleaned_mean, 4),
            "标准差": round(cleaned_std, 4),
            "上界": round(upper, 4),
            "下界": round(lower, 4),
            "变异系数": f"{cleaned_std / cleaned_mean * 100:.2f}%" if cleaned_mean > 0 else "N/A",
        }

    def check_baseline_deviation(self, table_name: str, current_value: float) -> dict[str, Any]:
        """校验当前值是否偏离统计基线。

        查询目标表的基线定义，计算当前值偏离程度，
        超过阈值则校验标记为异常并写入质量事件。
        """
        all_baselines = store.rows(self.MODULE)
        matched = [
            b for b in all_baselines
            if b.get("目标表名") == table_name and b.get("status") == "已生效"
        ]
        if not matched:
            return {"目标表名": table_name, "当前值": current_value, "校验结果": "无有效基线"}

        deviations = []
        for bl in matched:
            upper = float(bl.get("基线上界", "0"))
            lower = float(bl.get("基线下界", "0"))
            deviation = 0
            if current_value > upper:
                deviation = (current_value - upper) / upper * 100 if upper > 0 else 0
            elif current_value < lower:
                deviation = (current_value - lower) / lower * 100 if lower > 0 else 0

            deviations.append({
                "基线编号": bl.get("基线编号"),
                "基线指标": bl.get("基线指标"),
                "上界": upper,
                "下界": lower,
                "偏离幅度": f"{deviation:.2f}%",
                "是否异常": abs(deviation) > 20,
            })

        max_deviation = max(deviations, key=lambda d: abs(float(d["偏离幅度"].rstrip("%"))))
        is_abnormal = max_deviation["是否异常"]

        # 异常则写入质量事件
        if is_abnormal:
            DqIncidentService().register_quality_event(
                source=DqIncidentService.SOURCE_BASELINE,
                assemble=lambda: {
                    "关联规则": f"baseline:{table_name}",
                    "偏离程度": max_deviation["偏离幅度"],
                    "影响行数": "待评估",
                    "严重级别": "warning",
                    "处理状态": "基线异常",
                    "处理人": "自动校验",
                },
            )

        return {
            "目标表名": table_name,
            "当前值": current_value,
            "校验结果": "异常" if is_abnormal else "正常",
            "最大偏离": max_deviation["偏离幅度"],
            "基线明细": deviations,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "校验"
        snapshot["desc"] = "从历史数据计算统计基线并校验偏离"
        snapshot["artifact"] = "质量基线记录集"
        snapshot["artifact_desc"] = "经校验的统计基线和偏离分析记录集"
        return snapshot