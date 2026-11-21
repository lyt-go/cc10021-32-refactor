"""业务测试：覆盖8条关键数据链路，验证数据工程操作正确性。"""
from __future__ import annotations

import pytest
import sys
import os
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.metadata_crawler import MetadataCrawlerService
from app.services.table_catalog import TableCatalogService
from app.services.column_profile import ColumnProfileService
from app.services.lineage_graph import LineageGraphService
from app.services.metric_monitor import MetricMonitorService
from app.services.dq_scorecard import DqScorecardService
from app.services.tag_classify import TagClassifyService
from app.services.dq_template import DqTemplateService
from app.services.dq_rule_def import DqRuleDefService
from app.services.dq_check_run import DqCheckRunService
from app.services.dq_incident import DqIncidentService
from app.services.dq_report import DqReportService
from app.services.anomaly_pattern import AnomalyPatternService
from app.services.metadata_diff import MetadataDiffService
from app.services.dq_alert_channel import DqAlertChannelService
from app.services.dq_baseline import DqBaselineService
from app.services.mdm_entity import MdmEntityService
from app.services.dq_trend import DqTrendService
from app.store import store


class TestMetadataCrawlerPipeline:
    """链路1: 元数据采集器→表目录注册→字段画像计算→分类标签"""

    def test_crawl_metadata_schema(self):
        svc = MetadataCrawlerService()
        schema = svc.crawl_metadata_schema("mysql", "localhost:3306/mydb", "mydb.*")
        assert schema["数据源类型"] == "mysql"
        assert "表清单" in schema
        assert len(schema["表清单"]) > 0
        for tbl in schema["表清单"]:
            assert "表名称" in tbl
            assert "字段列表" in tbl
            assert len(tbl["字段列表"]) > 0

    def test_ingest_metadata_batch(self):
        svc = MetadataCrawlerService()
        schema = svc.crawl_metadata_schema("mysql", "localhost:3306/mydb", "mydb.*")
        result = svc.ingest_metadata_batch(schema)
        assert result["总表数"] > 0
        assert "成功注册" in result

    def test_crawl_hive_schema(self):
        svc = MetadataCrawlerService()
        schema = svc.crawl_metadata_schema("hive", "hdfs://cluster/warehouse", "*.ads_*")
        assert schema["数据源类型"] == "hive"
        for tbl in schema["表清单"]:
            if tbl["表名称"].startswith("ads_"):
                assert tbl["分区字段"] == "dt"

    def test_register_table_with_columns(self):
        svc = TableCatalogService()
        columns = [
            {"字段名称": "id", "数据类型": "BIGINT", "可为空": False, "是否主键": True},
            {"字段名称": "name", "数据类型": "VARCHAR(255)", "可为空": True, "是否主键": False},
            {"字段名称": "amount", "数据类型": "DECIMAL(18,2)", "可为空": True, "是否主键": False},
            {"字段名称": "created_at", "数据类型": "TIMESTAMP", "可为空": False, "是否主键": False},
        ]
        result = svc.register_table_with_columns("ods_order_info", "mysql", columns, 8560000)
        assert result["注册表"] == "ods_order_info"
        assert result["校验问题"] >= 0

    def test_compute_column_statistics(self):
        svc = ColumnProfileService()
        columns = [
            {"字段名称": "id", "数据类型": "BIGINT"},
            {"字段名称": "amount", "数据类型": "DECIMAL(18,2)"},
            {"字段名称": "name", "数据类型": "VARCHAR(255)"},
        ]
        profiles = svc.compute_column_statistics("test_table", columns)
        assert len(profiles) == 3
        for p in profiles:
            assert "空值比例" in p
            assert "唯一值数" in p
            assert "质量评分" in p

    def test_aggregate_table_profile(self):
        svc = ColumnProfileService()
        profiles = svc.compute_column_statistics("test_table", [
            {"字段名称": "id", "数据类型": "BIGINT"},
            {"字段名称": "name", "数据类型": "VARCHAR(255)"},
        ])
        agg = svc.aggregate_table_profile("test_table", profiles)
        assert agg["画像字段数"] == 2
        assert "平均质量评分" in agg
        assert agg["基线已建立"] is True


class TestQualityRulePipeline:
    """链路2: 质量规则定义→巡检任务→质量事件→报告生成"""

    def test_validate_rule_completeness(self):
        svc = DqRuleDefService()
        result = svc.validate_rule_completeness(1)
        assert "校验结果" in result
        assert "问题数量" in result

    def test_compile_rule_to_sql(self):
        svc = DqRuleDefService()
        result = svc.compile_rule_to_sql(1)
        assert "编译SQL" in result
        assert "SELECT" in result["编译SQL"]

    def test_execute_rule_check(self):
        svc = DqRuleDefService()
        result = svc.execute_rule_check(1)
        assert "校验结果" in result
        assert result["校验结果"] in ("通过", "失败")

    def test_schedule_inspection(self):
        svc = DqCheckRunService()
        result = svc.schedule_inspection(
            "DQ_R-0001",
            "质量规则定义样例1,质量规则定义样例2,质量规则定义样例3",
            "0 6 * * *"
        )
        assert result["总规则数"] > 0
        assert "通过数" in result

    def test_write_quality_incident(self):
        svc = DqIncidentService()
        entry = svc.write_quality_incident("DQ_R-0001", "warning", "35%", "ods_order_info")
        assert entry.get("status") == "待确认"
        assert entry.get("abnormal") is not None

    def test_aggregate_quality_report(self):
        svc = DqReportService()
        report = svc.aggregate_quality_report("daily")
        assert "巡检总次数" in report or "巡检通过率" in report

    def test_export_report_csv(self):
        svc = DqReportService()
        # 先生成一份报告
        svc.aggregate_quality_report("daily")
        csv_data = svc.export_report_csv(1)
        if csv_data:
            assert "报告编号" in csv_data


class TestMetricMonitorPipeline:
    """链路3: 巡检任务→指标波动→告警通知"""

    def test_query_metric_values(self):
        svc = MetricMonitorService()
        result = svc.query_metric_values("ods_order_info", "amount", 5000)
        assert result["目标表名"] == "ods_order_info"
        assert "波动幅度" in result
        assert "告警级别" in result

    def test_query_critical_alert(self):
        svc = MetricMonitorService()
        result = svc.query_metric_values("dwd_trade_detail", "amount", 100)
        assert "告警级别" in result

    def test_aggregate_metric_trends(self):
        svc = MetricMonitorService()
        trends = svc.aggregate_metric_trends("ods_order_info", 7)
        assert len(trends) == 7
        for t in trends:
            assert "指标值" in t
            assert "趋势" in t

    def test_dispatch_alert(self):
        svc = DqAlertChannelService()
        result = svc.dispatch_alert("DQ_R-0001", "warning", "指标波动超过阈值30%", "DQ_I-0001")
        assert result["通知方式"] in ("email+sms", "phone+sms", "email")
        assert "接收人" in result

    def test_aggregate_alert_summary(self):
        svc = DqAlertChannelService()
        summary = svc.aggregate_alert_summary(7)
        assert "告警总数" in summary
        assert "按级别" in summary


class TestTrendScorecardPipeline:
    """链路4: 质量事件→趋势分析→评分卡"""

    def test_analyze_incident_trend(self):
        svc = DqIncidentService()
        trend = svc.analyze_incident_trend("ods_order_info")
        assert "趋势方向" in trend
        assert "修复率" in trend

    def test_calculate_quality_score(self):
        svc = DqScorecardService()
        result = svc.calculate_quality_score("ods_order_info")
        assert "综合得分" in result
        assert result["综合得分"] > 0
        assert "等级" in result

    def test_compute_quality_trend(self):
        svc = DqTrendService()
        trend = svc.compute_quality_trend("ods_order_info", 7)
        assert trend["目标表名"] == "ods_order_info"
        assert "趋势方向" in trend
        assert "最新质量" in trend

    def test_aggregate_trend_comparison(self):
        svc = DqTrendService()
        result = svc.aggregate_trend_comparison(["ods_order_info", "dwd_trade_detail"])
        assert "排名" in result
        assert len(result["排名"]) >= 1


class TestLineageDiffPipeline:
    """链路5: 元数据采集→变更对比→血缘图谱更新"""

    def test_parse_lineage_from_sql(self):
        svc = LineageGraphService()
        sql = "SELECT * FROM ods_order_info o JOIN dim_product_sku p ON o.sku_id=p.id WHERE o.dt='20260101'"
        lineages = svc.parse_lineage_from_sql(sql, "dws_daily_summary")
        assert len(lineages) > 0
        for lr in lineages:
            assert "上游表名" in lr
            assert lr["下游表名"] == "dws_daily_summary"

    def test_build_lineage_graph(self):
        svc = LineageGraphService()
        # 先构建一些血缘关系
        svc.parse_lineage_from_sql(
            "FROM ods_order_info JOIN dim_product_sku", "dws_daily_summary"
        )
        graph = svc.build_lineage_graph("dws_daily_summary")
        assert graph["中心表"] == "dws_daily_summary"
        assert "上游血缘" in graph

    def test_detect_lineage_breakage(self):
        svc = LineageGraphService()
        breakages = svc.detect_lineage_breakage("dws_daily_summary")
        assert isinstance(breakages, list)

    def test_compare_schemas(self):
        svc = MetadataDiffService()
        old_cols = [
            {"字段名称": "id", "数据类型": "BIGINT", "可为空": False},
            {"字段名称": "name", "数据类型": "VARCHAR(255)", "可为空": True},
            {"字段名称": "old_field", "数据类型": "INTEGER", "可为空": True},
        ]
        new_cols = [
            {"字段名称": "id", "数据类型": "BIGINT", "可为空": False},
            {"字段名称": "name", "数据类型": "TEXT", "可为空": True},
            {"字段名称": "new_field", "数据类型": "VARCHAR(64)", "可为空": False},
        ]
        diff = svc.compare_schemas(old_cols, new_cols, "test_table")
        assert diff["表名称"] == "test_table"
        assert diff["变更总数"] > 0
        assert "旧字段数" in diff
        assert "新增字段" in diff


class TestAnomalyPipeline:
    """链路6: 异常模式训练→检测→事件上报"""

    def test_train_anomaly_pattern(self):
        svc = AnomalyPatternService()
        result = svc.train_anomaly_pattern(1, "7d")
        assert "模型参数" in result
        assert "训练结果" in result

    def test_detect_anomalies(self):
        svc = AnomalyPatternService()
        # 先训练
        svc.train_anomaly_pattern(1, "7d")
        # 检测
        result = svc.detect_anomalies(1)
        assert "检测样本数" in result
        assert "异常点数" in result
        assert "异常率" in result

    def test_classify_tables_by_metadata(self):
        svc = TagClassifyService()
        result = svc.classify_tables_by_metadata(60)
        assert "覆盖表数" in result
        assert "分层统计" in result


class TestBaselinePipeline:
    """链路7: 基线计算→基线校验→事件"""

    def test_calculate_baseline_from_history(self):
        svc = DqBaselineService()
        result = svc.calculate_baseline_from_history("ods_order_info", "row_count")
        assert "上界" in result
        assert "下界" in result
        assert result["均值"] > 0

    def test_check_baseline_deviation(self):
        svc = DqBaselineService()
        # 先建立基线
        svc.calculate_baseline_from_history("ods_order_info", "row_count")
        # 校验偏离
        result = svc.check_baseline_deviation("ods_order_info", 8500)
        assert "校验结果" in result
        assert result["校验结果"] in ("正常", "异常", "无有效基线")


class TestMdmPipeline:
    """链路8: 主数据注册→关联血缘→规则标准化"""

    def test_register_mdm_entity(self):
        svc = MdmEntityService()
        result = svc.register_mdm_entity(
            "product_sku", "商品域",
            ["sku_id", "spu_id"],
            ["sku_id", "spu_id", "sku_name", "category", "price", "status", "created_at"],
            "ERP", "MDM团队"
        )
        assert result["实体名称"] == "product_sku"
        assert result["业务域"] == "商品域"

    def test_validate_entity_consistency(self):
        svc = MdmEntityService()
        # 先注册
        reg = svc.register_mdm_entity(
            "test_entity", "测试域",
            ["id"], ["id", "name", "value"],
            "TEST", "QA"
        )
        # 使用注册返回的实体编号查询
        entity_rows = store.rows("mdm_entity")
        entity_id = max(int(r.get("id", 0)) for r in entity_rows)
        result = svc.validate_entity_consistency(entity_id)
        assert "实体名称" in result


class TestTemplatePipeline:
    """模板标准化链路"""

    def test_standardize_rule_template(self):
        svc = DqTemplateService()
        result = svc.standardize_rule_template(
            "空值率检查模板",
            "空值检查",
            "SELECT COUNT(*) FROM {table} WHERE {column} IS NULL OR {column} = ''",
            ["table", "column"]
        )
        assert result["标准化结果"] == "成功"
        assert "校验详情" in result

    def test_instantiate_template(self):
        svc = DqTemplateService()
        result = svc.instantiate_template(1, {"table": "ods_order_info", "column": "amount"})
        assert "实例化SQL" in result


class TestDomainSnapshot:
    """测试所有服务的domain_snapshot数据工程操作"""

    def test_all_snapshots_have_operation(self):
        services = [
            MetadataCrawlerService(),
            TableCatalogService(),
            ColumnProfileService(),
            LineageGraphService(),
            MetricMonitorService(),
            DqScorecardService(),
            TagClassifyService(),
            DqTemplateService(),
            DqRuleDefService(),
            DqCheckRunService(),
            DqIncidentService(),
            DqReportService(),
            AnomalyPatternService(),
            MetadataDiffService(),
            DqAlertChannelService(),
            DqBaselineService(),
            MdmEntityService(),
            DqTrendService(),
        ]
        for svc in services:
            snapshot = svc.domain_snapshot()
            assert "operation" in snapshot, f"{svc.MODULE}缺少operation"
            assert "desc" in snapshot, f"{svc.MODULE}缺少desc"
            assert "artifact" in snapshot, f"{svc.MODULE}缺少artifact"

class TestIncidentRegistrationRefactor:
    """质量事件共用登记逻辑：并发抢号冲突、来源回查、日报当班复核清单"""

    def test_single_write_keeps_original_semantics(self):
        """普通单次写入：严重级别和处置状态沿用原口径"""
        svc = DqIncidentService()
        entry = svc.write_quality_incident("DQ_R-0001", "critical", "35%", "ods_order_info")
        assert entry["严重级别"] == "critical"
        assert entry["处理状态"] == "新建"
        assert entry["处理人"] == "系统自动"
        assert entry["status"] == "待确认"
        assert entry["事件编号"] == f"DQ_I-{entry['id']:04d}"

    def test_seed_incidents_preserved(self):
        """保留原事件记录：种子事件不被改写"""
        rows = store.rows("dq_incident")
        assert rows[0]["事件编号"] == "DQ_I-0001"
        assert rows[0]["关联规则"] == "质量事件管理样例1"
        assert rows[1]["事件编号"] == "DQ_I-0002"
        assert rows[2]["事件编号"] == "DQ_I-0003"

    def test_shared_registration_covers_all_paths(self):
        """巡检失败、指标波动、单次写入共用登记逻辑：编号唯一、口径各自保持"""
        store.drain_review_checklist()
        svc = DqIncidentService()
        base = store.current_max_id("dq_incident")

        DqCheckRunService()._write_quality_incidents("DQ_C-sim", [{
            "规则编号": "DQ_R-0101", "规则类型": "空值检查",
            "目标表名": "ods_order_info", "失败原因": "空值检查校验未通过",
        }])
        MetricMonitorService()._create_quality_incident(
            "ods_order_info", "amount", {"波动幅度": "31.5%", "告警级别": "critical"})
        svc.write_quality_incident("DQ_R-0001", "info", "5%", "ods_order_info")

        rows = store.rows("dq_incident")
        check_row, metric_row, direct_row = rows[base], rows[base + 1], rows[base + 2]
        # 每条独立事实一个可查编号，连续且互不重叠
        assert [r["id"] for r in (check_row, metric_row, direct_row)] == [base + 1, base + 2, base + 3]
        assert len({r["事件编号"] for r in (check_row, metric_row, direct_row)}) == 3
        # 各路径口径不变
        assert (check_row["严重级别"], check_row["处理状态"], check_row["处理人"]) == ("warning", "自动生成", "巡检系统")
        assert (metric_row["严重级别"], metric_row["处理状态"], metric_row["处理人"]) == ("critical", "新建", "auto")
        assert metric_row["关联规则"] == "metric_monitor:ods_order_info.amount"
        assert (direct_row["严重级别"], direct_row["处理状态"], direct_row["处理人"]) == ("info", "新建", "系统自动")
        # 登记结果回写当班复核清单
        checklist = store.review_checklist()
        assert [item["来源"] for item in checklist] == ["巡检失败", "指标波动", "单次写入"]
        assert all(item["重试次数"] == 0 for item in checklist)

    def test_concurrent_conflict_retry_and_report_traceback(self):
        """模拟：指标源字段未组装完时巡检先写入，双方读到同一最大号；
        冲突重试后各自编号可查、来源不串，生成日报后分别回查。"""
        store.drain_review_checklist()
        svc = DqIncidentService()
        base = store.current_max_id("dq_incident")
        rows_before = len(store.rows("dq_incident"))

        metric_read = threading.Event()
        inspection_committed = threading.Event()
        conflicts = []
        outcome = {}

        def metric_assemble():
            # 指标告警已读到当前最大号，但源字段还没组装完
            metric_read.set()
            inspection_committed.wait(5)
            return {
                "关联规则": "metric_monitor:dwd_trade_detail.amount",
                "偏离程度": "42.5%",
                "影响行数": "12345",
                "严重级别": "critical",
                "处理状态": "新建",
                "处理人": "auto",
            }

        def run_metric():
            outcome["metric"] = svc.register_quality_event(
                source=DqIncidentService.SOURCE_METRIC,
                assemble=metric_assemble,
                on_conflict=conflicts.append,
            )

        worker = threading.Thread(target=run_metric)
        worker.start()
        assert metric_read.wait(5)

        # 同一轮报告期间，报告正在取事件数据：读到的是一致快照，无中间态
        mid_snapshot = store.snapshot("dq_incident")
        assert max(int(r["id"]) for r in mid_snapshot) == base

        # 巡检失败读到同一个最大号，并先完成写入
        inspection_entry = svc.register_quality_event(
            source=DqIncidentService.SOURCE_CHECK_RUN,
            assemble=lambda: {
                "关联规则": "DQ_R-0102",
                "偏离程度": "100%",
                "影响行数": "待评估",
                "严重级别": "warning",
                "处理状态": "自动生成",
                "处理人": "巡检系统",
            },
        )
        inspection_committed.set()
        worker.join(5)
        assert not worker.is_alive()
        metric_entry = outcome["metric"]

        # 并发抢号冲突真实发生：指标第一次提交失败，重试后拿到新号
        assert conflicts == [1]
        assert len(store.rows("dq_incident")) == rows_before + 2
        # 每条独立事实一个可查编号
        assert inspection_entry["id"] == base + 1
        assert metric_entry["id"] == base + 2
        all_rows = store.rows("dq_incident")
        assert len({r["id"] for r in all_rows}) == len(all_rows)
        assert len({r["事件编号"] for r in all_rows}) == len(all_rows)
        # 来源不串：各自事实保持各自路径的字段
        assert inspection_entry["处理人"] == "巡检系统"
        assert metric_entry["关联规则"] == "metric_monitor:dwd_trade_detail.amount"
        assert metric_entry["严重级别"] == "critical"

        # 生成日报：聚合巡检与波动产生的事件记录
        report_svc = DqReportService()
        report = report_svc.aggregate_quality_report("daily")
        assert report["质量事件总数"] == len(store.snapshot("dq_incident"))
        checklist = report["当班复核清单"]
        by_no = {item["事件编号"]: item for item in checklist}
        # 保持原有级别含义
        assert by_no[inspection_entry["事件编号"]]["严重级别"] == "warning"
        assert by_no[metric_entry["事件编号"]]["严重级别"] == "critical"

        # 分别回查独立事件及失败的那次重试（清单已随日报归档，仍可回查）
        assert store.review_checklist() == []
        inspection_src = report_svc.lookup_event_source(inspection_entry["事件编号"])
        metric_src = report_svc.lookup_event_source(metric_entry["事件编号"])
        assert inspection_src["来源"] == "巡检失败"
        assert inspection_src["重试次数"] == 0
        assert metric_src["来源"] == "指标波动"
        assert metric_src["重试次数"] == 1
        assert metric_src["源字段"]["关联规则"] == "metric_monitor:dwd_trade_detail.amount"
        assert inspection_src["源字段"]["处理人"] == "巡检系统"
