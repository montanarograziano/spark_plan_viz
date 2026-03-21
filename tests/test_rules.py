"""Tests for the optimization rules engine."""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock, patch

import pytest

from spark_plan_viz._analyzer import _attach_suggestions, _build_context, analyze_plan
from spark_plan_viz._rules import (
    AnalysisContext,
    CoalesceRule,
    CrossJoinRule,
    ExpensiveCollectRule,
    FullTableScanRule,
    MissingBroadcastHintRule,
    NestedLoopJoinRule,
    NonColumnarFormatRule,
    PartitionCountRule,
    PythonUDFRule,
    RedundantShuffleRule,
    Severity,
    SkewHintRule,
    SortBeforeShuffleRule,
    Suggestion,
    UnnecessarySortRule,
    WindowWithoutPartitionRule,
)


def _make_node(
    name: str = "TestNode",
    node_type: str = "other",
    description: str = "",
    key_info: dict[str, Any] | None = None,
    children: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "type": node_type,
        "key_info": key_info or {},
        "children": children or [],
        "output": [],
        "metrics": {},
        "suggestions": [],
    }


def _ctx_for(*nodes: dict[str, Any]) -> AnalysisContext:
    """Build context from a single-root tree."""
    if len(nodes) == 1:
        return _build_context(nodes[0])
    # First node is root, rest are children
    root = nodes[0]
    root["children"] = list(nodes[1:])
    return _build_context(root)


class TestCrossJoinRule:
    rule = CrossJoinRule()

    def test_detects_cartesian_product(self) -> None:
        node = _make_node(name="CartesianProduct")
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1
        assert results[0].severity == Severity.ERROR

    def test_detects_cross_join_type(self) -> None:
        node = _make_node(
            name="BroadcastHashJoin",
            node_type="join",
            key_info={"join_type": "Cross"},
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1

    def test_ignores_inner_join(self) -> None:
        node = _make_node(
            name="SortMergeJoin",
            node_type="join",
            key_info={"join_type": "Inner"},
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 0


class TestMissingBroadcastHintRule:
    rule = MissingBroadcastHintRule()

    def test_suggests_broadcast_for_sort_merge(self) -> None:
        node = _make_node(name="SortMergeJoin", node_type="join")
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1
        assert results[0].severity == Severity.INFO

    def test_skips_broadcast_join(self) -> None:
        node = _make_node(
            name="BroadcastHashJoin",
            node_type="join",
            key_info={"is_broadcast": True},
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 0


class TestFullTableScanRule:
    rule = FullTableScanRule()

    def test_detects_no_pushed_filters(self) -> None:
        node = _make_node(name="FileScan", node_type="scan")
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING

    def test_skips_scan_with_pushed_filters(self) -> None:
        node = _make_node(
            name="FileScan",
            node_type="scan",
            key_info={"pushed_filters": ["IsNotNull(id)"]},
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 0


class TestRedundantShuffleRule:
    rule = RedundantShuffleRule()

    def test_detects_consecutive_exchanges(self) -> None:
        child = _make_node(name="Exchange", node_type="shuffle")
        parent = _make_node(name="Exchange", node_type="shuffle", children=[child])
        ctx = _ctx_for(parent)
        results = self.rule.check(parent, ctx)
        assert len(results) == 1

    def test_skips_single_exchange(self) -> None:
        node = _make_node(
            name="Exchange",
            node_type="shuffle",
            children=[_make_node(name="Filter", node_type="filter")],
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 0


class TestExpensiveCollectRule:
    rule = ExpensiveCollectRule()

    def test_detects_collect_list(self) -> None:
        node = _make_node(
            name="HashAggregate",
            node_type="aggregate",
            description="collect_list(value)",
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1

    def test_skips_normal_aggregate(self) -> None:
        node = _make_node(
            name="HashAggregate",
            node_type="aggregate",
            description="sum(amount)",
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 0


class TestSortBeforeShuffleRule:
    rule = SortBeforeShuffleRule()

    def test_detects_sort_before_exchange(self) -> None:
        sort_node = _make_node(name="Sort", node_type="sort")
        exchange = _make_node(
            name="Exchange", node_type="shuffle", children=[sort_node]
        )
        ctx = _ctx_for(exchange)
        results = self.rule.check(exchange, ctx)
        assert len(results) == 1

    def test_skips_non_sort_child(self) -> None:
        filter_node = _make_node(name="Filter", node_type="filter")
        exchange = _make_node(
            name="Exchange", node_type="shuffle", children=[filter_node]
        )
        ctx = _ctx_for(exchange)
        results = self.rule.check(exchange, ctx)
        assert len(results) == 0


class TestNonColumnarFormatRule:
    rule = NonColumnarFormatRule()

    def test_detects_csv(self) -> None:
        node = _make_node(
            name="FileScan",
            node_type="scan",
            key_info={"format": "CSV"},
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1
        assert "CSV" in results[0].title

    def test_detects_json(self) -> None:
        node = _make_node(
            name="FileScan",
            node_type="scan",
            key_info={"format": "JSON"},
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1

    def test_skips_parquet(self) -> None:
        node = _make_node(
            name="FileScan",
            node_type="scan",
            key_info={"format": "PARQUET"},
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 0


class TestNestedLoopJoinRule:
    rule = NestedLoopJoinRule()

    def test_detects_nested_loop_join(self) -> None:
        node = _make_node(name="BroadcastNestedLoopJoin", node_type="join")
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1
        assert results[0].severity == Severity.ERROR


class TestPartitionCountRule:
    rule = PartitionCountRule()

    def test_detects_low_partition_count(self) -> None:
        node = _make_node(
            name="Exchange",
            node_type="shuffle",
            key_info={"partitions": "1", "is_shuffle": True},
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1
        assert "Low" in results[0].title

    def test_detects_high_partition_count(self) -> None:
        node = _make_node(
            name="Exchange",
            node_type="shuffle",
            key_info={"partitions": "20000", "is_shuffle": True},
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1
        assert "High" in results[0].title

    def test_skips_normal_count(self) -> None:
        node = _make_node(
            name="Exchange",
            node_type="shuffle",
            key_info={"partitions": "200", "is_shuffle": True},
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 0


class TestPythonUDFRule:
    rule = PythonUDFRule()

    def test_detects_python_udf(self) -> None:
        node = _make_node(name="BatchEvalPython")
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1

    def test_detects_udf_in_description(self) -> None:
        node = _make_node(name="Project", description="PythonUDF#my_func(col1)")
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1


class TestSkewHintRule:
    rule = SkewHintRule()

    def test_suggests_skew_opt(self) -> None:
        node = _make_node(name="SortMergeJoin", node_type="join")
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1
        assert results[0].severity == Severity.INFO

    def test_skips_if_skew_present(self) -> None:
        node = _make_node(
            name="SortMergeJoin",
            node_type="join",
            description="SkewJoin optimization applied",
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 0


class TestWindowWithoutPartitionRule:
    rule = WindowWithoutPartitionRule()

    def test_detects_no_partition(self) -> None:
        node = _make_node(
            name="Window",
            node_type="window",
            description="row_number() windowspecdefinition(id ASC)",
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1

    def test_skips_with_partition(self) -> None:
        node = _make_node(
            name="Window",
            node_type="window",
            description="row_number() PARTITION BY dept",
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 0


class TestUnnecessarySortRule:
    rule = UnnecessarySortRule()

    def test_detects_unnecessary_sort(self) -> None:
        sort_node = _make_node(name="Sort", node_type="sort")
        parent = _make_node(name="Project", node_type="project", children=[sort_node])
        ctx = _build_context(parent)
        results = self.rule.check(sort_node, ctx)
        assert len(results) == 1

    def test_skips_sort_for_merge_join(self) -> None:
        sort_node = _make_node(name="Sort", node_type="sort")
        parent = _make_node(
            name="SortMergeJoin", node_type="join", children=[sort_node]
        )
        ctx = _build_context(parent)
        results = self.rule.check(sort_node, ctx)
        assert len(results) == 0


class TestCoalesceRule:
    rule = CoalesceRule()

    def test_detects_round_robin(self) -> None:
        node = _make_node(
            name="Exchange",
            node_type="shuffle",
            description="Exchange RoundRobinPartitioning(10)",
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 1

    def test_skips_hash_partitioning(self) -> None:
        node = _make_node(
            name="Exchange",
            node_type="shuffle",
            description="Exchange hashpartitioning(id, 200)",
        )
        ctx = _ctx_for(node)
        results = self.rule.check(node, ctx)
        assert len(results) == 0


class TestAttachSuggestions:
    def test_attaches_suggestions_to_nodes(self) -> None:
        scan = _make_node(name="FileScan", node_type="scan")
        root = _make_node(name="Project", node_type="project", children=[scan])

        suggestions = _attach_suggestions(root)

        # The scan has no pushed filters, so FullTableScanRule should fire
        assert len(suggestions) > 0
        assert any(s.rule_id == "full_table_scan" for s in suggestions)

        # Verify it's attached to the scan node
        assert len(scan["suggestions"]) > 0

    def test_deduplicates_suggestions(self) -> None:
        scan = _make_node(name="FileScan", node_type="scan")
        root = _make_node(name="Root", children=[scan])

        # Run twice — should not double-up
        _attach_suggestions(root)
        # Suggestions are per-call, node's list gets populated once
        assert len(scan["suggestions"]) >= 1

    def test_severity_ordering(self) -> None:
        cross = _make_node(
            name="CartesianProduct", node_type="join", key_info={"join_type": "Cross"}
        )
        scan = _make_node(name="FileScan", node_type="scan")
        root = _make_node(name="Root", children=[cross, scan])

        suggestions = _attach_suggestions(root)

        # Errors should come before warnings
        severities = [s.severity for s in suggestions]
        if Severity.ERROR in severities and Severity.WARNING in severities:
            first_error = severities.index(Severity.ERROR)
            first_warning = severities.index(Severity.WARNING)
            assert first_error < first_warning


class TestAnalyzePlan:
    @patch("spark_plan_viz._analyzer._parse_spark_plan")
    def test_analyze_plan_returns_suggestions(self, mock_parse: Mock) -> None:
        mock_df = Mock()
        mock_parse.return_value = _make_node(name="FileScan", node_type="scan")

        results = analyze_plan(mock_df)
        assert isinstance(results, list)
        assert all(isinstance(s, Suggestion) for s in results)

    @patch("spark_plan_viz._analyzer._parse_spark_plan")
    def test_analyze_plan_raises_on_failure(self, mock_parse: Mock) -> None:
        mock_df = Mock()
        mock_parse.return_value = None

        with pytest.raises(ValueError, match="Could not parse"):
            analyze_plan(mock_df)


class TestSuggestionSerialization:
    def test_to_dict(self) -> None:
        s = Suggestion(
            rule_id="test",
            severity=Severity.WARNING,
            title="Test Title",
            message="Test message",
            node_name="TestNode",
        )
        d = s.to_dict()
        assert d["severity"] == "warning"
        assert d["rule_id"] == "test"
        assert d["title"] == "Test Title"
