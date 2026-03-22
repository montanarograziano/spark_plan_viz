"""Optimization rule framework and all built-in rules."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol


class Severity(Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class Suggestion:
    rule_id: str
    severity: Severity
    title: str
    message: str
    node_name: str

    def to_dict(self) -> dict[str, str]:
        return {
            "rule_id": self.rule_id,
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "node_name": self.node_name,
        }


@dataclass
class AnalysisContext:
    """Carries a flattened node list with parent refs for tree-wide analysis."""

    nodes: list[dict[str, Any]] = field(default_factory=list)
    parent_map: dict[int, dict[str, Any]] = field(default_factory=dict)


class Rule(Protocol):
    def check(
        self, node: dict[str, Any], context: AnalysisContext
    ) -> list[Suggestion]: ...


# ---------------------------------------------------------------------------
# Rule implementations
# ---------------------------------------------------------------------------

_PASS_THROUGH_TYPES = {"project", "filter", "other"}
_PUSHDOWN_FORMATS = {"PARQUET", "ORC", "DELTA", "AVRO"}
_ROW_BASED_FORMATS = {"CSV", "JSON"}
_BROADCASTABLE_JOIN_TYPES = {
    "Inner",
    "LeftOuter",
    "RightOuter",
    "LeftSemi",
    "LeftAnti",
}


class CrossJoinRule:
    """Detect cross joins / CartesianProduct — usually unintentional."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        name = node.get("name", "")
        desc = node.get("description", "")
        first_line = desc.splitlines()[0] if desc else ""
        ki = node.get("key_info", {})
        condition = ki.get("condition")

        if "NestedLoop" in name or "NestedLoop" in desc:
            return []
        if "CartesianProduct" in name and "(" in first_line:
            return []
        if "CartesianProduct" in name or ki.get("join_type") == "Cross":
            if condition:
                return []
            return [
                Suggestion(
                    rule_id="cross_join",
                    severity=Severity.ERROR,
                    title="Cross Join Detected",
                    message=(
                        "Cross joins produce the Cartesian product of both sides and "
                        "can explode data volume. Verify this is intentional or add a "
                        "join condition."
                    ),
                    node_name=name,
                )
            ]
        if "Cross" in desc and "Join" in name:
            return [
                Suggestion(
                    rule_id="cross_join",
                    severity=Severity.ERROR,
                    title="Cross Join Detected",
                    message=(
                        "Cross joins produce the Cartesian product of both sides and "
                        "can explode data volume. Verify this is intentional or add a "
                        "join condition."
                    ),
                    node_name=name,
                )
            ]
        return []


class MissingBroadcastHintRule:
    """Suggest broadcast hint when a shuffle join might benefit from it."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        name = node.get("name", "")
        ki = node.get("key_info", {})

        if node.get("type") != "join":
            return []
        if ki.get("is_broadcast"):
            return []
        join_type = ki.get("join_type")
        if join_type and join_type not in _BROADCASTABLE_JOIN_TYPES:
            return []
        if "SortMerge" in name or "ShuffledHash" in name:
            return [
                Suggestion(
                    rule_id="missing_broadcast_hint",
                    severity=Severity.INFO,
                    title="Possible Broadcast Join Opportunity",
                    message=(
                        "This join is currently shuffle-based. If one side is known to "
                        "be small enough for broadcast, a broadcast hint may avoid "
                        "shuffling both sides."
                    ),
                    node_name=name,
                )
            ]
        return []


class FullTableScanRule:
    """Detect pushdown-capable file scans with no pushed filters."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "scan":
            return []
        ki = node.get("key_info", {})
        if ki.get("format", "").upper() not in _PUSHDOWN_FORMATS:
            return []
        if ki.get("pushed_filters"):
            return []
        name = node.get("name", "")
        return [
            Suggestion(
                rule_id="full_table_scan",
                severity=Severity.WARNING,
                title="No Pushed Filters Detected",
                message=(
                    "This pushdown-capable scan shows no pushed filters. If your query "
                    "can filter on partition columns or pushdown-friendly predicates, "
                    "it may reduce data read."
                ),
                node_name=name,
            )
        ]


class RedundantShuffleRule:
    """Detect consecutive Exchange nodes."""

    _PASS_THROUGH_TYPES = {"project", "sort", "filter", "other"}

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "shuffle":
            return []

        current = node
        while True:
            children = current.get("children", [])
            if len(children) != 1:
                return []
            child = children[0]
            if child.get("type") == "shuffle":
                return [
                    Suggestion(
                        rule_id="redundant_shuffle",
                        severity=Severity.WARNING,
                        title="Redundant Shuffle",
                        message=(
                            "Back-to-back Exchange nodes detected. The first shuffle "
                            "may be unnecessary — check if repartitioning can be "
                            "consolidated."
                        ),
                        node_name=node.get("name", ""),
                    )
                ]
            if child.get("type") not in self._PASS_THROUGH_TYPES:
                return []
            current = child
        return []


class ExpensiveCollectRule:
    """Detect collect_list / collect_set in aggregates."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "aggregate":
            return []
        desc = node.get("description", "").lower()
        if "collect_list" in desc or "collect_set" in desc:
            return [
                Suggestion(
                    rule_id="expensive_collect",
                    severity=Severity.WARNING,
                    title="Expensive Collect Operation",
                    message=(
                        "collect_list/collect_set aggregates all values into a single "
                        "executor's memory. For large groups this can cause OOM. "
                        "Consider alternatives like array_agg with limits or "
                        "pre-filtering."
                    ),
                    node_name=node.get("name", ""),
                )
            ]
        return []


class SortBeforeShuffleRule:
    """Detect Sort immediately followed by Exchange — the sort is wasted."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "shuffle":
            return []
        children = node.get("children", [])
        for child in children:
            if child.get("type") == "sort":
                return [
                    Suggestion(
                        rule_id="sort_before_shuffle",
                        severity=Severity.WARNING,
                        title="Sort Before Shuffle",
                        message=(
                            "A Sort immediately before an Exchange is usually wasted "
                            "because the shuffle destroys the ordering. Check if the "
                            "sort can be removed or moved after the exchange."
                        ),
                        node_name=child.get("name", ""),
                    )
                ]
        return []


class NonColumnarFormatRule:
    """Detect CSV/JSON format in scans — suggest columnar formats."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "scan":
            return []
        ki = node.get("key_info", {})
        fmt = ki.get("format", "").upper()
        if fmt in _ROW_BASED_FORMATS and ki.get("pushed_filters"):
            return [
                Suggestion(
                    rule_id="non_columnar_format",
                    severity=Severity.INFO,
                    title=f"Row-Based Format ({fmt})",
                    message=(
                        f"Reading data in {fmt} format. Columnar formats like Parquet "
                        "or ORC often improve pruning and scan efficiency for analytic "
                        "workloads."
                    ),
                    node_name=node.get("name", ""),
                )
            ]
        return []


class NonColumnarNoPushdownRule:
    """Detect row-based scans with no pushed filters."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "scan":
            return []
        ki = node.get("key_info", {})
        fmt = ki.get("format", "").upper()
        if fmt in _ROW_BASED_FORMATS and not ki.get("pushed_filters"):
            return [
                Suggestion(
                    rule_id="non_columnar_no_pushdown",
                    severity=Severity.WARNING,
                    title=f"Row-Based Scan Without Pushdown ({fmt})",
                    message=(
                        f"This {fmt} scan has no pushed filters. Row-based formats "
                        "already limit pruning, so adding selective filters earlier or "
                        "converting to Parquet/ORC may reduce scan cost."
                    ),
                    node_name=node.get("name", ""),
                )
            ]
        return []


class NestedLoopJoinRule:
    """Detect BroadcastNestedLoopJoin — extremely expensive for large data."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        name = node.get("name", "")
        desc = node.get("description", "")
        first_line = desc.splitlines()[0] if desc else ""
        ki = node.get("key_info", {})

        if (
            "BroadcastNestedLoopJoin" in name
            or "BroadcastNestedLoopJoin" in desc
            or ("CartesianProduct" in name and "(" in first_line)
            or (
                node.get("type") == "join"
                and ki.get("join_type") == "Cross"
                and ki.get("condition")
            )
        ):
            return [
                Suggestion(
                    rule_id="nested_loop_join",
                    severity=Severity.ERROR,
                    title="Nested Loop Join",
                    message=(
                        "BroadcastNestedLoopJoin is an O(n*m) operation. It usually "
                        "appears when Spark cannot find an equi-join condition. "
                        "Rewrite the join with equality predicates if possible."
                    ),
                    node_name=name,
                )
            ]
        return []


class PartitionCountRule:
    """Detect extreme partition counts (<2 or >10000)."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "shuffle":
            return []
        ki = node.get("key_info", {})
        partitions_str = ki.get("partitions")
        if not partitions_str:
            return []
        try:
            count = int(partitions_str)
        except ValueError:
            return []

        name = node.get("name", "")
        if count < 2:
            return [
                Suggestion(
                    rule_id="partition_count_low",
                    severity=Severity.WARNING,
                    title=f"Very Low Partition Count ({count})",
                    message=(
                        "Only 1 partition means no parallelism. Consider increasing "
                        "spark.sql.shuffle.partitions or repartitioning."
                    ),
                    node_name=name,
                )
            ]
        if count > 10000:
            return [
                Suggestion(
                    rule_id="partition_count_high",
                    severity=Severity.WARNING,
                    title=f"Very High Partition Count ({count})",
                    message=(
                        "More than 10,000 partitions can cause excessive task "
                        "scheduling overhead. Consider coalescing or adjusting "
                        "spark.sql.shuffle.partitions."
                    ),
                    node_name=name,
                )
            ]
        return []


class PythonUDFRule:
    """Detect PythonUDF / BatchEvalPython / ArrowEvalPython nodes."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        name = node.get("name", "")
        desc = node.get("description", "")
        if any(
            kw in name or kw in desc
            for kw in ("PythonUDF", "BatchEvalPython", "ArrowEvalPython")
        ):
            return [
                Suggestion(
                    rule_id="python_udf",
                    severity=Severity.WARNING,
                    title="Python UDF Detected",
                    message=(
                        "Python UDFs serialize data between the JVM and Python, "
                        "which is slow. Consider using Spark SQL built-in functions, "
                        "pandas_udf with Arrow, or Spark Expressions instead."
                    ),
                    node_name=name,
                )
            ]
        return []


class SkewHintRule:
    """Suggest skew optimization for SortMergeJoin."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        return []


class WindowWithoutPartitionRule:
    """Detect Window functions without PARTITION BY."""

    _PASS_THROUGH_TYPES = {"sort", "project", "filter", "other"}

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "window":
            return []
        desc = node.get("description", "")
        if "partitionBy=[]" in desc:
            return [
                Suggestion(
                    rule_id="window_without_partition",
                    severity=Severity.WARNING,
                    title="Window Without PARTITION BY",
                    message=(
                        "Window function without PARTITION BY moves all data to a "
                        "single partition. Add a PARTITION BY clause to distribute "
                        "the work."
                    ),
                    node_name=node.get("name", ""),
                )
            ]
        lower_desc = desc.lower()
        if "partition by" in lower_desc or "partitionby" in lower_desc:
            return []

        current = node
        while len(current.get("children", [])) == 1:
            child = current["children"][0]
            if child.get("type") == "shuffle":
                child_desc = child.get("description", "")
                child_name = child.get("name", "")
                if "SinglePartition" in child_desc or "SinglePartition" in child_name:
                    return [
                        Suggestion(
                            rule_id="window_without_partition",
                            severity=Severity.WARNING,
                            title="Window Without PARTITION BY",
                            message=(
                                "Window function without PARTITION BY moves all data to a "
                                "single partition. Add a PARTITION BY clause to distribute "
                                "the work."
                            ),
                            node_name=node.get("name", ""),
                        )
                    ]
                return []
            if child.get("type") not in self._PASS_THROUGH_TYPES:
                break
            current = child

        if re.search(r"windowspecdefinition\([^,]+\s+(?:ASC|DESC)\b", desc):
            return [
                Suggestion(
                    rule_id="window_without_partition",
                    severity=Severity.WARNING,
                    title="Window Without PARTITION BY",
                    message=(
                        "Window function without PARTITION BY moves all data to a "
                        "single partition. Add a PARTITION BY clause to distribute "
                        "the work."
                    ),
                    node_name=node.get("name", ""),
                )
            ]

        return []


class UnnecessarySortRule:
    """Detect Sort not consumed by an ordering-dependent operation."""

    _ORDERING_CONSUMERS = {"SortMergeJoin", "Window", "TakeOrderedAndProject"}

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "sort":
            return []
        node_id = id(node)
        parent = context.parent_map.get(node_id)
        if parent is None:
            return []
        current = parent
        while current is not None:
            current_name = current.get("name", "")
            if any(kw in current_name for kw in self._ORDERING_CONSUMERS):
                return []
            if current.get("type") == "shuffle":
                return []
            if current.get("type") not in _PASS_THROUGH_TYPES:
                break
            grandparent = context.parent_map.get(id(current))
            if grandparent is None:
                return []
            current = grandparent
        return [
            Suggestion(
                rule_id="unnecessary_sort",
                severity=Severity.INFO,
                title="Potentially Unnecessary Sort",
                message=(
                    "This Sort's output does not appear to be consumed by an "
                    "ordering-dependent operation. If final ordering is not needed, "
                    "removing it can save time."
                ),
                node_name=node.get("name", ""),
            )
        ]


class SinglePartitionExchangeRule:
    """Detect shuffles that collapse work to a single partition."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "shuffle":
            return []
        desc = node.get("description", "")
        name = node.get("name", "")
        if "SinglePartition" not in desc and "SinglePartition" not in name:
            return []
        return [
            Suggestion(
                rule_id="single_partition_exchange",
                severity=Severity.WARNING,
                title="Single-Partition Exchange",
                message=(
                    "This exchange funnels work into a single partition, which can "
                    "serialize execution and create a bottleneck."
                ),
                node_name=name,
            )
        ]


class CoalesceRule:
    """Detect RoundRobinPartitioning and explain it as a repartition-style shuffle."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "shuffle":
            return []
        desc = node.get("description", "")
        if "RoundRobinPartitioning" not in desc:
            return []
        partitions = node.get("key_info", {}).get("partitions")
        partition_suffix = (
            f" If this change reduces partitions to {partitions}, consider "
            "coalesce(n) instead."
            if partitions
            else " If this change is only reducing partitions, consider "
            "coalesce(n) instead."
        )
        return [
            Suggestion(
                rule_id="coalesce",
                severity=Severity.INFO,
                title="Round-Robin Repartition",
                message=(
                    "RoundRobinPartitioning usually indicates a repartition-style full "
                    "shuffle." + partition_suffix
                ),
                node_name=node.get("name", ""),
            )
        ]


# Registry of all rules
ALL_RULES: list[Rule] = [
    CrossJoinRule(),
    MissingBroadcastHintRule(),
    FullTableScanRule(),
    RedundantShuffleRule(),
    ExpensiveCollectRule(),
    SortBeforeShuffleRule(),
    NonColumnarFormatRule(),
    NonColumnarNoPushdownRule(),
    NestedLoopJoinRule(),
    PartitionCountRule(),
    PythonUDFRule(),
    WindowWithoutPartitionRule(),
    UnnecessarySortRule(),
    SinglePartitionExchangeRule(),
    CoalesceRule(),
]
