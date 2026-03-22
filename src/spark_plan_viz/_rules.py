"""Optimization rule framework and all built-in rules."""

from __future__ import annotations

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


class CrossJoinRule:
    """Detect cross joins / CartesianProduct — usually unintentional."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        name = node.get("name", "")
        desc = node.get("description", "")
        ki = node.get("key_info", {})

        if "NestedLoop" in name or "NestedLoop" in desc:
            return []
        if "CartesianProduct" in name or ki.get("join_type") == "Cross":
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
        if "SortMerge" in name or "ShuffledHash" in name:
            return [
                Suggestion(
                    rule_id="missing_broadcast_hint",
                    severity=Severity.INFO,
                    title="Consider Broadcast Join",
                    message=(
                        "This is a shuffle-based join. If one side is small enough "
                        "(< spark.sql.autoBroadcastJoinThreshold), consider using "
                        "broadcast() to avoid the shuffle."
                    ),
                    node_name=name,
                )
            ]
        return []


class FullTableScanRule:
    """Detect scan nodes with no pushed filters."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "scan":
            return []
        ki = node.get("key_info", {})
        if ki.get("pushed_filters"):
            return []
        name = node.get("name", "")
        return [
            Suggestion(
                rule_id="full_table_scan",
                severity=Severity.WARNING,
                title="Full Table Scan",
                message=(
                    "No pushed filters detected on this scan. Adding filters that can "
                    "be pushed down (e.g., partition columns, predicate pushdown) will "
                    "reduce data read."
                ),
                node_name=name,
            )
        ]


class RedundantShuffleRule:
    """Detect consecutive Exchange nodes."""

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
            if child.get("type") not in {"project", "sort", "filter"}:
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
        if fmt in ("CSV", "JSON"):
            return [
                Suggestion(
                    rule_id="non_columnar_format",
                    severity=Severity.INFO,
                    title=f"Non-Columnar Format ({fmt})",
                    message=(
                        f"Reading data in {fmt} format. Columnar formats like Parquet "
                        "or ORC support predicate pushdown and column pruning, "
                        "significantly reducing I/O."
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
        if "BroadcastNestedLoopJoin" in name or "BroadcastNestedLoopJoin" in desc:
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
        name = node.get("name", "")
        desc = node.get("description", "")
        if "SortMergeJoin" not in name:
            return []
        if "skew" in desc.lower() or "SkewJoin" in desc:
            return []
        return [
            Suggestion(
                rule_id="skew_hint",
                severity=Severity.INFO,
                title="Consider Skew Optimization",
                message=(
                    "SortMergeJoin can be slow if join keys are skewed. "
                    "Enable AQE skew join optimization "
                    "(spark.sql.adaptive.skewJoin.enabled) or add a skew hint."
                ),
                node_name=name,
            )
        ]


class WindowWithoutPartitionRule:
    """Detect Window functions without PARTITION BY."""

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
            if child.get("type") not in {"sort", "project", "filter"}:
                break
            current = child

        lower_desc = desc.lower()
        if "partition by" not in lower_desc and "partitionby" not in lower_desc:
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
        parent_name = parent.get("name", "")
        if any(kw in parent_name for kw in self._ORDERING_CONSUMERS):
            return []
        if parent.get("type") == "shuffle":
            return []
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


class CoalesceRule:
    """Detect RoundRobinPartitioning that reduces partition count (coalesce)."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "shuffle":
            return []
        desc = node.get("description", "")
        if "RoundRobinPartitioning" not in desc:
            return []
        return [
            Suggestion(
                rule_id="coalesce",
                severity=Severity.INFO,
                title="Coalesce via Round-Robin",
                message=(
                    "RoundRobinPartitioning detected — this may be a coalesce() that "
                    "still triggers a full shuffle. If reducing partition count, "
                    "consider coalesce(n) which avoids a full shuffle when possible."
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
    NestedLoopJoinRule(),
    PartitionCountRule(),
    PythonUDFRule(),
    SkewHintRule(),
    WindowWithoutPartitionRule(),
    UnnecessarySortRule(),
    CoalesceRule(),
]
