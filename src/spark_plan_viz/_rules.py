"""Optimization rule framework and all built-in rules."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from spark_plan_viz._constants import (
    BROADCASTABLE_JOIN_TYPES,
    PARTITION_COUNT_MAX,
    PARTITION_COUNT_MIN,
    PASS_THROUGH_TYPES,
    PUSHDOWN_FORMATS,
    ROW_BASED_FORMATS,
    ROW_EXPLODING_GENERATORS,
)


class Severity(Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass(frozen=True)
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


def _suggest(
    rule_id: str,
    severity: Severity,
    title: str,
    message: str,
    node: dict[str, Any],
) -> Suggestion:
    return Suggestion(
        rule_id=rule_id,
        severity=severity,
        title=title,
        message=message,
        node_name=node.get("name", ""),
    )


def _ki(node: dict[str, Any]) -> dict[str, Any]:
    return node.get("key_info") or {}


def _desc(node: dict[str, Any]) -> str:
    return node.get("description") or ""


def _first_line(text: str) -> str:
    return text.splitlines()[0] if text else ""


# ---------------------------------------------------------------------------
# Shared suggestion copy
# ---------------------------------------------------------------------------

_CROSS_JOIN_MSG = (
    "Cross joins produce the Cartesian product of both sides and "
    "can explode data volume. Verify this is intentional or add a "
    "join condition."
)
_WINDOW_NO_PART_MSG = (
    "Window function without PARTITION BY moves all data to a "
    "single partition. Add a PARTITION BY clause to distribute "
    "the work."
)
_NESTED_LOOP_MSG = (
    "Nested-loop joins are O(n*m). They usually appear when Spark "
    "cannot find an equi-join condition. Rewrite the join with "
    "equality predicates if possible."
)


# ---------------------------------------------------------------------------
# Rule implementations
# ---------------------------------------------------------------------------


class CrossJoinRule:
    """Detect cross joins / CartesianProduct — usually unintentional."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        name = node.get("name", "")
        desc = _desc(node)
        first_line = _first_line(desc)
        ki = _ki(node)
        condition = ki.get("condition")

        if "NestedLoop" in name or "NestedLoop" in desc:
            return []
        # CartesianProduct with a condition is treated as nested-loop style
        if "CartesianProduct" in name and "(" in first_line:
            return []
        if "CartesianProduct" in name or ki.get("join_type") == "Cross":
            if condition:
                return []
            return [
                _suggest(
                    "cross_join",
                    Severity.ERROR,
                    "Cross Join Detected",
                    _CROSS_JOIN_MSG,
                    node,
                )
            ]
        if "Cross" in desc and "Join" in name:
            return [
                _suggest(
                    "cross_join",
                    Severity.ERROR,
                    "Cross Join Detected",
                    _CROSS_JOIN_MSG,
                    node,
                )
            ]
        return []


class MissingBroadcastHintRule:
    """Suggest broadcast hint when a shuffle join might benefit from it."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        name = node.get("name", "")
        ki = _ki(node)

        if node.get("type") != "join":
            return []
        if ki.get("is_broadcast") or ki.get("is_skew"):
            return []
        join_type = ki.get("join_type")
        if join_type and join_type not in BROADCASTABLE_JOIN_TYPES:
            return []
        if "SortMerge" in name or "ShuffledHash" in name:
            return [
                _suggest(
                    "missing_broadcast_hint",
                    Severity.INFO,
                    "Possible Broadcast Join Opportunity",
                    (
                        "This join is currently shuffle-based. If one side is known to "
                        "be small enough for broadcast, a broadcast hint may avoid "
                        "shuffling both sides."
                    ),
                    node,
                )
            ]
        return []


class FullTableScanRule:
    """Detect pushdown-capable file scans with no pushed filters."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "scan":
            return []
        ki = _ki(node)
        if ki.get("format", "").upper() not in PUSHDOWN_FORMATS:
            return []
        if ki.get("pushed_filters"):
            return []
        # Local/in-memory scans are not storage pushdown candidates
        name = node.get("name", "")
        if any(x in name for x in ("LocalTableScan", "Range", "OneRowRelation")):
            return []
        return [
            _suggest(
                "full_table_scan",
                Severity.WARNING,
                "No Pushed Filters Detected",
                (
                    "This pushdown-capable scan shows no pushed filters. If your query "
                    "can filter on partition columns or pushdown-friendly predicates, "
                    "it may reduce data read."
                ),
                node,
            )
        ]


class EmptyPartitionFiltersRule:
    """Detect partitioned scans that do no partition pruning."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "scan":
            return []
        ki = _ki(node)
        if not ki.get("has_partition_columns"):
            return []
        # partition_filters key present and empty → no pruning
        if "partition_filters" not in ki:
            return []
        if ki.get("partition_filters"):
            return []
        return [
            _suggest(
                "empty_partition_filters",
                Severity.WARNING,
                "No Partition Pruning",
                (
                    "This scan reads a partitioned table but PartitionFilters is empty. "
                    "Filter on partition columns (for example date or region) so Spark "
                    "can skip irrelevant partitions."
                ),
                node,
            )
        ]


class RedundantShuffleRule:
    """Detect consecutive Exchange nodes."""

    _WALK_TYPES = PASS_THROUGH_TYPES | {"sort", "broadcast", "shuffle_read"}

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
                    _suggest(
                        "redundant_shuffle",
                        Severity.WARNING,
                        "Redundant Shuffle",
                        (
                            "Back-to-back Exchange nodes detected. The first shuffle "
                            "may be unnecessary — check if repartitioning can be "
                            "consolidated."
                        ),
                        node,
                    )
                ]
            if child.get("type") not in self._WALK_TYPES:
                return []
            current = child


class ExpensiveCollectRule:
    """Detect collect_list / collect_set in aggregates."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "aggregate":
            return []
        desc = _desc(node).lower()
        if "collect_list" in desc or "collect_set" in desc:
            return [
                _suggest(
                    "expensive_collect",
                    Severity.WARNING,
                    "Expensive Collect Operation",
                    (
                        "collect_list/collect_set aggregates all values into a single "
                        "executor's memory. For large groups this can cause OOM. "
                        "Consider alternatives like array_agg with limits or "
                        "pre-filtering."
                    ),
                    node,
                )
            ]
        return []


class SortBeforeShuffleRule:
    """Detect Sort immediately followed by Exchange — the sort is wasted."""

    # WholeStageCodegen / InputAdapter often wrap the Sort under an Exchange.
    _WALK_TYPES = PASS_THROUGH_TYPES | {"broadcast", "shuffle_read"}

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "shuffle":
            return []

        stack = list(node.get("children", []))
        seen: set[int] = set()
        while stack:
            child = stack.pop()
            cid = id(child)
            if cid in seen:
                continue
            seen.add(cid)
            if child.get("type") == "sort":
                return [
                    _suggest(
                        "sort_before_shuffle",
                        Severity.WARNING,
                        "Sort Before Shuffle",
                        (
                            "A Sort immediately before an Exchange is usually wasted "
                            "because the shuffle destroys the ordering. Check if the "
                            "sort can be removed or moved after the exchange."
                        ),
                        child,
                    )
                ]
            if child.get("type") in self._WALK_TYPES:
                stack.extend(child.get("children", []))
        return []


class NonColumnarFormatRule:
    """Detect CSV/JSON format in scans — suggest columnar formats."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "scan":
            return []
        ki = _ki(node)
        fmt = ki.get("format", "").upper()
        if fmt in ROW_BASED_FORMATS and ki.get("pushed_filters"):
            return [
                _suggest(
                    "non_columnar_format",
                    Severity.INFO,
                    f"Row-Based Format ({fmt})",
                    (
                        f"Reading data in {fmt} format. Columnar formats like Parquet "
                        "or ORC often improve pruning and scan efficiency for analytic "
                        "workloads."
                    ),
                    node,
                )
            ]
        return []


class NonColumnarNoPushdownRule:
    """Detect row-based scans with no pushed filters."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "scan":
            return []
        ki = _ki(node)
        fmt = ki.get("format", "").upper()
        if fmt in ROW_BASED_FORMATS and not ki.get("pushed_filters"):
            return [
                _suggest(
                    "non_columnar_no_pushdown",
                    Severity.WARNING,
                    f"Row-Based Scan Without Pushdown ({fmt})",
                    (
                        f"This {fmt} scan has no pushed filters. Row-based formats "
                        "already limit pruning, so adding selective filters earlier or "
                        "converting to Parquet/ORC may reduce scan cost."
                    ),
                    node,
                )
            ]
        return []


class NestedLoopJoinRule:
    """Detect nested-loop / conditioned Cartesian joins — expensive for large data."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        name = node.get("name", "")
        desc = _desc(node)
        first_line = _first_line(desc)
        ki = _ki(node)

        is_nlj = (
            "NestedLoopJoin" in name
            or "NestedLoopJoin" in desc
            or "ShuffledNestedLoopJoin" in name
            or ("CartesianProduct" in name and "(" in first_line)
            or (
                node.get("type") == "join"
                and ki.get("join_type") == "Cross"
                and ki.get("condition")
            )
        )
        if not is_nlj:
            return []
        return [
            _suggest(
                "nested_loop_join",
                Severity.ERROR,
                "Nested Loop Join",
                _NESTED_LOOP_MSG,
                node,
            )
        ]


class PartitionCountRule:
    """Detect extreme partition counts (<2 or >10000)."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "shuffle":
            return []
        partitions_str = _ki(node).get("partitions")
        if not partitions_str:
            return []
        try:
            count = int(partitions_str)
        except (TypeError, ValueError):
            return []

        if count < PARTITION_COUNT_MIN:
            return [
                _suggest(
                    "partition_count_low",
                    Severity.WARNING,
                    f"Very Low Partition Count ({count})",
                    (
                        "Only 1 partition means no parallelism. Consider increasing "
                        "spark.sql.shuffle.partitions or repartitioning."
                    ),
                    node,
                )
            ]
        if count > PARTITION_COUNT_MAX:
            return [
                _suggest(
                    "partition_count_high",
                    Severity.WARNING,
                    f"Very High Partition Count ({count})",
                    (
                        f"More than {PARTITION_COUNT_MAX:,} partitions can cause "
                        "excessive task scheduling overhead. Consider coalescing or "
                        "adjusting spark.sql.shuffle.partitions."
                    ),
                    node,
                )
            ]
        return []


class PythonUDFRule:
    """Detect PythonUDF / BatchEvalPython / ArrowEvalPython nodes."""

    _MARKERS = (
        "PythonUDF",
        "BatchEvalPython",
        "ArrowEvalPython",
        "FlatMapGroupsInPandas",
        "MapInPandas",
        "PythonMapInArrow",
        "MapInArrow",
        "FlatMapCoGroupsInPandas",
    )

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        name = node.get("name", "")
        desc = _desc(node)
        if any(kw in name or kw in desc for kw in self._MARKERS):
            return [
                _suggest(
                    "python_udf",
                    Severity.WARNING,
                    "Python UDF Detected",
                    (
                        "Python UDFs serialize data between the JVM and Python, "
                        "which is slow. Consider using Spark SQL built-in functions, "
                        "pandas_udf with Arrow, or Spark Expressions instead."
                    ),
                    node,
                )
            ]
        return []


class SkewHintRule:
    """Surface AQE skew-join handling when the plan marks isSkew=true."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "join":
            return []
        ki = _ki(node)
        desc = _desc(node)
        if not (ki.get("is_skew") or "isSkew=true" in desc.replace(" ", "")):
            return []
        return [
            _suggest(
                "skew_join",
                Severity.INFO,
                "AQE Skew Join Active",
                (
                    "This join is marked as skew-optimized by Adaptive Query Execution. "
                    "Skewed partitions are being split at runtime. If skew persists, "
                    "consider salting keys or pre-aggregating the heavy side."
                ),
                node,
            )
        ]


class ExpandRule:
    """Detect Expand from CUBE/ROLLUP/GROUPING SETS or multiple COUNT DISTINCT."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        name = node.get("name", "")
        if node.get("type") != "expand" and "Expand" not in name:
            return []
        groups = _ki(node).get("expand_groups")
        desc = _desc(node)
        if groups is None:
            # Fallback: count projection groups in description
            groups = (
                len(re.findall(r"\[[^\[\]]*\]", desc.split("Expand")[-1][:500])) or None
            )

        if groups is not None and groups >= 2:
            msg = (
                f"Expand multiplies each input row by about {groups}x "
                "(CUBE/ROLLUP/GROUPING SETS or multiple COUNT DISTINCT). "
                "Large expand factors can dominate shuffle and memory cost. "
                "Prefer fewer grouping sets, or rewrite multiple COUNT DISTINCT "
                "into conditional aggregates when possible."
            )
        else:
            msg = (
                "Expand multiplies rows for CUBE/ROLLUP/GROUPING SETS or multiple "
                "COUNT DISTINCT. Large expand factors can dominate shuffle and "
                "memory cost."
            )
        return [
            _suggest(
                "expand",
                Severity.WARNING,
                "Row-Multiplying Expand",
                msg,
                node,
            )
        ]


class GenerateExplodeRule:
    """Detect Generate nodes that explode arrays/maps into many rows."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        name = node.get("name", "")
        if node.get("type") != "generate" and "Generate" not in name:
            return []
        generator = (_ki(node).get("generator") or "").lower()
        desc_lower = _desc(node).lower()
        if generator not in ROW_EXPLODING_GENERATORS:
            if not any(g in desc_lower for g in ROW_EXPLODING_GENERATORS):
                return []
            generator = next(
                (g for g in ROW_EXPLODING_GENERATORS if g in desc_lower),
                generator or "explode",
            )
        return [
            _suggest(
                "generate_explode",
                Severity.WARNING,
                f"Row Explosion ({generator})",
                (
                    f"{generator}() multiplies rows by collection size and can blow up "
                    "partition memory. Filter/project before exploding, drop unused "
                    "columns immediately after, and consider repartitioning if arrays "
                    "are large."
                ),
                node,
            )
        ]


class WindowWithoutPartitionRule:
    """Detect Window functions without PARTITION BY."""

    _WALK_TYPES = PASS_THROUGH_TYPES | {"sort"}

    def _hit(self, node: dict[str, Any]) -> list[Suggestion]:
        return [
            _suggest(
                "window_without_partition",
                Severity.WARNING,
                "Window Without PARTITION BY",
                _WINDOW_NO_PART_MSG,
                node,
            )
        ]

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "window":
            return []
        desc = _desc(node)
        if "partitionBy=[]" in desc:
            return self._hit(node)
        lower_desc = desc.lower()
        if "partition by" in lower_desc or "partitionby" in lower_desc:
            return []

        current = node
        while len(current.get("children", [])) == 1:
            child = current["children"][0]
            if child.get("type") == "shuffle":
                child_desc = _desc(child)
                child_name = child.get("name", "")
                if "SinglePartition" in child_desc or "SinglePartition" in child_name:
                    return self._hit(node)
                return []
            if child.get("type") not in self._WALK_TYPES:
                break
            current = child

        if re.search(
            r"windowspecdefinition\([^,]+\s+(?:ASC|DESC)\b", desc, re.IGNORECASE
        ):
            return self._hit(node)

        return []


class UnnecessarySortRule:
    """Detect Sort not consumed by an ordering-dependent operation."""

    _ORDERING_CONSUMERS = ("SortMergeJoin", "Window", "TakeOrderedAndProject")

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "sort":
            return []
        parent = context.parent_map.get(id(node))
        if parent is None:
            return []
        current: dict[str, Any] | None = parent
        while current is not None:
            current_name = current.get("name", "")
            if any(kw in current_name for kw in self._ORDERING_CONSUMERS):
                return []
            if current.get("type") in {"shuffle", "shuffle_read"}:
                return []
            if current.get("type") not in PASS_THROUGH_TYPES:
                break
            current = context.parent_map.get(id(current))
            if current is None:
                return []
        return [
            _suggest(
                "unnecessary_sort",
                Severity.INFO,
                "Potentially Unnecessary Sort",
                (
                    "This Sort's output does not appear to be consumed by an "
                    "ordering-dependent operation. If final ordering is not needed, "
                    "removing it can save time."
                ),
                node,
            )
        ]


class SinglePartitionExchangeRule:
    """Detect shuffles that collapse work to a single partition."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "shuffle":
            return []
        desc = _desc(node)
        name = node.get("name", "")
        ki = _ki(node)
        if (
            "SinglePartition" not in desc
            and "SinglePartition" not in name
            and ki.get("shuffle_type") != "SinglePartition"
            and ki.get("partitions") != "1"
        ):
            return []
        # partitions==1 from hashpartitioning(x, 1) should still warn via PartitionCount
        if (
            "SinglePartition" not in desc
            and "SinglePartition" not in name
            and ki.get("shuffle_type") != "SinglePartition"
        ):
            return []
        return [
            _suggest(
                "single_partition_exchange",
                Severity.WARNING,
                "Single-Partition Exchange",
                (
                    "This exchange funnels work into a single partition, which can "
                    "serialize execution and create a bottleneck."
                ),
                node,
            )
        ]


class CoalesceRule:
    """Detect RoundRobinPartitioning and explain it as a repartition-style shuffle."""

    def check(self, node: dict[str, Any], context: AnalysisContext) -> list[Suggestion]:
        if node.get("type") != "shuffle":
            return []
        desc = _desc(node)
        ki = _ki(node)
        if (
            "RoundRobinPartitioning" not in desc
            and ki.get("shuffle_type") != "RoundRobin"
        ):
            return []
        partitions = ki.get("partitions")
        partition_suffix = (
            f" If this change reduces partitions to {partitions}, consider "
            "coalesce(n) instead."
            if partitions
            else " If this change is only reducing partitions, consider "
            "coalesce(n) instead."
        )
        return [
            _suggest(
                "coalesce",
                Severity.INFO,
                "Round-Robin Repartition",
                (
                    "RoundRobinPartitioning usually indicates a repartition-style full "
                    "shuffle." + partition_suffix
                ),
                node,
            )
        ]


# Registry of all rules (order is stable for deterministic suggestion lists)
ALL_RULES: list[Rule] = [
    CrossJoinRule(),
    NestedLoopJoinRule(),
    FullTableScanRule(),
    EmptyPartitionFiltersRule(),
    ExpandRule(),
    GenerateExplodeRule(),
    RedundantShuffleRule(),
    ExpensiveCollectRule(),
    SortBeforeShuffleRule(),
    NonColumnarFormatRule(),
    NonColumnarNoPushdownRule(),
    PartitionCountRule(),
    PythonUDFRule(),
    WindowWithoutPartitionRule(),
    SinglePartitionExchangeRule(),
    MissingBroadcastHintRule(),
    UnnecessarySortRule(),
    CoalesceRule(),
    SkewHintRule(),
]
