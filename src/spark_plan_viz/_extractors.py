"""Extraction functions for parsing Spark plan node descriptions."""

from __future__ import annotations

import re
from typing import Any

from spark_plan_viz._constants import (
    MAX_DISPLAY_COLUMNS,
    MAX_DISPLAY_FILTERS,
    MAX_DISPLAY_FUNCTIONS,
    MAX_DISPLAY_GROUPING_KEYS,
)


def _extract_join_type(description: str) -> str | None:
    """Extract the type of join from the description."""
    match = re.search(
        r"\b(Inner|LeftOuter|RightOuter|FullOuter|LeftSemi|LeftAnti|Cross)\s*Join",
        description,
    )
    if match:
        return match.group(1)
    return None


def _extract_join_condition(description: str) -> str | None:
    """Extract join condition from the description."""
    match = re.search(r"(?:condition|BuildSide):\s*([^\n]+)", description)
    if match:
        return match.group(1).strip()
    return None


def _extract_filter_condition(description: str) -> str | None:
    """Extract filter condition from the description."""
    match = re.search(r"Filter\s+([^\n]+)", description)
    if match:
        condition = match.group(1).strip()
        condition = re.sub(r"^:\s*", "", condition)
        return condition
    return None


def _extract_selected_columns(description: str) -> list[str]:
    """Extract projected/selected columns from the description."""
    match = re.search(r"\[([^\]]+)\]", description)
    if match:
        cols_str = match.group(1)
        cols = [c.strip() for c in cols_str.split(",")]
        cols = [re.sub(r"#\d+L?", "", c) for c in cols]
        return cols[:MAX_DISPLAY_COLUMNS]
    return []


def _extract_aggregate_functions(description: str) -> list[str]:
    """Extract aggregate functions from the description."""
    agg_funcs = re.findall(
        r"\b(sum|count|avg|min|max|first|last|collect_list|collect_set)\s*\([^)]*\)",
        description,
        re.IGNORECASE,
    )
    return agg_funcs[:MAX_DISPLAY_FUNCTIONS]


def _extract_table_name(description: str) -> str | None:
    """Extract table/file name from scan operations."""
    match = re.search(r"(?:Location|Table):\s*([^\n,]+)", description)
    if match:
        path = match.group(1).strip()
        if "/" in path:
            parts = path.split("/")
            return parts[-1] if parts[-1] else parts[-2]
        return path
    match = re.search(r"(\w+\.)?(\w+)\.(\w+)", description)
    if match:
        return f"{match.group(2)}.{match.group(3)}"
    return None


def _is_broadcast_join(description: str, name: str) -> bool:
    """Check if this is a broadcast join (important for performance)."""
    return "Broadcast" in name or "broadcast" in description.lower()


def _extract_sort_order(description: str) -> str | None:
    """Extract sort order from Sort operations."""
    match = re.search(r"\[(.*?)\s+(ASC|DESC)", description, re.IGNORECASE)
    if match:
        col = re.sub(r"#\d+L?", "", match.group(1))
        order = match.group(2).upper()
        return f"{col} {order}"
    return None


def _extract_data_format(description: str) -> str | None:
    """Extract data format from scan operations (parquet, orc, json, etc)."""
    formats = ["parquet", "orc", "json", "csv", "avro", "delta"]
    for fmt in formats:
        if fmt in description.lower():
            return fmt.upper()
    return None


def _extract_pushed_filters(description: str) -> list[str]:
    """Extract pushed down filters (important for performance)."""
    match = re.search(r"PushedFilters?:\s*\[([^\]]+)\]", description)
    if match:
        filters = [f.strip() for f in match.group(1).split(",")]
        return filters[:MAX_DISPLAY_FILTERS]
    return []


def _extract_grouping_keys(description: str) -> list[str]:
    """Extract grouping keys from aggregate operations."""
    match = re.search(r"keys=\[([^\]]+)\]", description)
    if match:
        keys = [k.strip() for k in match.group(1).split(",")]
        keys = [re.sub(r"#\d+L?", "", k) for k in keys]
        return keys[:MAX_DISPLAY_GROUPING_KEYS]
    return []


def _extract_shuffle_info(description: str) -> dict[str, str]:
    """Extract shuffle type and partition count from shuffle/exchange nodes."""
    info: dict[str, str] = {}
    shuffle_match = re.search(r"(hash|range)partitioning", description, re.IGNORECASE)
    if shuffle_match:
        info["shuffle_type"] = shuffle_match.group(1).capitalize()

    partition_match = re.search(r"(\d+)\s*partitions?", description, re.IGNORECASE)
    if partition_match:
        info["partitions"] = partition_match.group(1)

    return info


def _extract_build_side(description: str) -> str | None:
    """Extract which side is broadcast in a broadcast join."""
    build_match = re.search(r"BuildSide:\s*(Left|Right)", description)
    if build_match:
        return build_match.group(1)
    return None


def _get_metric_values(node: Any) -> dict[str, Any]:
    """Extract SQL metrics (Spark 3+ SQLMetric objects) from a plan node."""
    metrics: dict[str, Any] = {}
    try:
        metric_map = node.metrics()
        iterator = metric_map.iterator()
        while iterator.hasNext():
            entry = iterator.next()
            name = entry._1()
            metric_obj = entry._2()
            try:
                metrics[name] = metric_obj.value()
            except Exception:
                pass
    except Exception:
        pass
    return metrics


def _get_output_info(node: Any) -> list[str]:
    """Extract the output attributes (columns) of a plan node."""
    outputs: list[str] = []
    try:
        iterator = node.output().iterator()
        while iterator.hasNext():
            attr = iterator.next()
            outputs.append(attr.toString())
    except Exception:
        pass
    return outputs
