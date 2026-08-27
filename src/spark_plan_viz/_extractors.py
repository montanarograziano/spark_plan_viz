"""Extraction functions for parsing Spark plan node descriptions."""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from spark_plan_viz._constants import (
    MAX_DISPLAY_COLUMNS,
    MAX_DISPLAY_FILTERS,
    MAX_DISPLAY_FUNCTIONS,
    MAX_DISPLAY_GROUPING_KEYS,
    ROW_EXPLODING_GENERATORS,
)

_JOIN_TYPES = (
    "Inner",
    "LeftOuter",
    "RightOuter",
    "FullOuter",
    "LeftSemi",
    "LeftAnti",
    "Cross",
    "Existence",
)
_DATA_FORMATS = (
    "parquet",
    "orc",
    "json",
    "csv",
    "avro",
    "delta",
    "iceberg",
    "text",
)


def _iter_scala(seq: Any) -> Iterable[Any]:
    """Yield items from a Scala Seq/Iterable exposed via Py4J."""
    try:
        iterator = seq.iterator()
    except Exception:
        return
    while iterator.hasNext():
        yield iterator.next()


def _strip_expr_ids(text: str) -> str:
    """Remove Catalyst expression IDs like ``#12`` / ``#12L``."""
    return re.sub(r"#\d+L?", "", text)


def _extract_join_type(description: str) -> str | None:
    """Extract the type of join from the description."""
    # Prefer explicit join-type token after brackets / build side:
    #   SortMergeJoin [a], [b], Inner
    #   BroadcastHashJoin [...], [...], LeftOuter, BuildRight, false
    #   BroadcastNestedLoopJoin BuildRight, Cross, (cond)
    line = description.splitlines()[0] if description else ""
    type_match = re.search(
        rf"\b({'|'.join(_JOIN_TYPES)})\b",
        line,
    )
    if type_match:
        return type_match.group(1)

    match = re.search(
        rf"\b({'|'.join(_JOIN_TYPES)})\s*Join",
        description,
    )
    if match:
        return match.group(1)
    return None


def _extract_join_condition(description: str) -> str | None:
    """Extract join condition from the description."""
    explicit_match = re.search(r"\bcondition:\s*([^\n]+)", description, re.IGNORECASE)
    if explicit_match:
        return explicit_match.group(1).strip()

    line = description.splitlines()[0].strip() if description else ""
    if not line:
        return None

    # Physical join nodes often encode the predicate as the final comma-separated
    # segment, e.g.:
    #   BroadcastNestedLoopJoin BuildRight, Inner, (salary#1 > amount#2)
    #   BroadcastHashJoin [...], [...], Inner, BuildRight, false
    match = re.search(
        r"(?:,\s*)((?:\([^()\n]+\)|\(\([^)\n]+\)\)|\[[^\]\n]+\]))\s*$",
        line,
    )
    if match:
        return match.group(1).strip()
    return None


def _is_skew_join(description: str, name: str = "") -> bool:
    """Return True when the plan marks a join as skew-optimized (AQE)."""
    text = f"{name} {description}"
    if re.search(r"\bisSkew\s*=\s*true\b", text, re.IGNORECASE):
        return True
    if "OptimizeSkewedJoin" in text or "skew join" in text.lower():
        return True
    # SortMergeJoin [l], [r], Inner, true  — trailing boolean is isSkew
    line = description.splitlines()[0] if description else ""
    return bool(
        ("SortMergeJoin" in name or "SortMergeJoin" in line)
        and re.search(r",\s*true\s*$", line.strip())
    )


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
        cols = [_strip_expr_ids(c) for c in cols]
        return cols[:MAX_DISPLAY_COLUMNS]
    return []


def _extract_aggregate_functions(description: str) -> list[str]:
    """Extract aggregate functions from the description."""
    agg_funcs = re.findall(
        r"\b(sum|count|avg|min|max|first|last|collect_list|collect_set|"
        r"countDistinct|approx_count_distinct|percentile_approx|"
        r"array_agg|listagg)\s*\([^)]*\)",
        description,
        re.IGNORECASE,
    )
    return agg_funcs[:MAX_DISPLAY_FUNCTIONS]


def _extract_table_name(description: str) -> str | None:
    """Extract table/file name from scan operations."""
    match = re.search(r"(?:Location|Table):\s*([^\n,]+)", description)
    if match:
        path = match.group(1).strip()
        # Strip InMemoryFileIndex(...) wrappers when present
        path = re.sub(r"^InMemoryFileIndex\([^]]*?\]?", "", path).strip(" []")
        if "/" in path:
            parts = [p for p in path.split("/") if p]
            return parts[-1] if parts else path
        return path
    match = re.search(r"(?:FileScan|BatchScan)\s+\w+\s+([\w.]+)", description)
    if match:
        return match.group(1)
    match = re.search(r"(\w+\.)?(\w+)\.(\w+)", description)
    if match:
        return f"{match.group(2)}.{match.group(3)}"
    return None


def _is_broadcast_join(description: str, name: str) -> bool:
    """Check if this is a broadcast join (important for performance)."""
    if "BroadcastExchange" in name:
        return True
    return "Broadcast" in name or "broadcast" in description.lower()


def _extract_sort_order(description: str) -> str | None:
    """Extract sort order from Sort operations."""
    match = re.search(r"\[(.*?)\s+(ASC|DESC)", description, re.IGNORECASE)
    if match:
        col = _strip_expr_ids(match.group(1))
        order = match.group(2).upper()
        return f"{col} {order}"
    return None


def _extract_data_format(description: str) -> str | None:
    """Extract data format from scan operations (parquet, orc, json, etc)."""
    lower = description.lower()
    # Prefer explicit Format: field when present
    fmt_field = re.search(r"Format:\s*([A-Za-z0-9_]+)", description)
    if fmt_field:
        return fmt_field.group(1).upper()
    for fmt in _DATA_FORMATS:
        if fmt in lower:
            return fmt.upper()
    return None


def _extract_bracket_list(description: str, label: str) -> list[str] | None:
    """Extract a labeled ``Label: [...]`` list, or None if the label is absent."""
    match = re.search(rf"{label}:\s*\[([^\]]*)\]", description)
    if not match:
        return None
    inner = match.group(1).strip()
    if not inner:
        return []
    return [item.strip() for item in inner.split(",") if item.strip()]


def _extract_pushed_filters(description: str) -> list[str]:
    """Extract pushed down filters (important for performance)."""
    filters = _extract_bracket_list(description, r"PushedFilters?")
    if filters is None:
        return []
    return filters[:MAX_DISPLAY_FILTERS]


def _extract_partition_filters(description: str) -> list[str] | None:
    """Extract partition filters. None means the field is absent."""
    return _extract_bracket_list(description, "PartitionFilters")


def _extract_data_filters(description: str) -> list[str] | None:
    """Extract DataFilters from a scan description. None if absent."""
    return _extract_bracket_list(description, "DataFilters")


def _has_partition_columns(description: str) -> bool:
    """Heuristic: FileScan lists partition columns outside ReadSchema."""
    # FileScan parquet [id#1L, p#2] ... ReadSchema: struct<id:bigint>
    scan_cols = re.search(
        r"(?:FileScan|BatchScan)\s+\S+\s+\[([^\]]+)\]",
        description,
    )
    read_schema = re.search(r"ReadSchema:\s*struct<([^>]*)>", description)
    if not scan_cols or not read_schema:
        # PartitionFilters key present is weak signal alone
        return "PartitionFilters:" in description and "ReadSchema:" in description

    def _norm(col: str) -> str:
        col = _strip_expr_ids(col).strip().lower()
        # struct field "id:bigint" → id
        return col.split(":")[0].strip()

    output_cols = {_norm(c) for c in scan_cols.group(1).split(",") if c.strip()}
    schema_cols = {_norm(c) for c in read_schema.group(1).split(",") if c.strip()}
    # Partition columns appear in the scan output but not the file ReadSchema
    return bool(output_cols - schema_cols)


def _extract_grouping_keys(description: str) -> list[str]:
    """Extract grouping keys from aggregate operations."""
    match = re.search(r"keys=\[([^\]]+)\]", description)
    if match:
        keys = [k.strip() for k in match.group(1).split(",")]
        keys = [_strip_expr_ids(k) for k in keys]
        return keys[:MAX_DISPLAY_GROUPING_KEYS]
    return []


def _extract_shuffle_info(description: str) -> dict[str, str]:
    """Extract shuffle type and partition count from shuffle/exchange nodes."""
    info: dict[str, str] = {}
    if "SinglePartition" in description:
        info["shuffle_type"] = "SinglePartition"
        info["partitions"] = "1"
        return info

    if "RoundRobinPartitioning" in description:
        info["shuffle_type"] = "RoundRobin"
    else:
        shuffle_match = re.search(
            r"(hash|range)partitioning", description, re.IGNORECASE
        )
        if shuffle_match:
            info["shuffle_type"] = shuffle_match.group(1).capitalize()

    # Coalesced / local shuffle readers (AQE)
    if re.search(r"\bCoalesced\b", description):
        info["shuffle_type"] = info.get("shuffle_type", "Coalesced")
        info["aqe_coalesced"] = "true"
    if re.search(r"\bLocal\b", description) and "Reader" in description:
        info["aqe_local_reader"] = "true"

    partition_match = re.search(
        r"(?:partitioning\([^)]*,\s*|RoundRobinPartitioning\()(\d+)\)?",
        description,
        re.IGNORECASE,
    )
    if not partition_match:
        partition_match = re.search(r"(\d+)\s*partitions?", description, re.IGNORECASE)
    if partition_match:
        info["partitions"] = partition_match.group(1)

    return info


def _extract_build_side(description: str) -> str | None:
    """Extract which side is broadcast in a broadcast join."""
    build_match = re.search(
        r"Build(?:Side)?:?\s*(Left|Right)|Build(Left|Right)", description
    )
    if build_match:
        return build_match.group(1) or build_match.group(2)
    return None


def _extract_generator_name(description: str, name: str = "") -> str | None:
    """Extract generator function name from a Generate node."""
    text = f"{name} {description}"
    match = re.search(
        r"\b("
        + "|".join(sorted(ROW_EXPLODING_GENERATORS, key=len, reverse=True))
        + r")\b",
        text,
        re.IGNORECASE,
    )
    if match:
        return match.group(1).lower()
    # Generic Generate <fn>(...)
    gen_match = re.search(r"\bGenerate\s+([A-Za-z_][A-Za-z0-9_]*)", text)
    if gen_match:
        return gen_match.group(1).lower()
    return None


def _extract_expand_projections(description: str) -> int | None:
    """Count Expand projection groups: ``Expand [[...], [...], ...], [...]``."""
    match = re.search(r"Expand\s*\[(\[.*\])\]\s*,", description, re.DOTALL)
    if not match:
        # single-line form
        match = re.search(r"Expand\s*\[(\[.*\])\]", description)
    if not match:
        return None
    inner = match.group(1)
    # Count top-level [...] groups
    groups = re.findall(r"\[[^\[\]]*\]", inner)
    return len(groups) if groups else None


def _get_metric_values(node: Any) -> dict[str, Any]:
    """Extract SQL metrics (Spark 3+ SQLMetric objects) from a plan node."""
    metrics: dict[str, Any] = {}
    try:
        for entry in _iter_scala(node.metrics()):
            name = entry._1()
            metric_obj = entry._2()
            try:
                metrics[name] = metric_obj.value()
            except Exception:
                continue
    except Exception:
        pass
    return metrics


def _get_output_info(node: Any) -> list[str]:
    """Extract the output attributes (columns) of a plan node."""
    outputs: list[str] = []
    try:
        for attr in _iter_scala(node.output()):
            outputs.append(attr.toString())
    except Exception:
        pass
    return outputs
