"""Spark execution plan parser — traverses JVM SparkPlan via Py4J."""

from __future__ import annotations

import logging
from typing import Any

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame

from spark_plan_viz._extractors import (
    _extract_aggregate_functions,
    _extract_build_side,
    _extract_data_format,
    _extract_expand_projections,
    _extract_filter_condition,
    _extract_generator_name,
    _extract_grouping_keys,
    _extract_join_condition,
    _extract_join_type,
    _extract_partition_filters,
    _extract_pushed_filters,
    _extract_selected_columns,
    _extract_shuffle_info,
    _extract_sort_order,
    _extract_table_name,
    _get_metric_values,
    _get_output_info,
    _has_partition_columns,
    _is_broadcast_join,
    _is_skew_join,
    _iter_scala,
)

logger = logging.getLogger("spark_plan_viz")

# Ordered (substring, type) pairs — first match wins.
_NODE_TYPE_RULES: tuple[tuple[str, str], ...] = (
    ("BroadcastExchange", "broadcast"),
    ("AQEShuffleRead", "shuffle_read"),
    ("CustomShuffleReader", "shuffle_read"),
    ("Exchange", "shuffle"),
    ("Shuffle", "shuffle"),
    ("BatchScan", "scan"),
    ("FileScan", "scan"),
    ("Scan", "scan"),
    ("Join", "join"),
    ("CartesianProduct", "join"),
    ("Filter", "filter"),
    ("Aggregate", "aggregate"),
    ("Expand", "expand"),
    ("Generate", "generate"),
    ("Sort", "sort"),
    ("Project", "project"),
    ("Window", "window"),
    ("Union", "union"),
    ("Range", "scan"),
    ("LocalTableScan", "scan"),
    ("InMemoryTableScan", "scan"),
)


def _classify_node_type(name: str) -> str:
    """Map a Spark physical node name to a visualization/analysis category."""
    for needle, node_type in _NODE_TYPE_RULES:
        if needle in name:
            return node_type
    return "other"


def _node_description(node: JavaObject) -> str:
    """Best-effort verbose description of a SparkPlan node."""
    try:
        return str(node.verboseStringWithSuffix())
    except Exception:
        try:
            return str(node.simpleString())
        except Exception:
            try:
                return str(node.toString())
            except Exception:
                return ""


def _extract_key_info(name: str, node_type: str, description: str) -> dict[str, Any]:
    """Populate type-specific key_info fields from a node description."""
    key_info: dict[str, Any] = {}

    if node_type == "join":
        join_type = _extract_join_type(description)
        join_cond = _extract_join_condition(description)
        if join_type:
            key_info["join_type"] = join_type
        if join_cond:
            key_info["condition"] = join_cond
        if _is_broadcast_join(description, name):
            key_info["is_broadcast"] = True
            build_side = _extract_build_side(description)
            if build_side:
                key_info["build_side"] = build_side
        if _is_skew_join(description, name):
            key_info["is_skew"] = True

    elif node_type == "filter":
        filter_cond = _extract_filter_condition(description)
        if filter_cond:
            key_info["condition"] = filter_cond

    elif node_type == "project":
        cols = _extract_selected_columns(description)
        if cols:
            key_info["columns"] = cols

    elif node_type == "aggregate":
        agg_funcs = _extract_aggregate_functions(description)
        if agg_funcs:
            key_info["functions"] = agg_funcs
        grouping_keys = _extract_grouping_keys(description)
        if grouping_keys:
            key_info["group_by"] = grouping_keys

    elif node_type == "scan":
        table = _extract_table_name(description)
        if table:
            key_info["table"] = table
        data_format = _extract_data_format(description)
        if data_format:
            key_info["format"] = data_format
        pushed_filters = _extract_pushed_filters(description)
        if pushed_filters:
            key_info["pushed_filters"] = pushed_filters
        partition_filters = _extract_partition_filters(description)
        if partition_filters is not None:
            key_info["partition_filters"] = partition_filters
        if _has_partition_columns(description):
            key_info["has_partition_columns"] = True

    elif node_type == "sort":
        sort_order = _extract_sort_order(description)
        if sort_order:
            key_info["order"] = sort_order

    elif node_type in {"shuffle", "shuffle_read", "broadcast"}:
        shuffle_info = _extract_shuffle_info(description)
        key_info.update(shuffle_info)
        if node_type == "shuffle":
            key_info["is_shuffle"] = True
        if node_type == "broadcast" or _is_broadcast_join(description, name):
            key_info["is_broadcast"] = True

    elif node_type == "generate":
        generator = _extract_generator_name(description, name)
        if generator:
            key_info["generator"] = generator

    elif node_type == "expand":
        groups = _extract_expand_projections(description)
        if groups is not None:
            key_info["expand_groups"] = groups

    return key_info


def _child_plans(node: JavaObject, name: str) -> list[Any]:
    """Resolve child SparkPlan nodes, including AQE wrappers (Spark 3/4)."""
    children_nodes: list[Any] = []

    if "AdaptiveSparkPlan" in name:
        final_plan = node.executedPlan()
        if final_plan:
            children_nodes.append(final_plan)
        return children_nodes

    if "QueryStage" in name:
        try:
            stage_plan = node.plan()
            if stage_plan:
                children_nodes.append(stage_plan)
                return children_nodes
        except Exception:
            pass

    # Single-child AQE / reuse wrappers (Spark 3.2+ and 4.x)
    single_child_wrappers = (
        "ReusedExchange",
        "AQEShuffleRead",
        "CustomShuffleReader",
        "InputAdapter",
        "WholeStageCodegen",
    )
    if any(w in name for w in single_child_wrappers):
        try:
            child = node.child()
            if child is not None:
                children_nodes.append(child)
                return children_nodes
        except Exception:
            pass

    children_nodes.extend(_iter_scala(node.children()))
    return children_nodes


def _walk_node(node: JavaObject) -> dict[str, Any]:
    """Recursively walk a SparkPlan node and build a tree dict."""
    name = str(node.nodeName())
    description = _node_description(node)
    node_type = _classify_node_type(name)
    key_info = _extract_key_info(name, node_type, description)

    data: dict[str, Any] = {
        "name": name,
        "description": description,
        "output": _get_output_info(node),
        "type": node_type,
        "key_info": key_info,
        "children": [],
        "metrics": _get_metric_values(node),
        "suggestions": [],
    }

    try:
        for child in _child_plans(node, name):
            data["children"].append(_walk_node(child))
    except Exception:
        logger.debug("Error traversing children of node %s", name, exc_info=True)

    return data


def _parse_spark_plan(df: DataFrame) -> dict[str, Any] | None:
    """
    Traverse the internal JVM SparkPlan object using Py4J.
    Designed for Spark 3.x / 4.x with Adaptive Query Execution (AQE) support.
    Returns a dictionary representing the tree structure.
    """
    try:
        plan: JavaObject = df._jdf.queryExecution().executedPlan()  # pyright: ignore[reportOptionalCall]
    except AttributeError:
        logger.warning(
            "Could not access the execution plan. Ensure this is a PySpark DataFrame."
        )
        return None

    return _walk_node(plan)
