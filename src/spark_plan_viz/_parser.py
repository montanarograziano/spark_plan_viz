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
    _extract_filter_condition,
    _extract_grouping_keys,
    _extract_join_condition,
    _extract_join_type,
    _extract_pushed_filters,
    _extract_selected_columns,
    _extract_shuffle_info,
    _extract_sort_order,
    _extract_table_name,
    _get_metric_values,
    _get_output_info,
    _is_broadcast_join,
)

logger = logging.getLogger("spark_plan_viz")


def _walk_node(node: JavaObject) -> dict[str, Any]:
    """Recursively walk a SparkPlan node and build a tree dict."""
    name = node.nodeName()

    description: str = ""
    try:
        description = node.verboseStringWithSuffix()
    except Exception:
        try:
            description = node.simpleString()
        except Exception:
            description = node.toString()

    # Categorize node for coloring
    node_type = "other"
    if "Exchange" in name or "Shuffle" in name:
        node_type = "shuffle"
    elif "Scan" in name or "BatchScan" in name or "FileScan" in name:
        node_type = "scan"
    elif "Join" in name:
        node_type = "join"
    elif "Filter" in name:
        node_type = "filter"
    elif "Aggregate" in name:
        node_type = "aggregate"
    elif "Sort" in name:
        node_type = "sort"
    elif "Project" in name:
        node_type = "project"
    elif "Window" in name:
        node_type = "window"
    elif "Union" in name:
        node_type = "union"

    # Extract specific information based on node type
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

    elif node_type == "sort":
        sort_order = _extract_sort_order(description)
        if sort_order:
            key_info["order"] = sort_order

    elif node_type == "shuffle":
        shuffle_info = _extract_shuffle_info(description)
        key_info.update(shuffle_info)
        key_info["is_shuffle"] = True

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

    # --- Spark 3+ AQE & Traversal Logic ---
    children_nodes: list[Any] = []

    try:
        if "AdaptiveSparkPlan" in name:
            final_plan = node.executedPlan()
            if final_plan:
                children_nodes.append(final_plan)
        elif "QueryStage" in name:
            stage_plan = node.plan()
            if stage_plan:
                children_nodes.append(stage_plan)
        elif "ReusedExchange" in name:
            child = node.child()
            if child:
                children_nodes.append(child)
        else:
            children_seq = node.children()
            iterator = children_seq.iterator()
            while iterator.hasNext():
                children_nodes.append(iterator.next())

        for child in children_nodes:
            data["children"].append(_walk_node(child))

    except Exception:
        logger.debug("Error traversing children of node %s", name, exc_info=True)

    return data


def _parse_spark_plan(df: DataFrame) -> dict[str, Any] | None:
    """
    Traverse the internal JVM SparkPlan object using Py4J.
    Designed for Spark 3.x+ with Adaptive Query Execution (AQE) support.
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
