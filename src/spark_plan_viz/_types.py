"""TypedDict definitions for spark_plan_viz — JSON-serializable for template injection."""

from __future__ import annotations

from typing import Any, TypedDict


class KeyInfo(TypedDict, total=False):
    """Optional key information extracted from a plan node."""

    join_type: str
    condition: str
    is_broadcast: bool
    build_side: str
    columns: list[str]
    functions: list[str]
    group_by: list[str]
    table: str
    format: str
    pushed_filters: list[str]
    order: str
    shuffle_type: str
    partitions: str
    is_shuffle: bool


class PlanNode(TypedDict):
    """A single node in the parsed Spark execution plan tree."""

    name: str
    description: str
    output: list[str]
    type: str
    key_info: KeyInfo
    children: list[PlanNode]
    metrics: dict[str, Any]
    suggestions: list[dict[str, str]]
