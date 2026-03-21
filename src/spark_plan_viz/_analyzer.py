"""Plan analysis engine — runs optimization rules against parsed plan trees."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame

from spark_plan_viz._parser import _parse_spark_plan
from spark_plan_viz._rules import ALL_RULES, AnalysisContext, Severity, Suggestion


def _flatten_tree(
    node: dict[str, Any],
    parent: dict[str, Any] | None,
    nodes: list[dict[str, Any]],
    parent_map: dict[int, dict[str, Any]],
) -> None:
    """Recursively flatten the tree into a list and build a parent map."""
    nodes.append(node)
    if parent is not None:
        parent_map[id(node)] = parent
    for child in node.get("children", []):
        _flatten_tree(child, node, nodes, parent_map)


def _build_context(tree: dict[str, Any]) -> AnalysisContext:
    """Build an AnalysisContext from a plan tree root."""
    ctx = AnalysisContext()
    _flatten_tree(tree, None, ctx.nodes, ctx.parent_map)
    return ctx


def _attach_suggestions(tree: dict[str, Any]) -> list[Suggestion]:
    """Run all optimization rules and attach suggestions to plan nodes.

    Modifies the tree in-place, populating each node's ``suggestions`` list.
    Returns the full list of suggestions (sorted by severity).
    """
    ctx = _build_context(tree)
    all_suggestions: list[Suggestion] = []

    # Deduplicate by (rule_id, node id)
    seen: set[tuple[str, int]] = set()

    for node in ctx.nodes:
        for rule in ALL_RULES:
            results = rule.check(node, ctx)
            for suggestion in results:
                key = (suggestion.rule_id, id(node))
                if key in seen:
                    continue
                seen.add(key)
                all_suggestions.append(suggestion)
                node.setdefault("suggestions", []).append(suggestion.to_dict())

    # Sort by severity: error > warning > info
    severity_order = {Severity.ERROR: 0, Severity.WARNING: 1, Severity.INFO: 2}
    all_suggestions.sort(key=lambda s: severity_order[s.severity])

    return all_suggestions


def analyze_plan(df: DataFrame) -> list[Suggestion]:
    """Parse the execution plan and return optimization suggestions.

    This is the public API for programmatic access to the optimization engine.

    Args:
        df: A PySpark DataFrame to analyze.

    Returns:
        A sorted, deduplicated list of Suggestion objects.

    Raises:
        ValueError: If the plan cannot be parsed.
    """
    tree = _parse_spark_plan(df)
    if tree is None:
        raise ValueError(
            "Could not parse the execution plan. Ensure this is a valid PySpark DataFrame."
        )
    return _attach_suggestions(tree)
