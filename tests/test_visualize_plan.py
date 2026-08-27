from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any
from unittest.mock import Mock, patch

import pytest

from spark_plan_viz import visualize_plan as package_visualize_plan
from spark_plan_viz.api import (
    _build_html_string,
    _parse_spark_plan,
    visualize_plan,
)

try:
    from pyspark.sql import SparkSession

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

JAVA_AVAILABLE = bool(os.environ.get("JAVA_HOME")) and os.path.exists(
    os.path.join(os.environ.get("JAVA_HOME", ""), "bin", "java")
)


class ScalaIterator:
    """Mock of a Scala iterator exposing hasNext()/next() over a Python list."""

    def __init__(self, items: list[Any] | None = None) -> None:
        self.items = items or []
        self.index = 0

    def hasNext(self) -> bool:
        return self.index < len(self.items)

    def next(self) -> Any:
        item = self.items[self.index]
        self.index += 1
        return item


def _make_output_attr(value: str) -> Mock:
    attr = Mock()
    attr.toString = Mock(return_value=value)
    return attr


def _make_metric_entry(name: str, value: int) -> Mock:
    metric_obj = Mock()
    metric_obj.value = Mock(return_value=value)
    entry = Mock()
    entry._1 = Mock(return_value=name)
    entry._2 = Mock(return_value=metric_obj)
    return entry


@pytest.fixture
def make_plan() -> Callable[..., Mock]:
    """Factory fixture for building a mock Spark plan node."""

    def _build(
        name: str = "Filter",
        description: str | None = None,
        output: list[Mock] | None = None,
        metrics: list[Mock] | None = None,
        children: list[Mock] | None = None,
        child: Mock | None = None,
        executed_plan: Mock | None = None,
    ) -> Mock:
        plan = Mock()
        plan.nodeName.return_value = name
        plan.verboseStringWithSuffix.return_value = (
            description if description is not None else f"{name} details"
        )
        plan.output.return_value.iterator.return_value = ScalaIterator(output)
        plan.metrics.return_value.iterator.return_value = ScalaIterator(metrics)
        plan.children.return_value.iterator.return_value = ScalaIterator(children)
        # AQE / single-child wrappers — default to no child so Mock doesn't recurse
        plan.child.return_value = child
        plan.executedPlan.return_value = executed_plan
        plan.plan.return_value = child
        return plan

    return _build


@pytest.fixture
def make_df() -> Callable[[Mock], Mock]:
    """Factory fixture wrapping a plan into a mock DataFrame."""

    def _build(plan: Mock) -> Mock:
        df = Mock()
        df._jdf.queryExecution.return_value.executedPlan.return_value = plan
        return df

    return _build


@pytest.fixture
def sample_tree() -> dict[str, Any]:
    return {
        "name": "Root",
        "description": "",
        "type": "other",
        "children": [],
        "metrics": {},
        "output": [],
        "suggestions": [],
    }


# --- _parse_spark_plan -------------------------------------------------------


def test_parse_spark_plan_invalid_dataframe() -> None:
    invalid_df = Mock()
    del invalid_df._jdf
    assert _parse_spark_plan(invalid_df) is None


def test_parse_spark_plan_basic_structure(
    make_plan: Callable[..., Mock], make_df: Callable[[Mock], Mock]
) -> None:
    df = make_df(make_plan(name="Filter", description="Filter (id > 10)"))

    result = _parse_spark_plan(df)

    assert result is not None
    assert result["name"] == "Filter"
    assert result["description"] == "Filter (id > 10)"
    assert result["type"] == "filter"
    assert result["children"] == []


@pytest.mark.parametrize(
    "node_name,expected_type",
    [
        ("Exchange", "shuffle"),
        ("ShuffleExchange", "shuffle"),
        ("BroadcastExchange", "broadcast"),
        ("AQEShuffleRead", "shuffle_read"),
        ("CustomShuffleReader", "shuffle_read"),
        ("FileScan", "scan"),
        ("BatchScan", "scan"),
        ("HashJoin", "join"),
        ("Filter", "filter"),
        ("HashAggregate", "aggregate"),
        ("Expand", "expand"),
        ("Generate", "generate"),
        ("Sort", "sort"),
        ("Project", "project"),
        ("Window", "window"),
        ("Union", "union"),
        ("Unknown", "other"),
    ],
)
def test_parse_spark_plan_node_types(
    node_name: str,
    expected_type: str,
    make_plan: Callable[..., Mock],
    make_df: Callable[[Mock], Mock],
) -> None:
    df = make_df(make_plan(name=node_name))
    result = _parse_spark_plan(df)

    assert result is not None
    assert result["type"] == expected_type


def test_parse_spark_plan_expand_and_scan_key_info(
    make_plan: Callable[..., Mock], make_df: Callable[[Mock], Mock]
) -> None:
    expand = make_plan(
        name="Expand",
        description=(
            "Expand [[a#1, b#2, 0], [a#1, null, 1], [null, b#2, 2], [null, null, 3]], "
            "[a#3, b#4, spark_grouping_id#5]"
        ),
    )
    result = _parse_spark_plan(make_df(expand))
    assert result is not None
    assert result["type"] == "expand"
    assert result["key_info"].get("expand_groups") == 4

    scan_desc = (
        "FileScan parquet [id#67L,p#68] Batched: true, DataFilters: [], "
        "Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/t], "
        "PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:bigint>"
    )
    scan = make_plan(name="FileScan parquet", description=scan_desc)
    scan_result = _parse_spark_plan(make_df(scan))
    assert scan_result is not None
    assert scan_result["key_info"].get("format") == "PARQUET"
    assert scan_result["key_info"].get("has_partition_columns") is True
    assert scan_result["key_info"].get("partition_filters") == []


def test_parse_spark_plan_with_metrics(
    make_plan: Callable[..., Mock], make_df: Callable[[Mock], Mock]
) -> None:
    df = make_df(make_plan(name="Scan", metrics=[_make_metric_entry("numRows", 1000)]))

    result = _parse_spark_plan(df)

    assert result is not None
    assert result["metrics"] == {"numRows": 1000}


def test_parse_spark_plan_with_output(
    make_plan: Callable[..., Mock], make_df: Callable[[Mock], Mock]
) -> None:
    df = make_df(
        make_plan(
            name="Project",
            output=[_make_output_attr("id#123"), _make_output_attr("name#456")],
        )
    )

    result = _parse_spark_plan(df)

    assert result is not None
    assert result["output"] == ["id#123", "name#456"]


def test_parse_spark_plan_with_children(
    make_plan: Callable[..., Mock], make_df: Callable[[Mock], Mock]
) -> None:
    child = make_plan(name="Scan", description="Scan table")
    df = make_df(make_plan(name="Join", description="Join on id", children=[child]))

    result = _parse_spark_plan(df)

    assert result is not None
    assert len(result["children"]) == 1
    assert result["children"][0]["name"] == "Scan"


def test_parse_spark_plan_adaptive_spark_plan(
    make_plan: Callable[..., Mock], make_df: Callable[[Mock], Mock]
) -> None:
    executed = make_plan(name="Filter", description="Filter optimized")
    aqe = make_plan(name="AdaptiveSparkPlan", description="AQE enabled")
    aqe.executedPlan.return_value = executed

    result = _parse_spark_plan(make_df(aqe))

    assert result is not None
    assert result["name"] == "AdaptiveSparkPlan"
    assert len(result["children"]) == 1
    assert result["children"][0]["name"] == "Filter"


def test_parse_spark_plan_has_suggestions_field(
    make_plan: Callable[..., Mock], make_df: Callable[[Mock], Mock]
) -> None:
    result = _parse_spark_plan(make_df(make_plan()))

    assert result is not None
    assert result["suggestions"] == []


# --- package surface ---------------------------------------------------------


def test_package_visualize_plan_remains_callable() -> None:
    assert callable(package_visualize_plan)


# --- _build_html_string ------------------------------------------------------


@pytest.mark.parametrize(
    "tree_overrides,expected_content",
    [
        (
            {"name": "Filter", "description": "Filter (id > 10)", "type": "filter"},
            ["<!DOCTYPE html>", "Spark Physical Plan", "d3.v7.min.js", "Filter"],
        ),
        (
            {
                "name": "TestNode",
                "description": "Test Description",
                "metrics": {"rows": 100},
                "output": ["col1", "col2"],
            },
            ["<!DOCTYPE html>", "Spark Physical Plan", "d3.v7.min.js"],
        ),
    ],
)
def test_build_html_structure_and_data(
    sample_tree: dict[str, Any],
    tree_overrides: dict[str, Any],
    expected_content: list[str],
) -> None:
    html = _build_html_string({**sample_tree, **tree_overrides})

    for content in expected_content:
        assert content in html


@pytest.mark.parametrize(
    "expected_element",
    [
        "d3.select",
        "tree-container",
        "details-panel",
        "zoomIn",
        "zoomOut",
        "suggestions-panel",
        "escapeHtml",
    ],
)
def test_build_html_contains_ui_element(
    sample_tree: dict[str, Any], expected_element: str
) -> None:
    assert expected_element in _build_html_string(sample_tree)


def test_build_html_xss_escaping(sample_tree: dict[str, Any]) -> None:
    tree = {
        **sample_tree,
        "description": '</script><script>alert("xss")</script>',
    }

    html = _build_html_string(tree)

    assert "</script><script>alert" not in html
    assert "<\\/script>" in html


# --- visualize_plan ----------------------------------------------------------


def test_visualize_plan_notebook_mode(sample_tree: dict[str, Any]) -> None:
    with (
        patch("spark_plan_viz._renderer._parse_spark_plan", return_value=sample_tree),
        patch("IPython.display.display") as mock_display,
        patch("IPython.display.IFrame") as mock_iframe,
    ):
        result = visualize_plan(Mock(), notebook=True, analyze=False)

    mock_display.assert_called_once()
    mock_iframe.assert_called_once()
    assert result == sample_tree


def test_visualize_plan_file_mode(sample_tree: dict[str, Any]) -> None:
    with (
        patch("spark_plan_viz._renderer._parse_spark_plan", return_value=sample_tree),
        patch("spark_plan_viz._renderer.webbrowser.open") as mock_browser,
        patch("builtins.open", create=True) as mock_open,
    ):
        result = visualize_plan(
            Mock(), notebook=False, output_file="test.html", analyze=False
        )

    mock_open.assert_called_once()
    mock_browser.assert_called_once()
    assert result is not None


def test_visualize_plan_file_mode_without_browser(sample_tree: dict[str, Any]) -> None:
    with (
        patch("spark_plan_viz._renderer._parse_spark_plan", return_value=sample_tree),
        patch("spark_plan_viz._renderer.webbrowser.open") as mock_browser,
        patch("builtins.open", create=True) as mock_open,
    ):
        result = visualize_plan(
            Mock(),
            notebook=False,
            output_file="test.html",
            analyze=False,
            open_browser=False,
        )

    mock_open.assert_called_once()
    mock_browser.assert_not_called()
    assert result is not None


def test_visualize_plan_parse_failure() -> None:
    with patch("spark_plan_viz._renderer._parse_spark_plan", return_value=None):
        assert visualize_plan(Mock(), notebook=True) is None


def test_visualize_plan_notebook_no_ipython(sample_tree: dict[str, Any]) -> None:
    import builtins

    original_import = builtins.__import__

    def mock_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "IPython.display":
            raise ImportError("IPython not available")
        return original_import(name, *args, **kwargs)

    with (
        patch("spark_plan_viz._renderer._parse_spark_plan", return_value=sample_tree),
        patch("builtins.__import__", side_effect=mock_import),
    ):
        result = visualize_plan(Mock(), notebook=True, analyze=False)

    assert result is not None


def test_visualize_plan_returns_tree(sample_tree: dict[str, Any]) -> None:
    with (
        patch("spark_plan_viz._renderer._parse_spark_plan", return_value=sample_tree),
        patch("IPython.display.display"),
        patch("IPython.display.IFrame"),
    ):
        result = visualize_plan(Mock(), notebook=True, analyze=False)

    assert result == sample_tree


@pytest.mark.skipif(
    not PYSPARK_AVAILABLE or not JAVA_AVAILABLE,
    reason="PySpark not installed or Java not available",
)
def test_visualize_plan_real_dataframe() -> None:
    spark = SparkSession.Builder().appName("SparkPlanVizTest").getOrCreate()
    try:
        employees_df = spark.createDataFrame(
            [
                (1, "Alice", 34, "Engineering"),
                (2, "Bob", 45, "Sales"),
                (3, "Cathy", 29, "Engineering"),
                (4, "David", 38, "Marketing"),
                (5, "Eve", 42, "Sales"),
            ],
            ["id", "name", "age", "department"],
        )
        salaries_df = spark.createDataFrame(
            [(1, 95000), (2, 85000), (3, 78000), (4, 72000), (5, 88000)],
            ["emp_id", "salary"],
        )
        departments_df = spark.createDataFrame(
            [
                ("Engineering", "Tech"),
                ("Sales", "Business"),
                ("Marketing", "Business"),
            ],
            ["dept_name", "division"],
        )

        result_df = (
            employees_df.filter(employees_df.age > 30)
            .join(salaries_df, employees_df.id == salaries_df.emp_id, "inner")
            .join(
                departments_df,
                employees_df.department == departments_df.dept_name,
                "left",
            )
            .filter(salaries_df.salary > 80000)
            .groupBy("division")
            .agg({"salary": "avg", "age": "max"})
            .sort("division")
        )

        result = visualize_plan(
            result_df, notebook=True, output_file="test_real_df.html"
        )
        assert result is not None
    finally:
        spark.stop()
