from unittest.mock import Mock, patch

import pytest

from spark_plan_viz.visualize_plan import (
    _build_html_string,
    _parse_spark_plan,
    visualize_plan,
)


# Helper classes for mocking Scala-like iterators
class EmptyIterator:
    """Mock iterator that returns no items."""

    def hasNext(self) -> bool:
        return False


class ItemsIterator:
    """Mock iterator that returns a list of items."""

    def __init__(self, items: list) -> None:
        self.items = items
        self.index = 0

    def hasNext(self) -> bool:
        return self.index < len(self.items)

    def next(self) -> Mock:
        item = self.items[self.index]
        self.index += 1
        return item


class TestParseSparkPlan:
    """Test the _parse_spark_plan function."""

    def test_parse_spark_plan_invalid_dataframe(self) -> None:
        """Test that invalid DataFrame returns None."""
        invalid_df = Mock()
        del invalid_df._jdf  # Remove the _jdf attribute

        result = _parse_spark_plan(invalid_df)
        assert result is None

    def test_parse_spark_plan_basic_structure(self) -> None:
        """Test parsing a simple plan structure."""
        mock_df = Mock()
        mock_plan = Mock()
        mock_plan.nodeName.return_value = "Filter"
        mock_plan.verboseStringWithSuffix.return_value = "Filter (id > 10)"
        mock_plan.output.return_value.iterator.return_value = EmptyIterator()
        mock_plan.metrics.return_value.iterator.return_value = EmptyIterator()
        mock_plan.children.return_value.iterator.return_value = EmptyIterator()

        mock_df._jdf.queryExecution.return_value.executedPlan.return_value = mock_plan

        result = _parse_spark_plan(mock_df)

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
            ("FileScan", "scan"),
            ("BatchScan", "scan"),
            ("HashJoin", "join"),
            ("Filter", "filter"),
            ("HashAggregate", "aggregate"),
            ("Sort", "sort"),
            ("Project", "project"),
            ("Window", "window"),
            ("Union", "union"),
            ("Unknown", "other"),
        ],
    )
    def test_parse_spark_plan_node_types(
        self, node_name: str, expected_type: str
    ) -> None:
        """Test that different node types are categorized correctly."""
        mock_df = Mock()
        mock_plan = Mock()
        mock_plan.nodeName.return_value = node_name
        mock_plan.verboseStringWithSuffix.return_value = f"{node_name} details"
        mock_plan.output.return_value.iterator.return_value = EmptyIterator()
        mock_plan.metrics.return_value.iterator.return_value = EmptyIterator()
        mock_plan.children.return_value.iterator.return_value = EmptyIterator()

        mock_df._jdf.queryExecution.return_value.executedPlan.return_value = mock_plan

        result = _parse_spark_plan(mock_df)
        assert result is not None
        assert result["type"] == expected_type

    def test_parse_spark_plan_with_metrics(self) -> None:
        """Test parsing plan with metrics."""
        mock_df = Mock()
        mock_plan = Mock()
        mock_plan.nodeName.return_value = "Scan"
        mock_plan.verboseStringWithSuffix.return_value = "Scan table"

        # Mock metrics
        mock_metric_obj = Mock()
        mock_metric_obj.value = Mock(return_value=1000)
        mock_entry = Mock()
        mock_entry._1 = Mock(return_value="numRows")
        mock_entry._2 = Mock(return_value=mock_metric_obj)

        mock_metrics = Mock()
        mock_metrics.iterator.return_value = ItemsIterator([mock_entry])
        mock_plan.metrics.return_value = mock_metrics

        mock_plan.output.return_value.iterator.return_value = EmptyIterator()
        mock_plan.children.return_value.iterator.return_value = EmptyIterator()

        mock_df._jdf.queryExecution.return_value.executedPlan.return_value = mock_plan

        result = _parse_spark_plan(mock_df)

        assert result is not None
        assert "metrics" in result
        assert result["metrics"]["numRows"] == 1000

    def test_parse_spark_plan_with_output(self) -> None:
        """Test parsing plan with output columns."""
        mock_df = Mock()
        mock_plan = Mock()
        mock_plan.nodeName.return_value = "Project"
        mock_plan.verboseStringWithSuffix.return_value = "Project [id, name]"

        # Mock output attributes
        mock_attr1 = Mock()
        mock_attr1.toString = Mock(return_value="id#123")
        mock_attr2 = Mock()
        mock_attr2.toString = Mock(return_value="name#456")

        mock_output = Mock()
        mock_output.iterator.return_value = ItemsIterator([mock_attr1, mock_attr2])
        mock_plan.output.return_value = mock_output

        mock_plan.metrics.return_value.iterator.return_value = EmptyIterator()
        mock_plan.children.return_value.iterator.return_value = EmptyIterator()

        mock_df._jdf.queryExecution.return_value.executedPlan.return_value = mock_plan

        result = _parse_spark_plan(mock_df)

        assert result is not None
        assert result["output"] == ["id#123", "name#456"]

    def test_parse_spark_plan_with_children(self) -> None:
        """Test parsing plan with child nodes."""
        mock_df = Mock()

        # Child node
        mock_child = Mock()
        mock_child.nodeName = Mock(return_value="Scan")
        mock_child.verboseStringWithSuffix = Mock(return_value="Scan table")
        mock_child.output.return_value.iterator.return_value = EmptyIterator()
        mock_child.metrics.return_value.iterator.return_value = EmptyIterator()
        mock_child.children.return_value.iterator.return_value = EmptyIterator()

        # Parent node
        mock_parent = Mock()
        mock_parent.nodeName = Mock(return_value="Join")
        mock_parent.verboseStringWithSuffix = Mock(return_value="Join on id")
        mock_parent.output.return_value.iterator.return_value = EmptyIterator()
        mock_parent.metrics.return_value.iterator.return_value = EmptyIterator()
        mock_parent.children.return_value.iterator.return_value = ItemsIterator(
            [mock_child]
        )

        mock_df._jdf.queryExecution.return_value.executedPlan.return_value = mock_parent

        result = _parse_spark_plan(mock_df)

        assert result is not None
        assert len(result["children"]) == 1
        assert result["children"][0]["name"] == "Scan"

    def test_parse_spark_plan_adaptive_spark_plan(self) -> None:
        """Test parsing AdaptiveSparkPlan."""
        mock_df = Mock()
        mock_aqe = Mock()
        mock_aqe.nodeName.return_value = "AdaptiveSparkPlan"
        mock_aqe.verboseStringWithSuffix.return_value = "AQE enabled"
        mock_aqe.output.return_value.iterator.return_value = iter([])
        mock_aqe.metrics.return_value.iterator.return_value = iter([])

        # Mock executed plan
        mock_executed = Mock()
        mock_executed.nodeName.return_value = "Filter"
        mock_executed.verboseStringWithSuffix.return_value = "Filter optimized"
        mock_executed.output.return_value.iterator.return_value = iter([])
        mock_executed.metrics.return_value.iterator.return_value = iter([])
        mock_executed.children.return_value.iterator.return_value = iter([])

        mock_aqe.executedPlan.return_value = mock_executed

        mock_df._jdf.queryExecution.return_value.executedPlan.return_value = mock_aqe

        result = _parse_spark_plan(mock_df)

        assert result is not None
        assert result["name"] == "AdaptiveSparkPlan"
        assert len(result["children"]) == 1
        assert result["children"][0]["name"] == "Filter"


class TestBuildHtmlString:
    """Test the _build_html_string function."""

    @pytest.mark.parametrize(
        "tree_data,expected_content",
        [
            (
                {
                    "name": "Filter",
                    "description": "Filter (id > 10)",
                    "type": "filter",
                    "children": [],
                    "metrics": {},
                    "output": [],
                },
                ["<!DOCTYPE html>", "Spark Physical Plan", "d3.v7.min.js", "Filter"],
            ),
            (
                {
                    "name": "TestNode",
                    "description": "Test Description",
                    "type": "other",
                    "children": [],
                    "metrics": {"rows": 100},
                    "output": ["col1", "col2"],
                },
                ["TestNode", "Test Description", '"rows": 100'],
            ),
        ],
    )
    def test_build_html_structure_and_data(
        self, tree_data: dict, expected_content: list[str]
    ) -> None:
        """Test that HTML is generated with correct structure and embedded data."""
        html = _build_html_string(tree_data)

        for content in expected_content:
            assert content in html

    def test_build_html_has_d3_visualization(self) -> None:
        """Test that HTML contains D3.js visualization elements."""
        tree_data = {
            "name": "Root",
            "description": "",
            "type": "other",
            "children": [],
            "metrics": {},
            "output": [],
        }

        html = _build_html_string(tree_data)

        # Check for D3.js elements
        required_elements = [
            "d3.select",
            "tree-container",
            "details-panel",
            "zoomIn",
            "zoomOut",
        ]
        for element in required_elements:
            assert element in html


class TestVisualizePlan:
    """Test the visualize_plan function."""

    @pytest.fixture
    def mock_tree(self) -> dict:
        """Fixture for mock tree data."""
        return {
            "name": "Test",
            "description": "",
            "type": "other",
            "children": [],
            "metrics": {},
            "output": [],
        }

    @patch("spark_plan_viz.visualize_plan._parse_spark_plan")
    @patch("IPython.display.display")
    @patch("IPython.display.IFrame")
    def test_visualize_plan_notebook_mode(
        self, mock_iframe: Mock, mock_display: Mock, mock_parse: Mock, mock_tree: dict
    ) -> None:
        """Test notebook mode displays inline."""
        mock_df = Mock()
        mock_parse.return_value = mock_tree

        visualize_plan(mock_df, notebook=True)

        mock_parse.assert_called_once_with(mock_df)
        mock_display.assert_called_once()
        mock_iframe.assert_called_once()

    @patch("spark_plan_viz.visualize_plan._parse_spark_plan")
    @patch("spark_plan_viz.visualize_plan.webbrowser.open")
    @patch("builtins.open", create=True)
    def test_visualize_plan_file_mode(
        self, mock_open: Mock, mock_browser: Mock, mock_parse: Mock, mock_tree: dict
    ) -> None:
        """Test file mode saves and opens in browser."""
        mock_df = Mock()
        mock_parse.return_value = mock_tree

        visualize_plan(mock_df, notebook=False, output_file="test.html")

        mock_parse.assert_called_once_with(mock_df)
        mock_open.assert_called_once()
        mock_browser.assert_called_once()

    @patch("spark_plan_viz.visualize_plan._parse_spark_plan")
    def test_visualize_plan_parse_failure(self, mock_parse: Mock) -> None:
        """Test that function handles parse failure gracefully."""
        mock_df = Mock()
        mock_parse.return_value = None

        # Should not raise exception
        visualize_plan(mock_df, notebook=True)

        mock_parse.assert_called_once_with(mock_df)

    @patch("spark_plan_viz.visualize_plan._parse_spark_plan")
    def test_visualize_plan_notebook_no_ipython(self, mock_parse: Mock) -> None:
        """Test notebook mode handles missing IPython gracefully."""
        import builtins

        mock_df = Mock()
        mock_tree = {
            "name": "Test",
            "description": "",
            "type": "other",
            "children": [],
            "metrics": {},
            "output": [],
        }
        mock_parse.return_value = mock_tree

        # Mock the import to raise ImportError
        original_import = builtins.__import__

        def mock_import(name: str, *args, **kwargs):  # type: ignore
            if name == "IPython.display":
                raise ImportError("IPython not available")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            # Should not raise exception
            visualize_plan(mock_df, notebook=True)

        mock_parse.assert_called_once_with(mock_df)
