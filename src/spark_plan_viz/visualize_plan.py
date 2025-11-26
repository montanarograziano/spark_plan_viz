import base64
import json
import os
import webbrowser
from importlib.resources import files
from typing import Any

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame


def _extract_join_type(description: str) -> str | None:
    """Extract the type of join from the description."""
    import re

    # Match patterns like "Inner Join", "LeftOuter Join", etc.
    match = re.search(
        r"\b(Inner|LeftOuter|RightOuter|FullOuter|LeftSemi|LeftAnti|Cross)\s*Join",
        description,
    )
    if match:
        return match.group(1)
    return None


def _extract_join_condition(description: str) -> str | None:
    """Extract join condition from the description."""
    import re

    # Match patterns like "condition" or join predicates
    match = re.search(r"(?:condition|BuildSide):\s*([^\n]+)", description)
    if match:
        return match.group(1).strip()
    return None


def _extract_filter_condition(description: str) -> str | None:
    """Extract filter condition from the description."""
    import re

    # Match patterns after Filter keyword
    match = re.search(r"Filter\s+([^\n]+)", description)
    if match:
        condition = match.group(1).strip()
        # Clean up common prefixes
        condition = re.sub(r"^:\s*", "", condition)
        return condition
    return None


def _extract_selected_columns(description: str) -> list[str]:
    """Extract projected/selected columns from the description."""
    import re

    # Match patterns like [col1#123, col2#456]
    match = re.search(r"\[([^\]]+)\]", description)
    if match:
        cols_str = match.group(1)
        # Split by comma and clean up
        cols = [c.strip() for c in cols_str.split(",")]
        # Remove attribute IDs (e.g., #123)
        cols = [re.sub(r"#\d+L?", "", c) for c in cols]
        return cols[:5]  # Limit to first 5 columns to avoid clutter
    return []


def _extract_aggregate_functions(description: str) -> list[str]:
    """Extract aggregate functions from the description."""
    import re

    # Match common aggregate patterns
    agg_funcs = re.findall(
        r"\b(sum|count|avg|min|max|first|last|collect_list|collect_set)\s*\([^)]*\)",
        description,
        re.IGNORECASE,
    )
    return agg_funcs[:3]  # Limit to first 3


def _extract_table_name(description: str) -> str | None:
    """Extract table/file name from scan operations."""
    import re

    # Match patterns like "Location: /path/to/table" or table names
    match = re.search(r"(?:Location|Table):\s*([^\n,]+)", description)
    if match:
        path = match.group(1).strip()
        # Extract just the table name from path if it's a path
        if "/" in path:
            parts = path.split("/")
            return parts[-1] if parts[-1] else parts[-2]
        return path
    # Try to match catalog.schema.table pattern
    match = re.search(r"(\w+\.)?(\w+)\.(\w+)", description)
    if match:
        return f"{match.group(2)}.{match.group(3)}"
    return None


def _is_broadcast_join(description: str, name: str) -> bool:
    """Check if this is a broadcast join (important for performance)."""
    return "Broadcast" in name or "broadcast" in description.lower()


def _extract_sort_order(description: str) -> str | None:
    """Extract sort order from Sort operations."""
    import re

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
    import re

    # Look for PushedFilters pattern
    match = re.search(r"PushedFilters?:\s*\[([^\]]+)\]", description)
    if match:
        filters = [f.strip() for f in match.group(1).split(",")]
        return filters[:3]  # Limit to first 3
    return []


def _extract_grouping_keys(description: str) -> list[str]:
    """Extract grouping keys from aggregate operations."""
    import re

    # Match patterns like "keys=[col1#123, col2#456]"
    match = re.search(r"keys=\[([^\]]+)\]", description)
    if match:
        keys = [k.strip() for k in match.group(1).split(",")]
        # Remove attribute IDs
        keys = [re.sub(r"#\d+L?", "", k) for k in keys]
        return keys[:3]  # Limit to first 3
    return []


def _parse_spark_plan(df: DataFrame) -> dict[str, Any] | None:
    """
    Traverses the internal JVM SparkPlan object using Py4J.
    Designed for Spark 3.x+ with Adaptive Query Execution (AQE) support.
    Returns a dictionary representing the tree structure.
    """
    try:
        # Access the JVM Physical Plan directly
        plan: JavaObject = df._jdf.queryExecution().executedPlan()  # pyright: ignore[reportOptionalCall]
    except AttributeError:
        print(
            "Error: Could not access the execution plan. Ensure this is a PySpark DataFrame."
        )
        return None

    def _get_metric_values(node: Any) -> dict[str, Any]:
        # Extract SQL metrics (Spark 3+ SQLMetric objects)
        metrics: dict[str, Any] = {}
        try:
            metric_map = node.metrics()
            iterator = metric_map.iterator()
            while iterator.hasNext():
                entry = iterator.next()
                name = entry._1()
                metric_obj = entry._2()
                try:
                    # Spark 3 SQLMetric.value() return type varies, usually Long
                    metrics[name] = metric_obj.value()
                except Exception:
                    pass
        except Exception:
            pass
        return metrics

    def _get_output_info(node: JavaObject) -> list[str]:
        """Extracts the output attributes (columns) of the node."""
        outputs: list[str] = []
        try:
            # node.output() returns a Seq[Attribute]
            iterator = node.output().iterator()
            while iterator.hasNext():
                attr = iterator.next()
                outputs.append(attr.toString())
        except Exception:
            pass
        return outputs

    def _walk_node(node: JavaObject) -> dict[str, Any]:
        name = node.nodeName()

        # Spark 3+ prefer verboseStringWithSuffix for explain-like details
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

        # Spark 3+ specific keywords
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

            # Check if it's a broadcast join (important for performance)
            if _is_broadcast_join(description, name):
                key_info["is_broadcast"] = True
                # Extract which side is broadcast
                import re

                build_match = re.search(r"BuildSide:\s*(Left|Right)", description)
                if build_match:
                    key_info["build_side"] = build_match.group(1)

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

            # Extract grouping keys
            grouping_keys = _extract_grouping_keys(description)
            if grouping_keys:
                key_info["group_by"] = grouping_keys

        elif node_type == "scan":
            table = _extract_table_name(description)
            if table:
                key_info["table"] = table

            # Extract data format
            data_format = _extract_data_format(description)
            if data_format:
                key_info["format"] = data_format

            # Extract pushed filters (important for performance)
            pushed_filters = _extract_pushed_filters(description)
            if pushed_filters:
                key_info["pushed_filters"] = pushed_filters

        elif node_type == "sort":
            sort_order = _extract_sort_order(description)
            if sort_order:
                key_info["order"] = sort_order

        elif node_type == "shuffle":
            # Extract shuffle type (e.g., hashpartitioning, rangepartitioning)
            import re

            shuffle_match = re.search(
                r"(hash|range)partitioning", description, re.IGNORECASE
            )
            if shuffle_match:
                key_info["shuffle_type"] = shuffle_match.group(1).capitalize()

            # Extract partition count
            partition_match = re.search(
                r"(\d+)\s*partitions?", description, re.IGNORECASE
            )
            if partition_match:
                key_info["partitions"] = partition_match.group(1)

            # Mark as shuffle for visual emphasis
            key_info["is_shuffle"] = True

        data: dict[str, Any] = {
            "name": name,
            "description": description,
            "output": _get_output_info(node),
            "type": node_type,
            "key_info": key_info,
            "children": [],
            "metrics": _get_metric_values(node),
        }

        # --- Spark 3+ AQE & Traversal Logic ---
        children_nodes: list[Any] = []

        try:
            # 1. Handle AdaptiveSparkPlan (AQE Root)
            if "AdaptiveSparkPlan" in name:
                # The executedPlan() is the actual physical plan chosen by AQE
                final_plan = node.executedPlan()
                if final_plan:
                    children_nodes.append(final_plan)

            # 2. Handle QueryStages (Leaves in the main tree, but contain plans)
            elif "QueryStage" in name:
                # e.g., ShuffleQueryStageExec, BroadcastQueryStageExec
                # These wrap the actual physical operator (like ShuffleExchange)
                # We drill down using .plan() to show what happened inside the stage
                stage_plan = node.plan()
                if stage_plan:
                    children_nodes.append(stage_plan)

            # 3. Handle Reused Exchanges
            elif "ReusedExchange" in name:
                # Points to an existing exchange. Drill down to show the lineage.
                child = node.child()
                if child:
                    children_nodes.append(child)

            # 4. Standard Traversal
            else:
                children_seq = node.children()
                iterator = children_seq.iterator()
                while iterator.hasNext():
                    children_nodes.append(iterator.next())

            # Recursively walk whatever children we found
            for child in children_nodes:
                data["children"].append(_walk_node(child))

        except Exception as e:
            data["error"] = f"Traversal Error: {str(e)}"

        return data

    return _walk_node(plan)


def _build_html_string(tree_data: dict[str, Any]) -> str:
    """
    Injects the tree data into a D3.js HTML template and returns the HTML string.
    """
    # Load template from package resources
    template_path = files("spark_plan_viz").joinpath("template.html")
    template_content = template_path.read_text(encoding="utf-8")

    # Replace the placeholder with actual JSON data
    json_data = json.dumps(tree_data)
    html_content = template_content.replace("{{ TREE_DATA }}", json_data)

    return html_content


def visualize_plan(
    df: DataFrame,
    notebook: bool = True,
    output_file: str = "spark_plan_viz.html",
) -> None:
    """
    Main function to visualize a PySpark DataFrame's physical execution plan.

    Args:
        df: The PySpark DataFrame.
        notebook: Boolean, if True (default), renders inline in notebook. If False, saves to file and opens in browser.
        output_file: Name of the output HTML file (used only when notebook=False).
    """
    print("Parsing Spark Plan...")
    tree_data = _parse_spark_plan(df)

    if tree_data:
        html_content = _build_html_string(tree_data)

        if notebook:
            print("Rendering inline (notebook mode)...")
            try:
                from IPython.display import IFrame, display

                # Encode the HTML to Base64 to avoid srcdoc escaping issues and support iframe isolation
                b64_html = base64.b64encode(html_content.encode("utf-8")).decode(
                    "utf-8"
                )
                data_uri = f"data:text/html;base64,{b64_html}"

                display(IFrame(src=data_uri, width="100%", height=800))

            except ImportError:
                print("Error: IPython is not installed. Cannot display inline.")
        else:
            print(f"Generating Visualization at {output_file}...")
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(html_content)

            path = os.path.abspath(output_file)
            print(f"Done! File saved to: {path}")
            webbrowser.open("file://" + path)
    else:
        print("Failed to generate plan.")
