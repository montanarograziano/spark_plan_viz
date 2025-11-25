import base64
import json
import os
import webbrowser
from importlib.resources import files
from typing import Any

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame


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
        node.outpu
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

        data: dict[str, Any] = {
            "name": name,
            "description": description,
            "output": _get_output_info(node),
            "type": node_type,
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
