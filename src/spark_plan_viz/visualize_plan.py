import base64
import json
import os
import webbrowser
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame


def _parse_spark_plan(df: DataFrame) -> Optional[Dict[str, Any]]:
    """
    Traverses the internal JVM SparkPlan object using Py4J.
    Designed for Spark 3.x+ with Adaptive Query Execution (AQE) support.
    Returns a dictionary representing the tree structure.
    """
    try:
        # Access the JVM Physical Plan directly
        plan = df._jdf.queryExecution().executedPlan()  # pyright: ignore[reportOptionalCall]
    except AttributeError:
        print(
            "Error: Could not access the execution plan. Ensure this is a PySpark DataFrame."
        )
        return None

    def _get_metric_values(node: Any) -> Dict[str, Any]:
        # Extract SQL metrics (Spark 3+ SQLMetric objects)
        metrics: Dict[str, Any] = {}
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

    def _get_output_info(node: Any) -> List[str]:
        """Extracts the output attributes (columns) of the node."""
        outputs: List[str] = []
        try:
            # node.output() returns a Seq[Attribute]
            iterator = node.output().iterator()
            while iterator.hasNext():
                attr = iterator.next()
                outputs.append(attr.toString())
        except Exception:
            pass
        return outputs

    def _walk_node(node: Any) -> Dict[str, Any]:
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

        data: Dict[str, Any] = {
            "name": name,
            "description": description,
            "output": _get_output_info(node),
            "type": node_type,
            "children": [],
            "metrics": _get_metric_values(node),
        }

        # --- Spark 3+ AQE & Traversal Logic ---
        children_nodes: List[Any] = []

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


def _build_html_string(tree_data: Dict[str, Any]) -> str:
    """
    Injects the tree data into a D3.js HTML template and returns the HTML string.
    """
    json_data = json.dumps(tree_data)

    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Spark Physical Plan</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 0; background: #f4f6f8; overflow: hidden; height: 100vh; display: flex; flex-direction: column; }}
        #header {{ padding: 10px 20px; border-bottom: 1px solid #ddd; background: #fff; display: flex; justify-content: space-between; align-items: center; flex-shrink: 0; z-index: 10; }}
        h2 {{ margin: 0; color: #333; font-size: 18px; }}
        .legend {{ font-size: 12px; display: flex; gap: 15px; flex-wrap: wrap; }}
        .legend-item {{ display: flex; align-items: center; gap: 5px; }}
        .dot {{ width: 10px; height: 10px; border-radius: 50%; }}

        #main-container {{ display: flex; flex: 1; overflow: hidden; position: relative; }}
        #tree-container {{ flex: 1; height: 100%; background: white; overflow: hidden; cursor: grab; }}
        #tree-container:active {{ cursor: grabbing; }}

        /* Details Panel (replaces Tooltip) */
        #details-panel {{
            width: 400px;
            background: #fff;
            border-left: 1px solid #ddd;
            padding: 20px;
            overflow-y: auto;
            display: none; /* Hidden by default */
            flex-shrink: 0;
            box-shadow: -2px 0 5px rgba(0,0,0,0.05);
            position: relative;
            z-index: 20;
        }}
        #details-panel.visible {{ display: block; }}

        .panel-header {{ display: flex; justify-content: space-between; align-items: start; margin-bottom: 15px; border-bottom: 1px solid #eee; padding-bottom: 10px; }}
        .panel-header h3 {{ margin: 0; color: #2c3e50; font-size: 16px; }}
        .close-btn {{ cursor: pointer; color: #999; font-size: 20px; line-height: 1; border: none; background: none; }}
        .close-btn:hover {{ color: #333; }}

        .details-section {{ margin-bottom: 15px; }}
        .details-label {{ color: #7f8c8d; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; font-weight: bold; margin-bottom: 5px; }}
        .details-content {{ font-family: "Consolas", "Monaco", monospace; font-size: 12px; color: #34495e; white-space: pre-wrap; background: #f8f9fa; padding: 10px; border-radius: 4px; border: 1px solid #eee; }}

        /* Node Styling */
        .node circle {{ fill: #fff; stroke: steelblue; stroke-width: 2px; cursor: pointer; transition: all 0.2s; }}
        .node circle:hover {{ stroke-width: 3px; }}
        .node.selected circle {{ stroke: #333 !important; stroke-width: 3px; }}

        .node text {{ font: 12px sans-serif; pointer-events: none; text-shadow: 0 1px 0 #fff, 1px 0 0 #fff, 0 -1px 0 #fff, -1px 0 0 #fff; }}

        .link {{ fill: none; stroke: #ccc; stroke-width: 1.5px; opacity: 0.6; }}

        /* Node Type Colors */
        .node.shuffle circle {{ stroke: #e74c3c; fill: #fadbd8; }}
        .node.scan circle {{ stroke: #27ae60; fill: #d5f5e3; }}
        .node.join circle {{ stroke: #8e44ad; fill: #ebdef0; }}
        .node.filter circle {{ stroke: #f39c12; fill: #fdebd0; }}
        .node.aggregate circle {{ stroke: #2980b9; fill: #d6eaf8; }}
        .node.sort circle {{ stroke: #d35400; fill: #edbb99; }}
        .node.project circle {{ stroke: #7f8c8d; fill: #eaeded; }}
        .node.window circle {{ stroke: #16a085; fill: #a2d9ce; }}
        .node.union circle {{ stroke: #2c3e50; fill: #abb2b9; }}

        /* Controls */
        .controls {{ position: absolute; bottom: 20px; left: 20px; display: flex; flex-direction: column; gap: 5px; z-index: 5; }}
        .btn {{ background: white; border: 1px solid #ccc; padding: 5px 10px; border-radius: 4px; cursor: pointer; font-size: 16px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
        .btn:hover {{ background: #f0f0f0; }}

    </style>
</head>
<body>

<div id="header">
    <h2>Spark Physical Plan</h2>
    <div class="legend">
        <div class="legend-item"><div class="dot" style="background: #e74c3c"></div> Exchange</div>
        <div class="legend-item"><div class="dot" style="background: #8e44ad"></div> Join</div>
        <div class="legend-item"><div class="dot" style="background: #27ae60"></div> Scan</div>
        <div class="legend-item"><div class="dot" style="background: #f39c12"></div> Filter</div>
        <div class="legend-item"><div class="dot" style="background: #2980b9"></div> Agg</div>
        <div class="legend-item"><div class="dot" style="background: #d35400"></div> Sort</div>
        <div class="legend-item"><div class="dot" style="background: #16a085"></div> Window</div>
        <div class="legend-item"><div class="dot" style="background: #7f8c8d"></div> Project</div>
    </div>
</div>

<div id="main-container">
    <div id="tree-container"></div>
    <div id="details-panel">
        <div class="panel-header">
            <h3 id="panel-title">Node Details</h3>
            <button class="close-btn" onclick="closePanel()">×</button>
        </div>
        <div id="panel-content"></div>
    </div>

    <div class="controls">
        <button class="btn" onclick="zoomIn()" title="Zoom In">+</button>
        <button class="btn" onclick="zoomOut()" title="Zoom Out">-</button>
        <button class="btn" onclick="resetZoom()" title="Reset View">⟲</button>
    </div>
</div>

<!-- Load D3.js from CDN -->
<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
    const treeData = {json_data};

    // Set dimensions
    const container = document.getElementById('tree-container');
    let width = container.clientWidth;
    let height = container.clientHeight;

    // Vertical Layout: Swap x and y concepts in d3
    // We want the tree to grow Downwards.
    // X axis = width (siblings)
    // Y axis = height (depth)

    const svg = d3.select("#tree-container").append("svg")
        .attr("width", "100%")
        .attr("height", "100%")
        .call(d3.zoom().on("zoom", function (event) {{
            g.attr("transform", event.transform);
        }}))
        .append("g")
        .attr("transform", "translate(" + width / 2 + "," + 50 + ")");

    const g = svg.append("g");
    let i = 0, duration = 500, root;

    // Node Size: [Width, Height] spacing
    const treemap = d3.tree().nodeSize([120, 100]);

    // Assigns parent, children, height, depth
    root = d3.hierarchy(treeData, function(d) {{ return d.children; }});
    root.x0 = 0;
    root.y0 = 0; // Top center

    update(root);

    function update(source) {{
        const treeData = treemap(root);
        const nodes = treeData.descendants();
        const links = treeData.links();

        // Vertical Layout Adjustments
        // d.x is horizontal position
        // d.y is vertical depth (calculated by D3 based on hierarchy)
        nodes.forEach(d => {{ d.y = d.depth * 100; }});

        // ****************** Nodes section ***************************
        const node = g.selectAll('g.node')
            .data(nodes, d => d.id || (d.id = ++i));

        const nodeEnter = node.enter().append('g')
            .attr('class', function(d) {{
                return "node " + (d.data.type || "other");
            }})
            .attr("transform", d => "translate(" + source.x0 + "," + source.y0 + ")")
            .on('click', click);

        nodeEnter.append('circle')
            .attr('class', 'node')
            .attr('r', 1e-6)
            .style("fill", function(d) {{ return d._children ? "lightsteelblue" : "#fff"; }});

        nodeEnter.append('text')
            .attr("dy", "0.35em")
            .attr("y", function(d) {{ return d.children || d._children ? -20 : 20; }})
            .attr("text-anchor", "middle")
            .text(function(d) {{
                // truncate long names
                return d.data.name.length > 20 ? d.data.name.substring(0, 18) + "..." : d.data.name;
            }})
            .style("fill-opacity", 1e-6);

        const nodeUpdate = node.merge(nodeEnter).transition().duration(duration)
            .attr("transform", d => "translate(" + d.x + "," + d.y + ")");

        nodeUpdate.select('circle.node')
            .attr('r', 10)
            .style("fill", d => d._children ? "lightsteelblue" : "");

        nodeUpdate.select('text').style('fill-opacity', 1);

        const nodeExit = node.exit().transition().duration(duration)
            .attr("transform", d => "translate(" + source.x + "," + source.y + ")")
            .remove();

        nodeExit.select('circle').attr('r', 1e-6);
        nodeExit.select('text').style('fill-opacity', 1e-6);

        // ****************** Links section ***************************
        const link = g.selectAll('path.link')
            .data(links, d => d.target.id);

        const linkEnter = link.enter().insert('path', "g")
            .attr("class", "link")
            .attr('d', function(d){{
                const o = {{x: source.x0, y: source.y0}};
                return diagonal(o, o);
            }});

        const linkUpdate = link.merge(linkEnter).transition().duration(duration)
            .attr('d', d => diagonal(d.source, d.target));

        const linkExit = link.exit().transition().duration(duration)
            .attr('d', function(d) {{
                const o = {{x: source.x, y: source.y}};
                return diagonal(o, o);
            }})
            .remove();

        nodes.forEach(d => {{
            d.x0 = d.x;
            d.y0 = d.y;
        }});

        function diagonal(s, d) {{
            // Vertical Bezier
            return `M ${{s.x}} ${{s.y}}
                    C ${{s.x}} ${{(s.y + d.y) / 2}},
                      ${{d.x}} ${{(s.y + d.y) / 2}},
                      ${{d.x}} ${{d.y}}`;
        }}
    }}

    // Interaction Logic
    let selectedNodeId = null;

    function click(event, d) {{
        // Toggle Panel
        const panel = document.getElementById('details-panel');

        // Highlight logic
        d3.selectAll('.node').classed('selected', false);

        if (selectedNodeId === d.id) {{
            // Deselect if clicking same node
            closePanel();
            selectedNodeId = null;
        }} else {{
            // Select new node
            d3.select(this).classed('selected', true);
            selectedNodeId = d.id;
            showDetails(d);
        }}
    }}

    function showDetails(d) {{
        const panel = document.getElementById('details-panel');
        const title = document.getElementById('panel-title');
        const content = document.getElementById('panel-content');

        panel.classList.add('visible');
        title.innerText = d.data.name;

        let desc = d.data.description || "";

        let html = "";

        if (desc) {{
            html += "<div class='details-section'>";
            html += "<div class='details-label'>Arguments & Details</div>";
            html += "<div class='details-content'>" + desc + "</div>";
            html += "</div>";
        }}

        if (d.data.output && d.data.output.length > 0) {{
             html += "<div class='details-section'>";
             html += "<div class='details-label'>Output Columns</div>";
             html += "<div class='details-content'>" + d.data.output.join("\\n") + "</div>";
             html += "</div>";
        }}

        if (d.data.metrics && Object.keys(d.data.metrics).length > 0) {{
             html += "<div class='details-section'>";
             html += "<div class='details-label'>Runtime Metrics</div>";
             html += "<div class='details-content'>" + JSON.stringify(d.data.metrics, null, 2) + "</div>";
             html += "</div>";
        }}

        content.innerHTML = html;
    }}

    window.closePanel = function() {{
        document.getElementById('details-panel').classList.remove('visible');
        d3.selectAll('.node').classed('selected', false);
        selectedNodeId = null;
    }}

    // Zoom Controls
    const zoomBehavior = d3.zoom().on("zoom", (e) => g.attr("transform", e.transform));
    const svgSelect = d3.select("svg");

    function zoomIn() {{ svgSelect.transition().call(zoomBehavior.scaleBy, 1.2); }}
    function zoomOut() {{ svgSelect.transition().call(zoomBehavior.scaleBy, 0.8); }}
    function resetZoom() {{
        svgSelect.transition().call(zoomBehavior.transform, d3.zoomIdentity.translate(width/2, 50));
    }}

    // Resize handler
    window.addEventListener('resize', () => {{
        width = container.clientWidth;
        height = container.clientHeight;
        svgSelect.attr("width", width).attr("height", height);
    }});

</script>
</body>
</html>
    """
    return html_content


def visualize_plan(
    df: Any,
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
