"""HTML rendering and main entry point for spark_plan_viz."""

from __future__ import annotations

import base64
import json
import logging
import os
import webbrowser
from importlib.resources import files
from typing import Any

from pyspark.sql import DataFrame

from spark_plan_viz._constants import IFRAME_HEIGHT
from spark_plan_viz._parser import _parse_spark_plan

logger = logging.getLogger("spark_plan_viz")


def _build_html_string(tree_data: dict[str, Any]) -> str:
    """Inject the tree data into a D3.js HTML template and return the HTML string."""
    template_path = files("spark_plan_viz").joinpath("template.html")
    template_content = template_path.read_text(encoding="utf-8")

    # Escape </script> in JSON data to prevent premature script tag closure
    json_data = json.dumps(tree_data).replace("</", "<\\/")
    html_content = template_content.replace("{{ TREE_DATA }}", json_data)

    return html_content


def visualize_plan(
    df: DataFrame,
    notebook: bool = True,
    output_file: str = "spark_plan_viz.html",
    analyze: bool = True,
) -> dict[str, Any] | None:
    """
    Visualize a PySpark DataFrame's physical execution plan.

    Args:
        df: The PySpark DataFrame.
        notebook: If True (default), renders inline in notebook.
            If False, saves to file and opens in browser.
        output_file: Name of the output HTML file (used only when notebook=False).
        analyze: If True (default), runs the optimization analyzer and
            attaches suggestions to plan nodes.

    Returns:
        The parsed plan tree dict, or None if parsing failed.
    """
    logger.info("Parsing Spark Plan...")
    tree_data = _parse_spark_plan(df)

    if tree_data is None:
        logger.warning("Failed to generate plan.")
        return None

    # Run optimization analysis if requested
    if analyze:
        try:
            from spark_plan_viz._analyzer import _attach_suggestions

            _attach_suggestions(tree_data)
        except Exception:
            logger.debug("Optimization analysis failed", exc_info=True)

    html_content = _build_html_string(tree_data)

    if notebook:
        logger.info("Rendering inline (notebook mode)...")
        try:
            from IPython.display import IFrame, display

            b64_html = base64.b64encode(html_content.encode("utf-8")).decode("utf-8")
            data_uri = f"data:text/html;base64,{b64_html}"
            display(IFrame(src=data_uri, width="100%", height=IFRAME_HEIGHT))
        except ImportError:
            logger.error("IPython is not installed. Cannot display inline.")
    else:
        logger.info("Generating Visualization at %s...", output_file)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)

        path = os.path.abspath(output_file)
        logger.info("Done! File saved to: %s", path)
        webbrowser.open("file://" + path)

    return tree_data
