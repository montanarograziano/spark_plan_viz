# Spark Plan Viz ⚡

A lightweight, interactive tool to visualize PySpark execution plans using D3.js. It helps developers and data engineers debug complex queries when the textual result of `df.explain()` is not enough. It helps identifying bottlenecks (Sorts, Shuffles), and displaying runtime metrics (AQE).

## Features

- Interactive Tree: Zoom, pan, and collapse nodes.
- Metric Insights: Click nodes to see runtime metrics (rows output, spill size, etc.).
- Jupyter Integration: Renders directly inside notebooks without external files.
- Zero Dependencies: Only requires Pyspark.
- AQE Support: Visualizes Adaptive Query Execution details.

## Installation

```sh
pip install spark-plan-viz
```


## Usage

### In a Jupyter Notebook

```py
from spark_plan_viz import visualize_plan

# Assuming 'df' is your PySpark DataFrame
visualize_plan(df, notebook=True)
```

### Export to HTML

```py
from spark_plan_viz import visualize_plan

# Generates a standalone HTML file
visualize_plan(df, output_file="my_query_plan.html", open_browser=True)
```

## How to read the chart

- **Red nodes**: Exchange/Shuffle (Network heavy)
- **Purple Nodes**: Joins
- **Green Nodes**: Scans (Data Ingestion)
- **Blue Nodes**: Aggregations
