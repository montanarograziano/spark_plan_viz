# Spark Plan Viz

A lightweight, interactive tool to visualize and analyze PySpark execution plans using D3.js. It helps developers and data engineers debug complex queries when the textual result of `df.explain()` is not enough.

**[Documentation](https://montanarograziano.github.io/spark_plan_viz/)** | **[PyPI](https://pypi.org/project/spark-plan-viz/)** | **[Optimization Reference](https://montanarograziano.github.io/spark_plan_viz/optimization-reference/)**

## Features

- **Interactive Visualization**: Zoom, pan, and click nodes to explore execution details
- **14-Rule Optimization Engine**: Detects cross joins, missing broadcasts, full table scans, Python UDFs, and more
- **Performance Insights**: Instantly identify shuffles, broadcast joins, and pushed filters
- **Jupyter Integration**: Renders directly inside notebooks without external files
- **Standalone HTML**: Export visualizations to share with your team
- **AQE Support**: Full support for Adaptive Query Execution details

## Installation

```sh
uv add spark-plan-viz
```

Or with pip:

```sh
pip install spark-plan-viz
```

## Quick Start

### Visualize

```python
from spark_plan_viz import visualize_plan

# Renders inline in Jupyter
visualize_plan(df, notebook=True)

# Or export to HTML file
visualize_plan(df, notebook=False, output_file="my_plan.html")
```

### Analyze

```python
from spark_plan_viz import analyze_plan

suggestions = analyze_plan(df)
for s in suggestions:
    print(f"[{s.severity.value}] {s.title}: {s.message}")
```

## Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from spark_plan_viz import visualize_plan

spark = SparkSession.builder.appName("Example").getOrCreate()

orders = spark.createDataFrame([
    (1, "Alice", 100),
    (2, "Bob", 200),
], ["id", "customer", "amount"])

customers = spark.createDataFrame([
    ("Alice", "NY"),
    ("Bob", "CA"),
], ["name", "state"])

result = orders.filter(orders.amount > 50) \
    .join(broadcast(customers), orders.customer == customers.name) \
    .groupBy("state") \
    .agg({"amount": "sum"})

visualize_plan(result, notebook=True)
```

![example visualization](<docs/example.jpeg>)

## Requirements

- Python 3.11+
- PySpark 3.x+
- For notebook mode: IPython/Jupyter

## Limitations

`spark_plan_viz` is not available on Databricks Serverless Compute, as it's not possible to access the `_jdf` object of a Spark DataFrame.

## License

MIT License

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

```bash
git clone https://github.com/montanarograziano/spark_plan_viz.git
cd spark_plan_viz
just install
just pre-commit
just check
just test
```

## Acknowledgments

Built with [D3.js](https://d3js.org/) for interactive visualizations.
