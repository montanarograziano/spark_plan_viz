# Getting Started

Get from zero to your first interactive plan visualization in under 2 minutes.

## 1. Install

```bash
uv add spark-plan-viz
```

Or with pip:

```bash
pip install spark-plan-viz
```

## 2. Visualize

```python
from pyspark.sql import SparkSession
from spark_plan_viz import visualize_plan

spark = SparkSession.builder.appName("demo").getOrCreate()

orders = spark.createDataFrame([
    (1, "Alice", 100.0),
    (2, "Bob", 200.0),
], ["id", "customer", "amount"])

customers = spark.createDataFrame([
    ("Alice", "NY"),
    ("Bob", "CA"),
], ["name", "state"])

result = (
    orders.filter(orders.amount > 50)
    .join(customers, orders.customer == customers.name)
    .groupBy("state")
    .agg({"amount": "sum"})
)

# Renders inline in Jupyter
visualize_plan(result, notebook=True)
```

## 3. Analyze

```python
from spark_plan_viz import analyze_plan

suggestions = analyze_plan(result)
for s in suggestions:
    print(f"[{s.severity.value}] {s.title}")
    print(f"  {s.message}\n")
```

## What You'll See

- **Color-coded tree** — data sources at the top, final result at the bottom
- **Clickable nodes** — click any node to see full details in the side panel
- **Optimization badges** — colored dots on nodes with suggestions
- **Suggestions panel** — click the "Suggestions" button to see all findings

## Next Steps

- [Notebook Mode](usage/notebook-mode.md) — details on Jupyter integration
- [File Mode](usage/file-mode.md) — export to standalone HTML
- [Optimization Reference](optimization-reference/index.md) — what each rule detects
