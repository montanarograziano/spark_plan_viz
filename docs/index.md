# Spark Plan Viz

**Interactive visualization and optimization analysis for Apache Spark execution plans.**

Spark Plan Viz helps data engineers and developers debug complex PySpark queries by turning the opaque textual output of `df.explain()` into an interactive, color-coded tree — with actionable optimization suggestions.

---

## Key Features

- **Interactive D3.js visualization** — zoom, pan, click nodes to explore details
- **15-rule optimization engine** — detects cross joins, single-partition exchanges, row-based scans without pushdown, Python UDFs, and more
- **Jupyter-native** — renders inline without external files
- **Standalone HTML export** — share visualizations with your team
- **AQE support** — full support for Adaptive Query Execution
- **Color-coded nodes** — instantly identify shuffles, joins, scans, filters, aggregates, sorts, windows, and projects

## Quick Example

```python
from spark_plan_viz import visualize_plan

# Visualize any PySpark DataFrame's execution plan
visualize_plan(df, notebook=True)
```

```python
from spark_plan_viz import analyze_plan

# Get optimization suggestions programmatically
suggestions = analyze_plan(df)
for s in suggestions:
    print(f"[{s.severity.value}] {s.title}: {s.message}")
```

## Next Steps

- [Getting Started](getting-started.md) — install and visualize your first plan in 2 minutes
- [Optimization Reference](optimization-reference/index.md) — learn about all 15 optimization rules
- [API Reference](api-reference.md) — full public API documentation
- [PyPI](https://pypi.org/project/spark-plan-viz/) — package on the Python Package Index
