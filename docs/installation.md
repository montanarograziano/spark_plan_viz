# Installation

## Requirements

- Python 3.11+
- PySpark 3.x+
- For notebook mode: IPython / Jupyter

## Using uv (recommended)

```bash
uv add spark-plan-viz
```

## Using pip

```bash
pip install spark-plan-viz
```

## Development Installation

```bash
git clone https://github.com/montanarograziano/spark_plan_viz.git
cd spark_plan_viz
just install    # installs all dependencies including dev tools
```

## Limitations

- **Databricks Serverless Compute** is not supported, as it does not expose the `_jdf` object needed to traverse the JVM execution plan.
- A working Java installation is required (PySpark uses Py4J to communicate with the JVM).

## Verifying Installation

```python
import spark_plan_viz
print(spark_plan_viz.__version__)
```
