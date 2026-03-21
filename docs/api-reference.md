# API Reference

## `visualize_plan`

```python
from spark_plan_viz import visualize_plan

def visualize_plan(
    df: DataFrame,
    notebook: bool = True,
    output_file: str = "spark_plan_viz.html",
    analyze: bool = True,
) -> dict | None:
```

Visualize a PySpark DataFrame's physical execution plan.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | *(required)* | The PySpark DataFrame to visualize |
| `notebook` | `bool` | `True` | Render inline in Jupyter notebook |
| `output_file` | `str` | `"spark_plan_viz.html"` | Output path (file mode only) |
| `analyze` | `bool` | `True` | Run optimization analysis |

**Returns:** The parsed plan tree as a `dict`, or `None` if parsing failed.

---

## `analyze_plan`

```python
from spark_plan_viz import analyze_plan

def analyze_plan(df: DataFrame) -> list[Suggestion]:
```

Parse the execution plan and return optimization suggestions without rendering.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `df` | `DataFrame` | The PySpark DataFrame to analyze |

**Returns:** A sorted, deduplicated list of `Suggestion` objects.

**Raises:** `ValueError` if the plan cannot be parsed.

---

## `Suggestion`

```python
from spark_plan_viz import Suggestion
```

A dataclass representing an optimization finding.

| Field | Type | Description |
|-------|------|-------------|
| `rule_id` | `str` | Unique rule identifier |
| `severity` | `Severity` | `ERROR`, `WARNING`, or `INFO` |
| `title` | `str` | Short finding description |
| `message` | `str` | Actionable recommendation |
| `node_name` | `str` | Plan node that triggered the rule |

**Methods:**

- `to_dict() -> dict[str, str]` — serialize to a plain dict (JSON-compatible)

---

## `Severity`

```python
from spark_plan_viz import Severity
```

An enum with three levels:

| Value | Meaning |
|-------|---------|
| `Severity.ERROR` | Likely correctness or severe performance issue |
| `Severity.WARNING` | Potential performance problem |
| `Severity.INFO` | Optimization opportunity |

---

## Type Definitions

### `PlanNode`

```python
from spark_plan_viz import PlanNode
```

A `TypedDict` representing a node in the parsed plan tree:

```python
class PlanNode(TypedDict):
    name: str
    description: str
    output: list[str]
    type: str                    # "shuffle", "scan", "join", "filter", etc.
    key_info: KeyInfo
    children: list[PlanNode]
    metrics: dict[str, Any]
    suggestions: list[dict[str, str]]
```

### `KeyInfo`

```python
from spark_plan_viz import KeyInfo
```

A `TypedDict` (all fields optional) with extracted node metadata:

```python
class KeyInfo(TypedDict, total=False):
    join_type: str
    condition: str
    is_broadcast: bool
    build_side: str
    columns: list[str]
    functions: list[str]
    group_by: list[str]
    table: str
    format: str
    pushed_filters: list[str]
    order: str
    shuffle_type: str
    partitions: str
    is_shuffle: bool
```
