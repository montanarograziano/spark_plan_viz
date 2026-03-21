# Programmatic API

Use `analyze_plan()` to get optimization suggestions without rendering a visualization.

## Basic Usage

```python
from spark_plan_viz import analyze_plan

suggestions = analyze_plan(df)
for s in suggestions:
    print(f"[{s.severity.value}] {s.title}")
    print(f"  Node: {s.node_name}")
    print(f"  {s.message}\n")
```

## The `Suggestion` Object

Each suggestion has these fields:

| Field | Type | Description |
|-------|------|-------------|
| `rule_id` | `str` | Unique rule identifier (e.g., `"cross_join"`) |
| `severity` | `Severity` | `ERROR`, `WARNING`, or `INFO` |
| `title` | `str` | Short description of the finding |
| `message` | `str` | Actionable recommendation |
| `node_name` | `str` | Name of the plan node that triggered the rule |

## Severity Levels

```python
from spark_plan_viz import Severity

# Severity.ERROR   — likely correctness or severe performance issue
# Severity.WARNING — potential performance problem
# Severity.INFO    — optimization opportunity
```

## Use Cases

### CI Gate

```python
from spark_plan_viz import analyze_plan, Severity

suggestions = analyze_plan(df)
errors = [s for s in suggestions if s.severity == Severity.ERROR]
if errors:
    raise RuntimeError(f"Plan has {len(errors)} error(s): {errors[0].title}")
```

### Logging

```python
import logging
from spark_plan_viz import analyze_plan

logger = logging.getLogger("query_audit")

for s in analyze_plan(df):
    logger.log(
        logging.ERROR if s.severity.value == "error" else logging.WARNING,
        "%s: %s (node=%s)", s.title, s.message, s.node_name,
    )
```
