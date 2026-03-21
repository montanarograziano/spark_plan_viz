# Usage Overview

Spark Plan Viz provides three ways to work with execution plans:

| Mode | Best for | Function |
|------|----------|----------|
| [Notebook Mode](notebook-mode.md) | Interactive exploration in Jupyter | `visualize_plan(df, notebook=True)` |
| [File Mode](file-mode.md) | Sharing HTML with teammates | `visualize_plan(df, notebook=False)` |
| [Programmatic API](programmatic-api.md) | CI pipelines, automated checks | `analyze_plan(df)` |

All modes parse the same underlying JVM execution plan via Py4J and run the same 14-rule optimization analysis.
