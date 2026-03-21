# File Mode

Export the execution plan as a standalone HTML file.

## Basic Usage

```python
from spark_plan_viz import visualize_plan

visualize_plan(df, notebook=False, output_file="my_plan.html")
```

This writes a self-contained HTML file and opens it in your default browser. The file includes D3.js loaded from CDN and all plan data embedded as JSON.

## Options

```python
visualize_plan(
    df,
    notebook=False,
    output_file="spark_plan_viz.html",  # Output path (default)
    analyze=True,                        # Include optimization suggestions
)
```

## Sharing

The generated HTML file is fully self-contained (aside from the D3.js CDN link) and can be:

- Attached to Jira/GitHub issues
- Shared via Slack or email
- Committed to a repository for review
- Hosted on any static file server
