# Notebook Mode

Render the execution plan inline in a Jupyter notebook.

## Basic Usage

```python
from spark_plan_viz import visualize_plan

visualize_plan(df, notebook=True)
```

The visualization is rendered as an IFrame with base64-encoded HTML, so it works in standard Jupyter, JupyterLab, and VS Code notebooks without writing any files to disk.

## Options

```python
visualize_plan(
    df,
    notebook=True,       # Render inline (default)
    analyze=True,        # Run optimization analysis (default)
)
```

Set `analyze=False` to skip the optimization engine and only show the plan tree.

## Return Value

`visualize_plan()` returns the parsed plan tree as a dictionary (or `None` if parsing failed). You can use this for further programmatic inspection:

```python
tree = visualize_plan(df, notebook=True)
if tree:
    print(f"Root node: {tree['name']}")
    print(f"Children: {len(tree['children'])}")
```

## Interaction

- **Click** a node to open the details panel
- **Scroll** to zoom in/out
- **Drag** to pan
- Use the **+/-** buttons for precise zoom control
- Click **Suggestions** to open the optimization panel
