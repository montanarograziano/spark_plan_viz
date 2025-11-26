# Spark Plan Viz ⚡

A lightweight, interactive tool to visualize PySpark execution plans using D3.js. It helps developers and data engineers debug complex queries when the textual result of `df.explain()` is not enough. Perfect for identifying bottlenecks, understanding query execution flow, and optimizing Spark performance.

## ✨ Features

- **📊 Interactive Visualization**: Zoom, pan, and click nodes to explore execution details
- **🎯 Performance Insights**: Instantly identify shuffles, broadcast joins, and pushed filters
- **📝 Rich Node Information**: See join types, filter conditions, aggregate functions, sort orders, and more
- **🔍 Detailed Side Panel**: Click any node to view complete information including output columns and full descriptions
- **📓 Jupyter Integration**: Renders directly inside notebooks without external files
- **🌐 Standalone HTML**: Export visualizations to share with your team
- **🎨 Smart Coloring**: Color-coded nodes by operation type for quick identification
- **⚡ AQE Support**: Full support for Adaptive Query Execution details
- **🔄 Execution Flow**: Tree layout shows data flow from sources (top) to result (bottom)

## 🚀 Installation

```sh
pip install spark-plan-viz
```

## 📖 Usage

### In a Jupyter Notebook

```python
from spark_plan_viz import visualize_plan

# Assuming 'df' is your PySpark DataFrame
visualize_plan(df, notebook=True)
```

### Export to HTML

```python
from spark_plan_viz import visualize_plan

# Generates a standalone HTML file and opens it in browser
visualize_plan(df, notebook=False, output_file="my_query_plan.html")
```

## 🎨 Reading the Visualization

### Node Colors

The visualization uses color coding to help you quickly identify different operation types:

- **🔴 Red (Exchange/Shuffle)**: Data shuffling across executors - potential performance bottleneck
- **🟣 Purple (Join)**: Join operations - watch for broadcast vs shuffle joins
- **🟢 Green (Scan)**: Data source operations - check for pushed filters
- **🟠 Orange (Filter)**: Filter operations
- **🔵 Blue (Aggregate)**: Aggregation operations - see grouping keys and functions
- **🟤 Brown (Sort)**: Sort operations - can be expensive
- **⚫ Gray (Project)**: Column selection/projection
- **🟦 Teal (Window)**: Window functions

### Tree Layout

- **Top**: Data sources (table/file scans)
- **Middle**: Transformations (filters, joins, aggregations)
- **Bottom**: Final result
- **Arrows**: Show data flow direction from sources to result

### Key Information Displayed on Nodes

Each node shows the most relevant information directly:

- **Joins**: Type (Inner, Left, etc.), 📡 Broadcast indicator
- **Filters**: Condition preview
- **Scans**: Table name, data format (PARQUET, JSON, etc.), ✓ Pushed filters indicator
- **Aggregates**: Functions used (sum, count, avg), grouping keys
- **Shuffles**: ⚠️ Warning indicator, partition count, shuffle type
- **Sorts**: Sort order (ASC/DESC)
- **Projects**: Selected columns

### Performance Optimization Tips

When reviewing your execution plan, look for:

1. **⚠️ Shuffle Operations (Red)**:
   - Minimize these as they involve network I/O
   - Consider repartitioning or using broadcast joins

2. **📡 Broadcast Joins (Purple with 📡)**:
   - Excellent for small tables
   - Much faster than shuffle joins
   - Verify the smaller table is being broadcast

3. **✓ Pushed Filters (Green with ✓)**:
   - Filters applied at the data source level
   - Reduces data read from disk
   - Ensure filters are pushed when possible

4. **Data Format**:
   - Columnar formats (PARQUET, ORC) are generally faster
   - Check if your tables use optimal formats

## 💡 Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from spark_plan_viz import visualize_plan

spark = SparkSession.builder.appName("Example").getOrCreate()

# Create sample data
orders = spark.createDataFrame([
    (1, "Alice", 100),
    (2, "Bob", 200),
], ["id", "customer", "amount"])

customers = spark.createDataFrame([
    ("Alice", "NY"),
    ("Bob", "CA"),
], ["name", "state"])

# Complex query with broadcast join
result = orders.filter(orders.amount > 50) \
    .join(broadcast(customers), orders.customer == customers.name) \
    .groupBy("state") \
    .agg({"amount": "sum"})

# Visualize the execution plan
visualize_plan(result, notebook=True)
```

## 📸 Example Visualization

Here's an example screenshot of the visualization:

![example visualization](<docs/example.jpeg>)

## 📋 Requirements

- Python 3.11+
- PySpark 3.x+
- For notebook mode: IPython/Jupyter

## ⚠️ Limitations

`spark_plan_viz` is not available on Databricks Serverless Compute, as it's not possible to access the `_jdf` object of a Spark Dataframe.

*PS*: I haven't test it on Personal Compute Clusters as I don't have a Databricks subscription active. If you want to analyze query plan on
Databricks you can use the builtin **See query profile**.

## 📄 License

MIT License

## 🤝 Contributing

Contributions are welcome! Feel free to open issues or submit pull requests.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/montanarograziano/spark_plan_viz.git
cd spark_plan_viz

# Install dependencies
just install

# Install pre commits through prek
just pre-commit

# Test on specific Python version
just test-python 3.12

# Test on all supported Python versions (3.11-3.13)
just test-all
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

## 🙏 Acknowledgments

Built with [D3.js](https://d3js.org/) for interactive visualizations.
