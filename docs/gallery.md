# Gallery

Complete, runnable examples showing every optimization rule in action.
Each section shows a **bad pattern** (triggers the rule) and a **fix**.

All examples assume this setup:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_plan_viz import visualize_plan, analyze_plan, Severity

spark = SparkSession.builder.appName("examples").getOrCreate()

employees = spark.createDataFrame([
    (1, "Alice", 34, "Engineering", 95000),
    (2, "Bob",   45, "Sales",       85000),
    (3, "Cathy", 29, "Engineering", 78000),
    (4, "David", 38, "Marketing",   72000),
    (5, "Eve",   42, "Sales",       88000),
], ["id", "name", "age", "department", "salary"])

departments = spark.createDataFrame([
    ("Engineering", "Tech",     "US"),
    ("Sales",       "Business", "US"),
    ("Marketing",   "Business", "EU"),
], ["dept_name", "division", "region"])

orders = spark.createDataFrame([
    (1, 1, 100.0), (2, 2, 250.0), (3, 1, 75.0),
    (4, 3, 300.0), (5, 5, 180.0), (6, 4, 90.0),
], ["order_id", "emp_id", "amount"])
```

---

## Complex Join with Aggregation

A three-table join with filters and aggregation — the kind of query where `df.explain()` output becomes hard to read:

```python
result = (
    employees.filter(employees.age > 30)
    .join(orders, employees.id == orders.emp_id, "inner")
    .join(departments, employees.department == departments.dept_name, "left")
    .filter(orders.amount > 80)
    .groupBy("division")
    .agg({"salary": "avg", "age": "max"})
    .sort("division")
)

visualize_plan(result, notebook=True)
```

![Example visualization](example.jpeg)

---

## Error: Cross Join

A cross join produces the Cartesian product — if both sides have 1 000 rows, the result has 1 000 000.

```python
# BAD — triggers cross_join rule (ERROR)
result = employees.crossJoin(departments)
visualize_plan(result)

# FIX — add a join condition
result = employees.join(departments, employees.department == departments.dept_name)
visualize_plan(result)
```

---

## Error: Nested Loop Join

A non-equality condition forces an O(n*m) nested loop join.

```python
# BAD — triggers nested_loop_join rule (ERROR)
result = employees.join(orders, employees.salary > orders.amount)
visualize_plan(result)

# FIX — add an equality predicate alongside the range condition
result = employees.join(
    orders,
    (employees.id == orders.emp_id) & (employees.salary > orders.amount),
)
visualize_plan(result)
```

---

## Warning: No Pushed Filters Detected

Reading a table without pushed filters wastes I/O.

```python
# BAD — triggers full_table_scan rule (WARNING)
result = spark.read.parquet("path/to/employees.parquet").select("id", "name")
visualize_plan(result)

# BETTER — add a filter; on Parquet/ORC it gets pushed to storage
result = spark.read.parquet("path/to/employees.parquet").filter(
    F.col("age") > 30
).select("id", "name")
visualize_plan(result)
```

---

## Warning: Expensive collect_list / collect_set

These aggregate all values into one executor's memory.

```python
# BAD — triggers expensive_collect rule (WARNING)
result = employees.groupBy("department").agg(
    F.collect_list("name").alias("all_names")
)
visualize_plan(result)

# GOOD — standard aggregates are safe
result = employees.groupBy("department").agg(
    F.avg("salary").alias("avg_salary"),
    F.count("*").alias("headcount"),
)
visualize_plan(result)
```

---

## Warning: Window Without PARTITION BY

A global window moves all data to one partition.

```python
# BAD — triggers window_without_partition rule (WARNING)
w = Window.orderBy("salary")
result = employees.withColumn("global_rank", F.row_number().over(w))
visualize_plan(result)

# FIX — add PARTITION BY to distribute the work
w = Window.partitionBy("department").orderBy("salary")
result = employees.withColumn("dept_rank", F.row_number().over(w))
visualize_plan(result)
```

---

## Warning: Python UDF

Python UDFs serialize data between JVM and Python on every row.

```python
# BAD — triggers python_udf rule (WARNING)
@F.udf("string")
def upper_name(s):
    return s.upper() if s else None

result = employees.select(upper_name("name").alias("upper_name"))
visualize_plan(result)

# FIX — use Spark's built-in upper()
result = employees.select(F.upper("name").alias("upper_name"))
visualize_plan(result)
```

---

## Warning: Redundant Shuffle

Back-to-back repartitions waste a full network shuffle.

```python
# BAD — triggers redundant_shuffle rule (WARNING)
result = employees.repartition(10, "department").repartition(5)
visualize_plan(result)

# FIX — single repartition
result = employees.repartition(10, "department")
visualize_plan(result)
```

---

## Info: Missing Broadcast Hint

Shuffle joins are expensive when one side is small.

```python
# BEFORE — supported shuffle join (triggers missing_broadcast_hint rule, INFO)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
result = employees.join(departments, employees.department == departments.dept_name)
visualize_plan(result)

# FIX — explicit broadcast avoids the shuffle
result = employees.join(
    F.broadcast(departments), employees.department == departments.dept_name
)
visualize_plan(result)
```

---

## Warning: Row-Based Scan Without Pushdown (CSV / JSON)

Row-based formats without pushed filters often lead to expensive scans.

```python
# BAD — triggers non_columnar_no_pushdown rule (WARNING)
csv_df = spark.read.csv("path/to/data.csv", header=True)
visualize_plan(csv_df)

# FIX — convert to Parquet
csv_df.write.parquet("path/to/data.parquet")
pq_df = spark.read.parquet("path/to/data.parquet")
visualize_plan(pq_df)
```

---

## Info: Round-Robin Repartition

`repartition(n)` triggers a full shuffle even when reducing partitions.

```python
# BAD — triggers coalesce rule (INFO)
result = employees.repartition(2)
visualize_plan(result)

# FIX — coalesce avoids the full shuffle
result = employees.coalesce(2)
visualize_plan(result)
```

---

## Warning: Single-Partition Exchange

Global exchanges can serialize a stage onto one task.

```python
# Triggers single_partition_exchange rule (WARNING)
from pyspark.sql.window import Window

window = Window.orderBy("id")
result = employees.withColumn("rn", F.row_number().over(window))
visualize_plan(result)
```

---

## Programmatic Analysis

Use `analyze_plan()` to get suggestions without rendering:

```python
from spark_plan_viz import analyze_plan, Severity

# Build a deliberately suboptimal query
result = (
    employees.crossJoin(departments)
    .groupBy("division")
    .agg(F.collect_list("name").alias("all_names"))
)

suggestions = analyze_plan(result)
for s in suggestions:
    print(f"[{s.severity.value:7s}] {s.title}")
    print(f"         {s.message}\n")

# Filter by severity
errors = [s for s in suggestions if s.severity == Severity.ERROR]
print(f"{len(errors)} error(s), {len(suggestions)} total finding(s)")
```
