# Gallery

Example visualizations showing different query patterns.

## Complex Join with Aggregation

A three-table join with filters and aggregation — the kind of query where `df.explain()` output becomes hard to read:

```python
result = (
    employees.filter(employees.age > 30)
    .join(salaries, employees.id == salaries.emp_id, "inner")
    .join(departments, employees.department == departments.dept_name, "left")
    .filter(salaries.salary > 80000)
    .groupBy("division")
    .agg({"salary": "avg", "age": "max"})
    .sort("division")
)

visualize_plan(result, notebook=True)
```

![Example visualization](example.jpeg)

## Broadcast Join

Using `broadcast()` to avoid a shuffle:

```python
from pyspark.sql.functions import broadcast

result = orders.join(broadcast(customers), orders.customer_id == customers.id)
visualize_plan(result)
```

In the visualization, broadcast joins show a purple node with a satellite dish icon and "BROADCAST (optimized)" in the details panel.

## Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

w = Window.partitionBy("department").orderBy("salary")
result = employees.withColumn("rank", row_number().over(w))
visualize_plan(result)
```

Window nodes appear in teal. If you forget `partitionBy`, the optimization engine will flag it with a warning.
