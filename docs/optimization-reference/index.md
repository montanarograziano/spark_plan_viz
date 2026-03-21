# Optimization Reference

Spark Plan Viz includes a 14-rule optimization engine that analyzes your execution plan and surfaces actionable suggestions. Rules are grouped by severity.

---

## Errors

Issues that likely indicate a correctness problem or severe performance regression.

### Cross Join (`cross_join`)

**Detects:** `CartesianProduct` nodes or joins with `Cross` join type.

**Why it matters:** A cross join produces the Cartesian product of both sides — if the left side has 1M rows and the right has 1M rows, the result has 1 trillion rows. This is almost always unintentional.

**Example plan pattern:**
```
CartesianProduct
├── Scan table_a (1M rows)
└── Scan table_b (1M rows)
```

**Fix:** Add a join condition. If you genuinely need a cross join, make it explicit with `.crossJoin()` so reviewers know it's intentional.

---

### Nested Loop Join (`nested_loop_join`)

**Detects:** `BroadcastNestedLoopJoin` nodes.

**Why it matters:** This is an O(n*m) join that compares every row on the left with every row on the right. Spark falls back to this when it cannot find an equi-join condition (equality predicate).

**Fix:** Rewrite the join to use equality predicates where possible:

```python
# Bad — triggers nested loop join
df1.join(df2, df1.val > df2.val)

# Better — add an equality condition if possible
df1.join(df2, (df1.key == df2.key) & (df1.val > df2.val))
```

---

## Warnings

Performance issues that should be investigated.

### Full Table Scan (`full_table_scan`)

**Detects:** Scan nodes with no pushed filters.

**Why it matters:** Without pushed filters, Spark reads the entire table/file from storage. For large datasets, this wastes I/O and memory.

**Fix:** Add filter predicates that can be pushed to the storage layer. Partition columns and simple comparisons on columns stored in file metadata are good candidates.

---

### Redundant Shuffle (`redundant_shuffle`)

**Detects:** Consecutive Exchange nodes (shuffle followed by shuffle).

**Why it matters:** Each shuffle involves disk I/O, network transfer, and serialization. Two back-to-back shuffles may indicate that one is unnecessary.

**Fix:** Check if `.repartition()` or `.coalesce()` calls can be consolidated, or if the query can be restructured to avoid the double shuffle.

---

### Expensive Collect (`expensive_collect`)

**Detects:** `collect_list` or `collect_set` in aggregate descriptions.

**Why it matters:** These functions collect all values for a group into a single list on one executor. For large groups, this can cause OutOfMemoryError.

**Fix:**
- Add `.limit()` before the aggregation if you only need a sample
- Use `array_agg` with size limits
- Pre-filter to reduce group sizes
- Consider whether you really need all values or just a count/distinct count

---

### Sort Before Shuffle (`sort_before_shuffle`)

**Detects:** A `Sort` node whose parent is an `Exchange` (shuffle).

**Why it matters:** The shuffle destroys the ordering established by the sort, making the sort entirely wasted work.

**Fix:** Remove the sort, or move it after the shuffle if ordering is needed downstream.

---

### Partition Count (`partition_count_low` / `partition_count_high`)

**Detects:** Shuffle nodes with fewer than 2 or more than 10,000 partitions.

**Why it matters:**
- **Too few** — no parallelism; one executor does all the work
- **Too many** — excessive task scheduling overhead and small files

**Fix:** Adjust `spark.sql.shuffle.partitions` or use `.repartition(n)` / `.coalesce(n)` to control partition count.

---

### Python UDF (`python_udf`)

**Detects:** `PythonUDF`, `BatchEvalPython`, or `ArrowEvalPython` nodes.

**Why it matters:** Python UDFs serialize data from the JVM to Python and back, which is 10-100x slower than native Spark functions.

**Fix:**
- Replace with Spark SQL built-in functions where possible
- Use `pandas_udf` (vectorized UDFs) for better performance
- For complex logic, consider writing a Scala/Java UDF

---

### Window Without PARTITION BY (`window_without_partition`)

**Detects:** Window function nodes without a `PARTITION BY` clause.

**Why it matters:** Without `PARTITION BY`, all data is moved to a single partition, eliminating parallelism. This is effectively a global sort.

**Fix:** Add a `PARTITION BY` clause to distribute the window computation:

```python
# Bad — single partition
Window.orderBy("date")

# Better — partitioned
Window.partitionBy("user_id").orderBy("date")
```

---

## Info

Optimization opportunities that may or may not apply to your use case.

### Consider Broadcast Join (`missing_broadcast_hint`)

**Detects:** `SortMergeJoin` or `ShuffledHashJoin` where broadcast might help.

**Why it matters:** If one side of the join is small enough to fit in executor memory, a broadcast join avoids the expensive shuffle on both sides.

**Fix:**

```python
from pyspark.sql.functions import broadcast

# Hint Spark to broadcast the small table
result = large_df.join(broadcast(small_df), "key")
```

Or increase `spark.sql.autoBroadcastJoinThreshold` (default 10MB).

---

### Non-Columnar Format (`non_columnar_format`)

**Detects:** CSV or JSON format in scan nodes.

**Why it matters:** Row-based formats (CSV, JSON) don't support column pruning or predicate pushdown, so Spark must read and parse the entire file.

**Fix:** Convert to Parquet or ORC:

```python
df.write.parquet("path/to/output")
```

---

### Consider Skew Optimization (`skew_hint`)

**Detects:** `SortMergeJoin` without skew optimization.

**Why it matters:** If join keys are unevenly distributed, a few tasks process most of the data (skew), causing stragglers.

**Fix:** Enable AQE skew join optimization:

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

---

### Potentially Unnecessary Sort (`unnecessary_sort`)

**Detects:** Sort nodes whose output is not consumed by an ordering-dependent operation (SortMergeJoin, Window, TakeOrderedAndProject).

**Why it matters:** Sorts are expensive (O(n log n)). If the sorted order isn't used by a downstream operator, the sort is wasted work.

**Fix:** Remove the `.sort()` or `.orderBy()` call if it's not needed for the final output.

---

### Coalesce via Round-Robin (`coalesce`)

**Detects:** `RoundRobinPartitioning` in shuffle nodes.

**Why it matters:** Round-robin partitioning typically appears from `.repartition(n)` and triggers a full shuffle even when reducing partition count.

**Fix:** Use `.coalesce(n)` instead of `.repartition(n)` when reducing partitions — coalesce avoids a full shuffle by combining partitions locally.
