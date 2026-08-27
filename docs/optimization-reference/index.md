# Optimization Reference

Spark Plan Viz includes a 19-rule optimization engine that analyzes your execution plan and surfaces actionable suggestions. Rules are grouped by severity.

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

### No Pushed Filters Detected (`full_table_scan`)

**Detects:** Pushdown-capable scans (for example Parquet/ORC/Delta/Avro/Iceberg) with no pushed filters.

**Why it matters:** If a scan format supports predicate pushdown but Spark is not pushing any filters, the engine may read more data than necessary.

**Fix:** Add filter predicates that can be pushed to the storage layer. Partition columns and simple comparisons are good candidates.

---

### No Partition Pruning (`empty_partition_filters`)

**Detects:** File scans over a partitioned table where `PartitionFilters: []`.

**Why it matters:** Without partition filters Spark must list and open every partition directory. On large date-partitioned lakes this dominates job time before any data is read.

**Fix:** Filter on partition columns early:

```python
# Bad — reads every department partition
spark.read.parquet("/data/employees")

# Better — prunes to one partition
spark.read.parquet("/data/employees").filter(F.col("department") == "Sales")
```

---

### Row-Multiplying Expand (`expand`)

**Detects:** `Expand` nodes from `CUBE` / `ROLLUP` / `GROUPING SETS` or multiple `COUNT(DISTINCT ...)`.

**Why it matters:** Expand multiplies each input row by the number of grouping sets (for a 2-column cube that is 4x). The inflated rows then shuffle and aggregate, which can dominate memory and network cost.

**Fix:**
- Prefer fewer grouping sets when possible
- Rewrite multiple `COUNT(DISTINCT x)` into conditional aggregates or separate queries when expand factors are large
- Pre-aggregate before the cube/rollup when the grain allows it

---

### Row Explosion (`generate_explode`)

**Detects:** `Generate` nodes with `explode` / `posexplode` / `inline` / `stack`.

**Why it matters:** Exploding arrays multiplies rows by collection size. Wide rows plus large arrays often cause partition OOMs and massive shuffles downstream.

**Fix:**
- Filter and project before exploding
- Drop the array column immediately after explode
- Repartition before explode when arrays are large

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

### Single-Partition Exchange (`single_partition_exchange`)

**Detects:** Exchange nodes with `SinglePartition`.

**Why it matters:** A single-partition exchange funnels all work through one task, which can serialize the stage and become a bottleneck.

**Fix:** Avoid global operations when possible, or repartition by a meaningful key so the work can stay distributed.

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

### Possible Broadcast Join Opportunity (`missing_broadcast_hint`)

**Detects:** Supported `SortMergeJoin` or `ShuffledHashJoin` plans where the join is still shuffle-based and the join type is compatible with broadcast.

**Why it matters:** If one side of the join is small enough to fit in executor memory, a broadcast join can avoid shuffling both sides. This is a heuristic hint, not a proof that broadcast is appropriate.

**Fix:**

```python
from pyspark.sql.functions import broadcast

# Hint Spark to broadcast the small table
result = large_df.join(broadcast(small_df), "key")
```

Or increase `spark.sql.autoBroadcastJoinThreshold` if broadcast is appropriate for your workload.

---

### AQE Skew Join Active (`skew_join`)

**Detects:** Joins marked with AQE skew handling (`isSkew=true` on `SortMergeJoin`).

**Why it matters:** This is informational — Adaptive Query Execution already split skewed partitions. If the job is still slow on that join, residual skew may remain.

**Fix:** If skew persists after AQE, salt hot keys, pre-aggregate the heavy side, or raise AQE skew thresholds carefully.

---

### Row-Based Format (`non_columnar_format`)

**Detects:** CSV or JSON scans where pushed filters are already present.

**Why it matters:** Row-based formats often cost more to scan than columnar formats in analytic workloads, even when Spark can still apply some filtering.

**Fix:** Convert to Parquet or ORC:

```python
df.write.parquet("path/to/output")
```

---

### Row-Based Scan Without Pushdown (`non_columnar_no_pushdown`)

**Detects:** CSV or JSON scans with no pushed filters.

**Why it matters:** This combines two concrete signals: the scan is row-based and Spark is not pushing any filters. That often means higher-than-necessary scan cost.

**Fix:** Add selective filters early when possible, or convert the dataset to Parquet/ORC:

```python
df.write.parquet("path/to/output")
```

---

### Potentially Unnecessary Sort (`unnecessary_sort`)

**Detects:** Sort nodes whose output is not consumed by an ordering-dependent operation (SortMergeJoin, Window, TakeOrderedAndProject).

**Why it matters:** Sorts are expensive (O(n log n)). If the sorted order isn't used by a downstream operator, the sort is wasted work.

**Example:** `orderBy` before `dropDuplicates` often leaves a Sort that does not affect the dedupe keys.

**Fix:** Remove the `.sort()` or `.orderBy()` call if it's not needed for the final output:

```python
# Bad — Sort may survive under Aggregate/Deduplicate
df.orderBy("salary").dropDuplicates(["department"])

# Better
df.dropDuplicates(["department"])
```

---

### Round-Robin Repartition (`coalesce`)

**Detects:** `RoundRobinPartitioning` in shuffle nodes.

**Why it matters:** Round-robin partitioning usually indicates a repartition-style full shuffle.

**Fix:** If the change is only reducing partition count, use `.coalesce(n)` instead of `.repartition(n)` to avoid the full shuffle.
