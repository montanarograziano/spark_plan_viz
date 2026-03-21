# Node Types

Each node in the visualization is color-coded by its operation type.

## Exchange / Shuffle (Red)

**Examples:** `Exchange`, `ShuffleExchange`, `BroadcastExchange`

Shuffle nodes redistribute data across executors. This involves serializing data, writing to disk, transferring over the network, and deserializing — often the most expensive operation in a plan.

**Key info shown:** Shuffle type (hash/range), partition count.

## Join (Purple)

**Examples:** `SortMergeJoin`, `BroadcastHashJoin`, `ShuffledHashJoin`, `BroadcastNestedLoopJoin`

Join nodes combine two datasets. The join strategy is critical for performance:

- **BroadcastHashJoin** — fastest for small-to-large joins; broadcasts the small side to all executors
- **SortMergeJoin** — scalable but requires shuffling and sorting both sides
- **BroadcastNestedLoopJoin** — O(n*m), used when there's no equi-join condition

**Key info shown:** Join type (Inner, LeftOuter, etc.), broadcast indicator, build side.

## Scan (Green)

**Examples:** `FileScan`, `BatchScan`, `InMemoryTableScan`

Scan nodes read data from storage. Performance depends on:

- **Format** — columnar formats (Parquet, ORC) support column pruning and predicate pushdown
- **Pushed filters** — filters applied at the storage layer, reducing data read from disk

**Key info shown:** Table name, format, pushed filters indicator.

## Filter (Orange)

**Examples:** `Filter`

Filter nodes apply predicates to rows. Filters that can't be pushed to the scan layer appear as separate nodes.

**Key info shown:** Filter condition.

## Aggregate (Blue)

**Examples:** `HashAggregate`, `SortAggregate`, `ObjectHashAggregate`

Aggregate nodes compute grouped or global aggregations. Spark typically uses a two-phase approach: partial aggregation before the shuffle, then final aggregation after.

**Key info shown:** Aggregate functions (sum, count, avg, etc.), grouping keys.

## Sort (Brown)

**Examples:** `Sort`

Sort nodes order data by one or more columns. Sorts before a shuffle are usually wasted because the shuffle destroys ordering.

**Key info shown:** Sort order (column ASC/DESC).

## Project (Gray)

**Examples:** `Project`

Project nodes select and transform columns. They correspond to `.select()` or column expressions in your query.

**Key info shown:** Selected columns.

## Window (Teal)

**Examples:** `Window`

Window nodes compute window functions (e.g., `row_number()`, `rank()`, running aggregates). A window without `PARTITION BY` forces all data to a single partition.

## Union (Dark)

**Examples:** `Union`

Union nodes combine multiple datasets with the same schema.
