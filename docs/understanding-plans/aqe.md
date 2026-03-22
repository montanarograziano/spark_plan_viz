# Adaptive Query Execution (AQE)

Adaptive Query Execution, enabled by default since Spark 3.2, allows Spark to re-optimize the physical plan at runtime based on actual data statistics collected during execution.

## What AQE Does

- **Coalesces post-shuffle partitions** — reduces the number of partitions when data is smaller than expected
- **Converts sort-merge joins to broadcast joins** — if one side of a join turns out to be small at runtime
- **Optimizes skew joins** — splits skewed partitions to prevent straggler tasks

## How It Appears in the Visualization

When AQE is active, the plan tree starts with an `AdaptiveSparkPlan` root node. Inside, you'll see:

- **QueryStage nodes** — `ShuffleQueryStageExec`, `BroadcastQueryStageExec` — these represent materialized stages that AQE uses to collect statistics
- **ReusedExchange** — an exchange whose output is reused by multiple consumers

Spark Plan Viz drills into query stages and reused exchanges to show the full operator tree, so you see the actual operations, not just the stage wrappers.

## AQE-Related Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `spark.sql.adaptive.enabled` | `true` (3.2+) | Enable AQE |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Coalesce small partitions |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Optimize skewed joins |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | `30MB` | Runtime broadcast threshold |

## Tips

- Even with AQE, it's useful to review the plan — AQE cannot fix fundamentally inefficient queries (e.g., unnecessary cross joins, Python UDFs)
- Runtime statistics from AQE can explain why Spark changed join strategies, coalesced partitions, or optimized skewed data
