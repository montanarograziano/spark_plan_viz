# Understanding Spark Physical Plans

When you call an action on a Spark DataFrame (e.g., `.show()`, `.collect()`, `.write`), Spark's Catalyst optimizer produces a **physical execution plan** — the concrete strategy for computing your result.

## From SQL to Execution

1. **Unresolved Logical Plan** — raw AST from your DataFrame API calls or SQL
2. **Analyzed Logical Plan** — column names and types resolved against the catalog
3. **Optimized Logical Plan** — Catalyst applies rule-based and cost-based optimizations
4. **Physical Plan** — one or more candidate plans are generated; the best is selected
5. **Execution** — the selected physical plan runs on the cluster

`visualize_plan()` shows step 4 — the **physical plan** — which is the actual set of operations that Spark will execute.

## Why the Physical Plan Matters

The logical plan tells you *what* Spark will compute. The physical plan tells you *how*:

- **Join strategy** — Will Spark broadcast the small table or shuffle both sides?
- **Scan pushdown** — Are filters pushed to the storage layer?
- **Exchange (shuffle)** — Where does data move across the network?
- **Sort placement** — Is Spark sorting data that will be immediately re-shuffled?

These decisions have enormous performance implications. A query that takes 5 minutes with a shuffle join might take 5 seconds with a broadcast join.

## Reading the Visualization

Spark Plan Viz renders the physical plan as an **inverted tree**:

- **Top** — data sources (table scans, file reads)
- **Middle** — transformations (filters, joins, aggregates, sorts)
- **Bottom** — final result

Data flows from top to bottom, and arrows show the direction.

## Next Steps

- [Node Types](node-types.md) — what each color-coded node means
- [Adaptive Query Execution](aqe.md) — how AQE changes the plan at runtime
