"""Named constants for spark_plan_viz — replaces all magic numbers."""

MAX_DISPLAY_COLUMNS = 5
MAX_DISPLAY_FUNCTIONS = 3
MAX_DISPLAY_FILTERS = 3
MAX_DISPLAY_GROUPING_KEYS = 3
NODE_NAME_TRUNCATION = 25
TEXT_TRUNCATION = 30
IFRAME_HEIGHT = 800

# Partition counts outside this range trigger partition_count warnings.
PARTITION_COUNT_MIN = 2
PARTITION_COUNT_MAX = 10_000

# Formats that support predicate/column pushdown well.
PUSHDOWN_FORMATS = frozenset({"PARQUET", "ORC", "DELTA", "AVRO", "ICEBERG"})
ROW_BASED_FORMATS = frozenset({"CSV", "JSON", "TEXT"})

# Join types that can use broadcast hash join strategy.
BROADCASTABLE_JOIN_TYPES = frozenset(
    {
        "Inner",
        "LeftOuter",
        "RightOuter",
        "LeftSemi",
        "LeftAnti",
    }
)

# Node types that do not change partitioning / ordering semantics for tree walks.
PASS_THROUGH_TYPES = frozenset({"project", "filter", "other"})

# Generator expressions that multiply rows.
ROW_EXPLODING_GENERATORS = frozenset(
    {
        "explode",
        "explode_outer",
        "posexplode",
        "posexplode_outer",
        "inline",
        "inline_outer",
        "stack",
    }
)
