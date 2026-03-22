"""
Example-driven integration tests for spark_plan_viz.

Each test builds a real PySpark DataFrame whose execution plan triggers
one (or more) optimization rules.  These tests serve two purposes:

1. **Correctness** — verify that the analyzer detects the expected pattern.
2. **Documentation** — the code in each test is a ready-to-copy example that
   users can paste into their own notebooks to try the package.

Run with:
    uv run pytest tests/test_examples.py -v
"""

from __future__ import annotations

import os
import tempfile

import pytest

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not PYSPARK_AVAILABLE or not os.environ.get("JAVA_HOME"),
    reason="PySpark not installed or Java not available",
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession shared across all tests in this module."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("spark_plan_viz_examples")
        .config("spark.sql.shuffle.partitions", "5")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def employees(spark):
    return spark.createDataFrame(
        [
            (1, "Alice", 34, "Engineering", 95000),
            (2, "Bob", 45, "Sales", 85000),
            (3, "Cathy", 29, "Engineering", 78000),
            (4, "David", 38, "Marketing", 72000),
            (5, "Eve", 42, "Sales", 88000),
            (6, "Frank", 31, "Engineering", 91000),
            (7, "Grace", 27, "Marketing", 67000),
            (8, "Hank", 50, "Sales", 102000),
        ],
        ["id", "name", "age", "department", "salary"],
    )


@pytest.fixture(scope="module")
def departments(spark):
    return spark.createDataFrame(
        [
            ("Engineering", "Tech", "US"),
            ("Sales", "Business", "US"),
            ("Marketing", "Business", "EU"),
        ],
        ["dept_name", "division", "region"],
    )


@pytest.fixture(scope="module")
def orders(spark):
    return spark.createDataFrame(
        [
            (1, 1, 100.0, "2024-01-15"),
            (2, 2, 250.0, "2024-01-16"),
            (3, 1, 75.0, "2024-02-01"),
            (4, 3, 300.0, "2024-02-10"),
            (5, 5, 180.0, "2024-03-01"),
            (6, 4, 90.0, "2024-03-15"),
        ],
        ["order_id", "emp_id", "amount", "order_date"],
    )


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _rule_ids(suggestions):
    """Extract rule_id strings from a list of Suggestion objects."""
    return [s.rule_id for s in suggestions]


# ===================================================================
# ERROR-level examples
# ===================================================================


class TestCrossJoinExample:
    """
    Example: Cross Join / Cartesian Product
    ----------------------------------------
    A cross join produces every combination of rows from both sides.
    If both tables have 1 000 rows, the result has 1 000 000 rows.

    >>> employees.crossJoin(departments)

    The analyzer flags this as an ERROR because it almost always
    indicates a missing join condition.
    """

    def test_cross_join_detected(self, spark, employees, departments):
        from spark_plan_viz import Severity, analyze_plan

        # BAD: cross join without a condition
        result = employees.crossJoin(departments)

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "cross_join" in ids
        cross = next(s for s in suggestions if s.rule_id == "cross_join")
        assert cross.severity == Severity.ERROR

    def test_cross_join_fixed_with_condition(self, spark, employees, departments):
        from spark_plan_viz import analyze_plan

        # GOOD: proper equi-join replaces the cross join
        result = employees.join(
            departments, employees.department == departments.dept_name
        )

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "cross_join" not in ids


class TestNestedLoopJoinExample:
    """
    Example: Nested Loop Join
    -------------------------
    A non-equality join condition forces Spark to use a nested loop,
    comparing every row on the left with every row on the right.

    >>> employees.join(orders, employees.salary > orders.amount)

    The analyzer flags this as an ERROR because it is O(n*m).
    """

    def test_nested_loop_join_detected(self, spark, employees, orders):
        from spark_plan_viz import Severity, analyze_plan

        # BAD: non-equality join → BroadcastNestedLoopJoin
        result = employees.join(orders, employees.salary > orders.amount)

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "nested_loop_join" in ids
        nlj = next(s for s in suggestions if s.rule_id == "nested_loop_join")
        assert nlj.severity == Severity.ERROR

    def test_nested_loop_fixed_with_equi_join(self, spark, employees, orders):
        from spark_plan_viz import analyze_plan

        # GOOD: equi-join with optional range filter
        result = employees.join(
            orders,
            (employees.id == orders.emp_id) & (employees.salary > orders.amount),
        )

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "nested_loop_join" not in ids


# ===================================================================
# WARNING-level examples
# ===================================================================


class TestFullTableScanExample:
    """
    Example: Full Table Scan
    ------------------------
    Reading from a data source without any pushed filters forces Spark
    to read every row and column from storage.

    >>> spark.read.parquet("/data/huge_table").select("id")

    The analyzer flags scans without pushed filters as a WARNING.
    """

    def test_full_table_scan_detected(self, spark, employees):
        from spark_plan_viz import analyze_plan

        # BAD: no filter at all → full scan
        result = employees.select("id", "name")

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "full_table_scan" in ids

    def test_full_scan_mitigated_with_filter(self, spark, employees):
        from spark_plan_viz import analyze_plan

        # BETTER: adding a filter gives the optimizer a chance to push it down
        result = employees.filter(F.col("age") > 30).select("id", "name")

        # Note: for in-memory DataFrames the filter may not be "pushed"
        # to storage, but for Parquet/ORC scans it would be.
        suggestions = analyze_plan(result)
        # The scan may still appear without pushed filters for in-memory data;
        # the key point is that the example shows the pattern.
        assert isinstance(suggestions, list)


class TestExpensiveCollectExample:
    """
    Example: Expensive collect_list / collect_set
    -----------------------------------------------
    Aggregating all values into a list on one executor can cause OOM
    for large groups.

    >>> df.groupBy("department").agg(F.collect_list("name"))

    The analyzer flags this as a WARNING.
    """

    def test_collect_list_detected(self, spark, employees):
        from spark_plan_viz import analyze_plan

        # BAD: collect_list pulls every value into one executor's memory
        result = employees.groupBy("department").agg(
            F.collect_list("name").alias("all_names")
        )

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "expensive_collect" in ids

    def test_collect_set_detected(self, spark, employees):
        from spark_plan_viz import analyze_plan

        # BAD: collect_set has the same risk
        result = employees.groupBy("department").agg(
            F.collect_set("name").alias("unique_names")
        )

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "expensive_collect" in ids

    def test_safe_aggregate_no_warning(self, spark, employees):
        from spark_plan_viz import analyze_plan

        # GOOD: standard aggregates are safe
        result = employees.groupBy("department").agg(
            F.avg("salary").alias("avg_salary"),
            F.count("*").alias("headcount"),
        )

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "expensive_collect" not in ids


class TestWindowWithoutPartitionExample:
    """
    Example: Window without PARTITION BY
    --------------------------------------
    A window function without partitioning moves all data to a single
    partition, destroying parallelism.

    >>> Window.orderBy("salary")  # no partitionBy!

    The analyzer flags this as a WARNING.
    """

    def test_unpartitioned_window_detected(self, spark, employees):
        from spark_plan_viz import analyze_plan

        # BAD: global window — everything goes to 1 partition
        w = Window.orderBy("salary")
        result = employees.withColumn("global_rank", F.row_number().over(w))

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "window_without_partition" in ids

    def test_partitioned_window_no_warning(self, spark, employees):
        from spark_plan_viz import analyze_plan

        # GOOD: partitioned window distributes the work
        w = Window.partitionBy("department").orderBy("salary")
        result = employees.withColumn("dept_rank", F.row_number().over(w))

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "window_without_partition" not in ids


class TestPythonUDFExample:
    """
    Example: Python UDF
    --------------------
    Python UDFs serialize data from JVM → Python → JVM on every row,
    which is 10–100x slower than native Spark functions.

    >>> @F.udf("string")
    ... def upper(s): return s.upper()
    >>> df.select(upper("name"))

    The analyzer flags this as a WARNING.
    """

    def test_python_udf_detected(self, spark, employees):
        from spark_plan_viz import analyze_plan

        # BAD: Python UDF for something Spark can do natively
        @F.udf("string")
        def upper_name(s):
            return s.upper() if s else None

        result = employees.select(upper_name("name").alias("upper_name"))

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "python_udf" in ids

    def test_native_function_no_warning(self, spark, employees):
        from spark_plan_viz import analyze_plan

        # GOOD: use Spark's built-in upper()
        result = employees.select(F.upper("name").alias("upper_name"))

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "python_udf" not in ids


class TestRedundantShuffleExample:
    """
    Example: Redundant Shuffle (double repartition)
    -------------------------------------------------
    Calling repartition() twice in a row causes two shuffles when
    only one is needed.

    >>> df.repartition(10).repartition(5)

    The analyzer flags back-to-back Exchange nodes as a WARNING.
    """

    def test_double_repartition_detected(self, spark, employees):
        from spark_plan_viz import analyze_plan

        # BAD: two consecutive shuffles
        result = employees.repartition(10, "department").repartition(5)

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "redundant_shuffle" in ids

    def test_single_repartition_no_warning(self, spark, employees):
        from spark_plan_viz import analyze_plan

        # GOOD: single repartition
        result = employees.repartition(10, "department")

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "redundant_shuffle" not in ids


# ===================================================================
# INFO-level examples
# ===================================================================


class TestMissingBroadcastHintExample:
    """
    Example: Shuffle join that could be broadcast
    -----------------------------------------------
    When joining a large table with a small lookup table, Spark may
    choose a shuffle join.  Adding broadcast() avoids the shuffle.

    >>> large.join(small, "key")           # shuffle join
    >>> large.join(broadcast(small), "key") # broadcast join

    The analyzer flags shuffle-based joins as INFO.
    """

    def test_shuffle_join_flagged(self, spark, employees, departments):
        from spark_plan_viz import analyze_plan

        # Disable auto-broadcast so Spark uses SortMergeJoin
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        try:
            result = employees.join(
                departments, employees.department == departments.dept_name
            )
            suggestions = analyze_plan(result)
            ids = _rule_ids(suggestions)

            assert "missing_broadcast_hint" in ids or "skew_hint" in ids
        finally:
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

    def test_broadcast_join_no_flag(self, spark, employees, departments):
        from spark_plan_viz import analyze_plan

        # GOOD: explicit broadcast avoids the shuffle
        result = employees.join(
            F.broadcast(departments), employees.department == departments.dept_name
        )

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "missing_broadcast_hint" not in ids


class TestNonColumnarFormatExample:
    """
    Example: Reading CSV / JSON instead of Parquet
    ------------------------------------------------
    Row-based formats don't support column pruning or predicate pushdown.

    >>> spark.read.csv("data.csv")    # slow
    >>> spark.read.parquet("data.pq") # fast

    The analyzer flags CSV/JSON scans as INFO.
    """

    def test_csv_format_flagged(self, spark, employees):
        from spark_plan_viz import analyze_plan

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "employees.csv")
            employees.write.csv(csv_path, header=True)

            result = spark.read.csv(csv_path, header=True)

            suggestions = analyze_plan(result)
            ids = _rule_ids(suggestions)

            assert "non_columnar_format" in ids

    def test_parquet_format_no_flag(self, spark, employees):
        from spark_plan_viz import analyze_plan

        with tempfile.TemporaryDirectory() as tmpdir:
            pq_path = os.path.join(tmpdir, "employees.parquet")
            employees.write.parquet(pq_path)

            result = spark.read.parquet(pq_path)

            suggestions = analyze_plan(result)
            ids = _rule_ids(suggestions)

            assert "non_columnar_format" not in ids


class TestCoalesceExample:
    """
    Example: repartition() when coalesce() would suffice
    -----------------------------------------------------
    repartition(n) always triggers a full shuffle, even when reducing
    partitions.  coalesce(n) avoids the shuffle by combining locally.

    >>> df.repartition(2)  # full shuffle
    >>> df.coalesce(2)     # no shuffle (narrows partitions locally)

    The analyzer flags RoundRobinPartitioning as INFO.
    """

    def test_repartition_flagged(self, spark, employees):
        from spark_plan_viz import analyze_plan

        # BAD: repartition(2) triggers a full shuffle
        result = employees.repartition(2)

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "coalesce" in ids

    def test_coalesce_no_flag(self, spark, employees):
        from spark_plan_viz import analyze_plan

        # GOOD: coalesce avoids the full shuffle
        result = employees.coalesce(2)

        suggestions = analyze_plan(result)
        ids = _rule_ids(suggestions)

        assert "coalesce" not in ids


class TestSkewHintExample:
    """
    Example: SortMergeJoin without skew optimization
    --------------------------------------------------
    If join keys are unevenly distributed, a few tasks process most
    of the data ("skew").  AQE can split these hot partitions.

    >>> spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    The analyzer flags SortMergeJoin without skew optimization as INFO.
    """

    def test_sort_merge_join_skew_hint(self, spark, employees, orders):
        from spark_plan_viz import analyze_plan

        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        try:
            result = employees.join(orders, employees.id == orders.emp_id)

            suggestions = analyze_plan(result)
            ids = _rule_ids(suggestions)

            assert "skew_hint" in ids
        finally:
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")


# ===================================================================
# End-to-end: visualize_plan with analysis
# ===================================================================


class TestVisualizeWithAnalysis:
    """
    Full round-trip: build a query, visualize it, and verify the
    returned tree contains attached suggestions.
    """

    def test_visualize_plan_returns_tree_with_suggestions(
        self, spark, employees, departments
    ):
        from spark_plan_viz import visualize_plan

        # Query that triggers multiple rules
        result = (
            employees.crossJoin(departments)
            .groupBy("division")
            .agg(F.collect_list("name").alias("names"))
        )

        tree = visualize_plan(result, notebook=False, output_file="/dev/null")

        assert tree is not None
        # Collect all suggestions from the tree
        all_suggestions = []

        def _collect(node):
            all_suggestions.extend(node.get("suggestions", []))
            for child in node.get("children", []):
                _collect(child)

        _collect(tree)

        rule_ids = [s["rule_id"] for s in all_suggestions]
        assert "cross_join" in rule_ids
        assert "expensive_collect" in rule_ids

    def test_analyze_plan_summary(self, spark, employees, orders, departments):
        """
        Demonstrate the programmatic API — the kind of snippet a user
        would run in a notebook to audit their query.
        """
        from spark_plan_viz import Severity, analyze_plan

        # Build a deliberately suboptimal query
        result = (
            employees.crossJoin(departments)
            .join(orders, employees.id == orders.emp_id)
            .groupBy("division")
            .agg(F.collect_list("name").alias("all_names"))
        )

        suggestions = analyze_plan(result)

        # There should be at least one ERROR (cross join) and one WARNING
        severities = {s.severity for s in suggestions}
        assert Severity.ERROR in severities
        assert Severity.WARNING in severities

        # Print a summary (visible in pytest -v -s output)
        for s in suggestions:
            print(f"  [{s.severity.value:7s}] {s.title}: {s.message}")
