#!/usr/bin/env python3
"""Test the parsing functions for extracting key information from Spark plan descriptions."""

import sys

sys.path.insert(0, "/Users/graziano/GitHub/spark_plan_viz/src")

from spark_plan_viz.visualize_plan import (
    _extract_aggregate_functions,
    _extract_filter_condition,
    _extract_join_type,
    _extract_selected_columns,
    _extract_table_name,
)


def test_join_extraction():
    desc1 = "SortMergeJoin [Name#1], [Name#5], Inner Join"
    desc2 = "BroadcastHashJoin [id#0], [user_id#10], LeftOuter Join"

    assert _extract_join_type(desc1) == "Inner", (
        f"Expected 'Inner', got {_extract_join_type(desc1)}"
    )
    assert _extract_join_type(desc2) == "LeftOuter", (
        f"Expected 'LeftOuter', got {_extract_join_type(desc2)}"
    )
    print("✓ Join type extraction works")


def test_filter_extraction():
    desc = "Filter (Age#1 > 30)"
    result = _extract_filter_condition(desc)
    assert result is not None and "Age" in result, (
        f"Expected filter condition with 'Age', got {result}"
    )
    print("✓ Filter condition extraction works")


def test_column_extraction():
    desc = "Project [Name#1, Age#2, Department#5]"
    result = _extract_selected_columns(desc)
    assert len(result) > 0, f"Expected columns, got {result}"
    assert "Name" in result[0], f"Expected 'Name' in columns, got {result}"
    print("✓ Column extraction works")


def test_aggregate_extraction():
    desc = "Aggregate [sum(Salary#10), count(Employee#5), avg(Age#2)]"
    result = _extract_aggregate_functions(desc)
    assert len(result) > 0, f"Expected aggregate functions, got {result}"
    print(f"✓ Aggregate extraction works: {result}")


def test_table_extraction():
    desc1 = "FileScan parquet [id#0,name#1] Location: /data/users/table_name"
    desc2 = "Scan parquet default.employees.employee_data[id#0,name#1]"

    result1 = _extract_table_name(desc1)
    result2 = _extract_table_name(desc2)

    assert result1 is not None, f"Expected table name, got {result1}"
    assert result2 is not None, f"Expected table name, got {result2}"
    print(f"✓ Table extraction works: {result1}, {result2}")
