"""Test the parsing functions for extracting key information from Spark plan descriptions."""

from __future__ import annotations

from spark_plan_viz.api import (
    _classify_node_type,
    _extract_aggregate_functions,
    _extract_expand_projections,
    _extract_filter_condition,
    _extract_generator_name,
    _extract_join_condition,
    _extract_join_type,
    _extract_partition_filters,
    _extract_selected_columns,
    _extract_shuffle_info,
    _extract_table_name,
    _has_partition_columns,
    _is_skew_join,
)


def test_join_extraction() -> None:
    desc1 = "SortMergeJoin [Name#1], [Name#5], Inner Join"
    desc2 = "BroadcastHashJoin [id#0], [user_id#10], LeftOuter Join"
    desc3 = "SortMergeJoin [id#0], [id#1], Inner"

    assert _extract_join_type(desc1) == "Inner"
    assert _extract_join_type(desc2) == "LeftOuter"
    assert _extract_join_type(desc3) == "Inner"


def test_filter_extraction() -> None:
    desc = "Filter (Age#1 > 30)"
    result = _extract_filter_condition(desc)
    assert result is not None and "Age" in result


def test_join_condition_extraction() -> None:
    equi_desc = "BroadcastHashJoin [id#0], [user_id#10], Inner, BuildRight, false"
    non_equi_desc = "BroadcastNestedLoopJoin BuildRight, Inner, (salary#1 > amount#2)"
    explicit_desc = "Join condition: (id#0 = user_id#10)"

    assert _extract_join_condition(equi_desc) is None
    assert _extract_join_condition(non_equi_desc) == "(salary#1 > amount#2)"
    assert _extract_join_condition(explicit_desc) == "(id#0 = user_id#10)"


def test_column_extraction() -> None:
    desc = "Project [Name#1, Age#2, Department#5]"
    result = _extract_selected_columns(desc)
    assert len(result) > 0
    assert "Name" in result[0]


def test_aggregate_extraction() -> None:
    desc = "Aggregate [sum(Salary#10), count(Employee#5), avg(Age#2)]"
    result = _extract_aggregate_functions(desc)
    assert len(result) > 0


def test_table_extraction() -> None:
    desc1 = "FileScan parquet [id#0,name#1] Location: /data/users/table_name"
    desc2 = "Scan parquet default.employees.employee_data[id#0,name#1]"

    assert _extract_table_name(desc1) is not None
    assert _extract_table_name(desc2) is not None


def test_shuffle_info_extraction() -> None:
    rr_desc = "Exchange RoundRobinPartitioning(10)"
    hash_desc = "Exchange hashpartitioning(id#1, 200)"
    single_desc = "Exchange SinglePartition"

    assert _extract_shuffle_info(rr_desc)["partitions"] == "10"
    assert _extract_shuffle_info(rr_desc)["shuffle_type"] == "RoundRobin"
    assert _extract_shuffle_info(hash_desc)["partitions"] == "200"
    assert _extract_shuffle_info(single_desc)["partitions"] == "1"
    assert _extract_shuffle_info(single_desc)["shuffle_type"] == "SinglePartition"


def test_expand_and_generate_extraction() -> None:
    expand_desc = (
        "Expand [[a#1, b#2, 0], [a#1, null, 1], [null, b#2, 2], [null, null, 3]], "
        "[a#3, b#4, spark_grouping_id#5]"
    )
    assert _extract_expand_projections(expand_desc) == 4
    assert (
        _extract_generator_name(
            "Generate explode([1,2]), [id#0L], false, [x#4]", "Generate"
        )
        == "explode"
    )


def test_partition_filter_extraction() -> None:
    partitioned = (
        "FileScan parquet [id#67L,p#68] Batched: true, DataFilters: [], "
        "Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/t], "
        "PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:bigint>"
    )
    pruned = (
        "FileScan parquet [id#71L,p#72] Batched: true, DataFilters: [], "
        "Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/t], "
        "PartitionFilters: [isnotnull(p#72), (p#72 = 0)], PushedFilters: [], "
        "ReadSchema: struct<id:bigint>"
    )
    assert _has_partition_columns(partitioned)
    assert _extract_partition_filters(partitioned) == []
    assert _extract_partition_filters(pruned) is not None
    assert len(_extract_partition_filters(pruned) or []) == 2


def test_skew_join_detection() -> None:
    assert _is_skew_join("SortMergeJoin [a], [b], Inner, true", "SortMergeJoin")
    assert not _is_skew_join("SortMergeJoin [a], [b], Inner, false", "SortMergeJoin")
    assert _is_skew_join("SortMergeJoin isSkew=true", "SortMergeJoin")


def test_classify_node_type() -> None:
    assert _classify_node_type("BroadcastExchange") == "broadcast"
    assert _classify_node_type("AQEShuffleRead") == "shuffle_read"
    assert _classify_node_type("CustomShuffleReader") == "shuffle_read"
    assert _classify_node_type("Expand") == "expand"
    assert _classify_node_type("Generate") == "generate"
    assert _classify_node_type("FileScan parquet") == "scan"
    assert _classify_node_type("SortMergeJoin") == "join"
