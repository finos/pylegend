# Copyright 2023 Goldman Sachs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import pytest
from textwrap import dedent
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_tds_frame import LegendApiTdsFrame
from pylegend.extensions.tds.legend_api.frames.legend_api_table_spec_input_frame import LegendApiTableSpecInputFrame
from pylegend.tests.test_helpers.legend_service_frame import simple_person_service_frame, simple_trade_service_frame
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.language import AggregateSpecification


class TestGroupByAppliedFunction:

    def test_group_by_error_on_unknown_column(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.group_by(["col3"], [])
        assert r.value.args[0] == ("Column - 'col3' in group by columns list doesn't exist in the current frame. "
                                   "Current frame columns: ['col1', 'col2']")

    def test_group_by_error_on_empty_cols(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.group_by([], [])
        assert r.value.args[0] == ("At-least one grouping column or aggregate specification must be provided "
                                   "when using group_by function")

    def test_group_by_error_on_duplicate_cols(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.group_by(["col1"], [AggregateSpecification(lambda x: x, lambda y: y, "col1")])  # type: ignore
        assert r.value.args[0] == ("Found duplicate column names in grouping columns and aggregation columns. "
                                   "Grouping columns - ['col1'], Aggregation columns - ['col1']")

    def test_group_by_error_on_incompatible_map_fn(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.group_by(["col1"], [AggregateSpecification(lambda: 1, lambda y: y, "col3")])  # type: ignore
        assert r.value.args[0] == ("AggregateSpecification at index 0 (0-indexed) incompatible. "
                                   "Map function should be a lambda which takes one argument (TDSRow)")

    def test_group_by_error_on_map_fn_evaluation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame.group_by(["col1"], [AggregateSpecification(lambda x: x["col5"], lambda y: y, "col3")])  # type: ignore
        assert r.value.args[0] == ("AggregateSpecification at index 0 (0-indexed) incompatible. "
                                   "Error occurred while evaluating map function. "
                                   "Message: Column - 'col5' doesn't exist in the current frame. "
                                   "Current frame columns: ['col1', 'col2']")

    def test_group_by_error_on_map_fn_non_primitive(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.group_by(["col1"], [AggregateSpecification(lambda x: x, lambda y: y, "col3")])  # type: ignore
        assert r.value.args[0] == ("AggregateSpecification at index 0 (0-indexed) incompatible. Map function returns "
                                   "non-primitive - <class 'pylegend.core.language.tds_row.TdsRow'>")

    def test_group_by_error_on_incompatible_agg_fn(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.group_by(
                ["col1"],
                [AggregateSpecification(lambda x: x["col2"], lambda: 1, "col3")]  # type: ignore
            )
        assert r.value.args[0] == ("AggregateSpecification at index 0 (0-indexed) incompatible. Aggregate function "
                                   "should be a lambda which takes one argument (primitive collection)")

    def test_group_by_error_on_agg_fn_evaluation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame.group_by(
                ["col1"],
                [AggregateSpecification(lambda x: x["col2"], lambda y: y.unknown(), "col3")]  # type: ignore
            )
        assert r.value.args[0] == ("AggregateSpecification at index 0 (0-indexed) incompatible. "
                                   "Error occurred while evaluating aggregate function. "
                                   "Message: 'PyLegendStringCollection' object has no attribute 'unknown'")

    def test_group_by_error_on_agg_fn_non_primitive(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.group_by(
                ["col1"],
                [AggregateSpecification(lambda x: x["col2"], lambda y: y, "col3")]  # type: ignore
            )
        assert r.value.args[0] == ("AggregateSpecification at index 0 (0-indexed) incompatible. "
                                   "Aggregate function returns non-primitive - "
                                   "<class 'pylegend.core.language.primitive_collection.PyLegendStringCollection'>")

    def test_sql_gen_group_by(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col1"],
            [AggregateSpecification(lambda x: x["col2"], lambda y: y.count(), "Count")]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: Count, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                COUNT(
                    "root".col2
                ) AS "Count"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_group_by_with_distinct(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.distinct()
        frame = frame.group_by(
            ["col1"],
            [AggregateSpecification(lambda x: x["col2"], lambda y: y.count(), "Count")]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: Count, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                COUNT(
                    "root"."col2"
                ) AS "Count"
            FROM
                (
                    SELECT DISTINCT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
            GROUP BY
                "root"."col1"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_group_by_with_limit(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.take(10)
        frame = frame.group_by(
            ["col1"],
            [AggregateSpecification(lambda x: x["col2"], lambda y: y.count(), "Count")]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: Count, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                COUNT(
                    "root"."col2"
                ) AS "Count"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    LIMIT 10
                ) AS "root"
            GROUP BY
                "root"."col1"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_multi_group_by(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.take(10)
        frame = frame.group_by(
            ["col1"],
            [AggregateSpecification(lambda x: x["col2"], lambda y: y.count(), "Count1")]
        )
        frame = frame.group_by(
            ["col1"],
            [AggregateSpecification(lambda x: x["Count1"], lambda y: y.count(), "Count2")]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: Count2, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                COUNT(
                    "root"."Count1"
                ) AS "Count2"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        COUNT(
                            "root"."col2"
                        ) AS "Count1"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2"
                            FROM
                                test_schema.test_table AS "root"
                            LIMIT 10
                        ) AS "root"
                    GROUP BY
                        "root"."col1"
                ) AS "root"
            GROUP BY
                "root"."col1"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_group_by_multi_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col1"],
            [
                AggregateSpecification(lambda x: x["col2"], lambda y: y.count(), "Count1"),
                AggregateSpecification(lambda x: x["col2"], lambda y: y.count(), "Count2")
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: Count1, Type: Integer), "
            "TdsColumn(Name: Count2, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                COUNT(
                    "root".col2
                ) AS "Count1",
                COUNT(
                    "root".col2
                ) AS "Count2"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_group_by_average_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            [AggregateSpecification(lambda x: x["col1"], lambda y: y.average(), "Average")]  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Average, Type: Float)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                AVG(
                    "root".col1
                ) AS "Average"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_group_by_average_agg_pre_op(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            [AggregateSpecification(lambda x: x["col1"] + 20, lambda y: y.average(), "Average")]  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Average, Type: Float)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                AVG(
                    ("root".col1 + 20)
                ) AS "Average"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_group_by_average_agg_post_op(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            [AggregateSpecification(lambda x: x["col1"], lambda y: y.average() + 2, "Average")]  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Average, Type: Number)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                (AVG(
                    "root".col1
                ) + 2) AS "Average"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_group_by_integer_max_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            [AggregateSpecification(lambda x: x["col1"], lambda y: y.max(), "Maximum")]  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Maximum, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                MAX(
                    "root".col1
                ) AS "Maximum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_e2e_group_by(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Firm/Legal Name"],
            [
                AggregateSpecification(lambda x: x['First Name'], lambda y: y.count(), 'Employee Count')
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Firm/Legal Name, Type: String), TdsColumn(Name: Employee Count, Type: Integer)]"
        expected = {'columns': ['Firm/Legal Name', 'Employee Count'],
                    'rows': [{'values': ['Firm A', 1]},
                             {'values': ['Firm B', 1]},
                             {'values': ['Firm C', 1]},
                             {'values': ['Firm X', 4]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_with_limit(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server['engine_port'])
        frame = frame.take(5)
        frame = frame.group_by(
            ["Firm/Legal Name"],
            [
                AggregateSpecification(lambda x: x['First Name'], lambda y: y.count(), 'Employee Count')
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Firm/Legal Name, Type: String), TdsColumn(Name: Employee Count, Type: Integer)]"
        expected = {'columns': ['Firm/Legal Name', 'Employee Count'],
                    'rows': [{'values': ['Firm A', 1]},
                             {'values': ['Firm X', 4]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_on_literal(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server['engine_port'])
        frame = frame.group_by(
            [],
            [
                AggregateSpecification(lambda x: 1, lambda y: y.count(), 'Total Employees')
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Total Employees, Type: Integer)]"
        expected = {'columns': ['Total Employees'], 'rows': [{'values': [7]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_multi_grouping_cols(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) \
            -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Firm/Legal Name", "First Name"],
            [
                AggregateSpecification(lambda x: x['Last Name'], lambda y: y.count(), 'Employee Count')
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Firm/Legal Name, Type: String), TdsColumn(Name: First Name, Type: String), "
                "TdsColumn(Name: Employee Count, Type: Integer)]")
        expected = {'columns': ['Firm/Legal Name', 'First Name', 'Employee Count'],
                    'rows': [{'values': ['Firm A', 'Fabrice', 1]},
                             {'values': ['Firm B', 'Oliver', 1]},
                             {'values': ['Firm C', 'David', 1]},
                             {'values': ['Firm X', 'Anthony', 1]},
                             {'values': ['Firm X', 'John', 2]},
                             {'values': ['Firm X', 'Peter', 1]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    @pytest.mark.skip(reason="Legend server doesn't execute this SQL as group by clause has derivation")
    def test_e2e_group_by_on_extended_col(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server['engine_port'])
        frame = frame.extend([lambda x: x['Last Name'] + '_Gen'], ["Last Name Gen"])  # type: ignore
        frame = frame.group_by(
            ["Firm/Legal Name", "Last Name Gen"],
            [
                AggregateSpecification(lambda x: x['First Name'], lambda y: y.count(), 'Employee Count')
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Firm/Legal Name, Type: String), TdsColumn(Name: Last Name Gen, Type: String), "
                "TdsColumn(Name: Employee Count, Type: Integer)]")
        expected = {'columns': ['Firm/Legal Name', 'Employee Count'],
                    'rows': [{'values': ['Firm A', 1]},
                             {'values': ['Firm B', 1]},
                             {'values': ['Firm C', 1]},
                             {'values': ['Firm X', 4]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_multi_aggregations(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Firm/Legal Name"],
            [
                AggregateSpecification(lambda x: x['First Name'], lambda y: y.count(), 'Employee Count1'),
                AggregateSpecification(lambda x: x['First Name'], lambda y: y.count(), 'Employee Count2')
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Firm/Legal Name, Type: String), TdsColumn(Name: Employee Count1, Type: Integer), "
                "TdsColumn(Name: Employee Count2, Type: Integer)]")
        expected = {'columns': ['Firm/Legal Name', 'Employee Count1', 'Employee Count2'],
                    'rows': [{'values': ['Firm A', 1, 1]},
                             {'values': ['Firm B', 1, 1]},
                             {'values': ['Firm C', 1, 1]},
                             {'values': ['Firm X', 4, 4]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_average_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_trade_service_frame(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Product/Name"],
            [
                AggregateSpecification(
                    lambda x: x['Quantity'],
                    lambda y: y.average(),  # type: ignore
                    'Average Qty'
                )
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Product/Name, Type: String), TdsColumn(Name: Average Qty, Type: Float)]"
        expected = {'columns': ['Product/Name', 'Average Qty'],
                    'rows': [{'values': [None, 5]},
                             {'values': ['Firm A', 22]},
                             {'values': ['Firm C', 35.2]},
                             {'values': ['Firm X', 172.5]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_average_agg_pre_op(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_trade_service_frame(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Product/Name"],
            [
                AggregateSpecification(
                    lambda x: x['Quantity'] + 20,  # type: ignore
                    lambda y: y.average(),  # type: ignore
                    'Average Qty'
                )
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Product/Name, Type: String), TdsColumn(Name: Average Qty, Type: Float)]"
        expected = {'columns': ['Product/Name', 'Average Qty'],
                    'rows': [{'values': [None, 25]},
                             {'values': ['Firm A', 42]},
                             {'values': ['Firm C', 55.2]},
                             {'values': ['Firm X', 192.5]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_average_agg_post_op(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_trade_service_frame(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Product/Name"],
            [
                AggregateSpecification(
                    lambda x: x['Quantity'],
                    lambda y: y.average() + 2,  # type: ignore
                    'Average Qty'
                )
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Product/Name, Type: String), TdsColumn(Name: Average Qty, Type: Number)]"
        expected = {'columns': ['Product/Name', 'Average Qty'],
                    'rows': [{'values': [None, 7]},
                             {'values': ['Firm A', 24]},
                             {'values': ['Firm C', 37.2]},
                             {'values': ['Firm X', 174.5]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_integer_max_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_trade_service_frame(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Product/Name"],
            [
                AggregateSpecification(
                    lambda x: x['Id'],
                    lambda y: y.max(),  # type: ignore
                    'Max Trade Id'
                )
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Product/Name, Type: String), TdsColumn(Name: Max Trade Id, Type: Integer)]"
        expected = {'columns': ['Product/Name', 'Max Trade Id'],
                    'rows': [{'values': [None, 11]},
                             {'values': ['Firm A', 5]},
                             {'values': ['Firm C', 10]},
                             {'values': ['Firm X', 2]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
