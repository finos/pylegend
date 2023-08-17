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
from pylegend.tests.test_helpers.legend_service_frame import simple_person_service_frame
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)


class TestExtendAppliedFunction:

    def test_extend_function_error_on_diff_sizes(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.extend([lambda x: x.get_integer("col3")], ["col4", "col5"])
        assert r.value.args[0] == ("For extend function, function list and column names list arguments should be of "
                                   "same size. Passed param sizes -  Functions: 1, Column names: 2")

    def test_extend_function_error_on_non_lambda_func(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.extend([1], ["col4"])  # type: ignore
        assert r.value.args[0] == ("Error at extend function at index 0 (0-indexed). Each extend function "
                                   "should be a lambda which takes one argument (TDSRow)")

    def test_extend_function_error_on_non_string_name(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.extend([lambda x: 1], [1])  # type: ignore
        assert r.value.args[0] == "Error at extend column name at index 0 (0-indexed). Column name should be a string"

    def test_extend_function_error_on_incompatible_lambda_1(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame.extend([lambda x: x.get_integer("col3")], ["col4"])
        assert r.value.args[0] == ("Extend function at index 0 (0-indexed) incompatible. "
                                   "Error occurred while evaluating. Message: "
                                   "Column - 'col3' doesn't exist in the current frame. "
                                   "Current frame columns: ['col1', 'col2']")

    def test_extend_function_error_on_incompatible_lambda_2(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.extend([lambda x: {}], ["col4"])  # type: ignore
        assert r.value.args[0] == ("Extend function at index 0 (0-indexed) incompatible. "
                                   "Returns non-primitive - <class 'dict'>")

    def test_extend_function_error_on_duplicate_column_names(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.extend([lambda x: 1, lambda x: 2], ["col4", "col4"])
        assert r.value.args[0] == "Extend column names list has duplicates: ['col4', 'col4']"

    def test_extend_function_error_on_conflicting_column_names(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.extend([lambda x: 1, lambda x: 2], ["col1", "col4"])
        assert r.value.args[0] == "Extend column name - 'col1' already exists in base frame"

    def test_sql_gen_extend_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.extend([lambda x: x.get_integer("col1") + 1], ["col3"])
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                ("root".col1 + 1) AS "col3"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_extend_function_multi(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.extend([
            lambda x: x.get_integer("col1") + 1,
            lambda x: x.get_integer("col1") + 2
        ], ["col3", "col4"])
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                ("root".col1 + 1) AS "col3",
                ("root".col1 + 2) AS "col4"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_e2e_extend_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.extend([lambda r: r.get_string("First Name").upper()], ["Upper"])
        assert ("[" + ", ".join([str(c) for c in frame.columns()]) + "]" ==
                "[TdsColumn(Name: First Name, Type: String), TdsColumn(Name: Last Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Firm/Legal Name, Type: String), "
                "TdsColumn(Name: Upper, Type: String)]")
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name', 'Upper'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X', 'PETER']},
                             {'values': ['John', 'Johnson', 22, 'Firm X', 'JOHN']},
                             {'values': ['John', 'Hill', 12, 'Firm X', 'JOHN']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X', 'ANTHONY']},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A', 'FABRICE']},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B', 'OLIVER']},
                             {'values': ['David', 'Harris', 35, 'Firm C', 'DAVID']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_extend_function_multi(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.extend([
            lambda r: r.get_string("First Name").upper(),
            lambda r: r["Age"] < 25  # type: ignore
        ], ["Upper", "AgeCheck"])
        assert ("[" + ", ".join([str(c) for c in frame.columns()]) + "]" ==
                "[TdsColumn(Name: First Name, Type: String), TdsColumn(Name: Last Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Firm/Legal Name, Type: String), "
                "TdsColumn(Name: Upper, Type: String), TdsColumn(Name: AgeCheck, Type: Boolean)]")
        expected = {'columns': ['First Name',
                                'Last Name',
                                'Age',
                                'Firm/Legal Name',
                                'Upper',
                                'AgeCheck'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X', 'PETER', True]},
                             {'values': ['John', 'Johnson', 22, 'Firm X', 'JOHN', True]},
                             {'values': ['John', 'Hill', 12, 'Firm X', 'JOHN', True]},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X', 'ANTHONY', True]},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A', 'FABRICE', False]},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B', 'OLIVER', False]},
                             {'values': ['David', 'Harris', 35, 'Firm C', 'DAVID', False]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
