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


class TestFilterAppliedFunction:

    def test_filter_function_error_on_unknown_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame.filter(lambda x: x.get_integer("col3"))  # type: ignore
        assert r.value.args[0] == ("Filter function incompatible. Error occurred while evaluating. Message: "
                                   "Column - 'col3' doesn't exist in the current frame. Current frame columns: "
                                   "['col1', 'col2']")

    def test_filter_function_error_non_lambda_arg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        with pytest.raises(TypeError) as r:
            frame.filter(1)  # type: ignore
        assert r.value.args[0] == "Filter function should be a lambda which takes one argument (TDSRow)"

    def test_filter_function_error_multi_param_lambda_arg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        with pytest.raises(TypeError) as r:
            frame.filter(lambda x, y: 1)  # type: ignore
        assert r.value.args[0] == "Filter function should be a lambda which takes one argument (TDSRow)"

    def test_filter_function_error_on_non_boolean_func(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame.filter(lambda x: x.get_integer("col1"))  # type: ignore
        assert r.value.args[0] == ("Filter function incompatible. Returns non boolean - "
                                   "<class 'pylegend.core.language.primitives.integer.PyLegendInteger'>")

    def test_sql_gen_filter_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.filter(lambda x: x.get_string("col2").startswith('A'))
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                ("root".col2 LIKE \'A%\')'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_filter_literal(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.filter(lambda x: 1 == 2)   # type: ignore
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                false'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_filter_function_chained(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.filter(lambda x: x.get_string("col2").startswith('A'))
        frame = frame.filter(lambda x: x.get_integer("col1") > 10)
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                (("root".col2 LIKE \'A%\') AND ("root".col1 > 10))'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_filter_function_chained_with_top(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.take(10)
        frame = frame.filter(lambda x: x.get_string("col2").startswith('A'))
        frame = frame.filter(lambda x: x.get_integer("col1") > 10)
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    LIMIT 10
                ) AS "root"
            WHERE
                (("root"."col2" LIKE \'A%\') AND ("root"."col1" > 10))'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    @pytest.mark.skip
    def test_e2e_filter_function_literal(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.filter(lambda r: 1 == 2)  # type: ignore
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': []}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_filter_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.filter(lambda r: r.get_integer("Age") < 25)
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X']},
                             {'values': ['John', 'Johnson', 22, 'Firm X']},
                             {'values': ['John', 'Hill', 12, 'Firm X']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_filter_function_chained(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.filter(lambda r: (r.get_integer("Age") < 25))
        frame = frame.filter(lambda r: (r.get_integer("Age") < 23))
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['John', 'Johnson', 22, 'Firm X']},
                             {'values': ['John', 'Hill', 12, 'Firm X']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_filter_function_with_top(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]])\
            -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.take(3)
        frame = frame.filter(lambda r: (r.get_integer("Age") < 25) | (r.get_integer("Age") >= 35))
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X']},
                             {'values': ['John', 'Johnson', 22, 'Firm X']},
                             {'values': ['John', 'Hill', 12, 'Firm X']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
