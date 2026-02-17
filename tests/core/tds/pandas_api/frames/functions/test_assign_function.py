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
from textwrap import dedent
from datetime import date, datetime
import pytest

from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from tests.test_helpers import generate_pure_query_and_compile


class TestAssignFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_assign_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        with pytest.raises(RuntimeError) as r:
            frame.assign(newcol=lambda x: [1, 2])  # type: ignore
        assert r.value.args[0] == "Type not supported"

    def test_apply_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # axis
        with pytest.raises(ValueError) as v:
            frame.apply(lambda x: x, axis=1)
        assert v.value.args[0] == "Only column-wise apply is supported. Use axis=0 or 'index'"
        # raw
        with pytest.raises(NotImplementedError) as n:
            frame.apply(lambda x: x, raw=True)
        assert n.value.args[0] == "raw=True is not supported. Use raw=False"
        # result_type
        with pytest.raises(NotImplementedError) as n:
            frame.apply(lambda x: x, result_type='expand')
        assert n.value.args[0] == "result_type is not supported"
        # by_row
        with pytest.raises(NotImplementedError) as n:
            frame.apply(lambda x: x, by_row=True)
        assert n.value.args[0] == "by_row must be False or 'compat'"
        # engine
        with pytest.raises(NotImplementedError) as n:
            frame.apply(lambda x: x, engine='numba')
        assert n.value.args[0] == "Only engine='python' is supported"
        # engine kwargs
        with pytest.raises(NotImplementedError) as n:
            frame.apply(lambda x: x, engine_kwargs={'optimize': True})
        assert n.value.args[0] == "engine_kwargs are not supported"
        # str function
        with pytest.raises(NotImplementedError) as n:
            frame.apply("sum", axis=0)
        assert n.value.args[0] == "String-based apply is not supported"
        # invalid function
        with pytest.raises(TypeError) as t:
            frame.apply(123)  # type: ignore
        assert t.value.args[0] == "Function must be a callable"

    def test_assign(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.assign(sumColumn=lambda x: x.get_integer("col1") + x.get_integer("col2"))

        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                ("root".col1 + "root".col2) AS "sumColumn"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, sumColumn:c|(toOne($c.col1) + toOne($c.col2))])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        frame = frame.assign(col1=lambda x: x['col2']+5)  # type: ignore
        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, sumColumn:c|(toOne($c.col1) + "
            "toOne($c.col2))])\n"
            "  ->project(~[col1:c|(toOne($c.col2) + 5), "
            "col2:c|$c.col2, sumColumn:c|$c.sumColumn])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)
        expected_sql = (
            'SELECT\n'
            '    ("root".col2 + 5) AS "col1",\n'
            '    "root".col2 AS "col2",\n'
            '    ("root".col1 + "root".col2) AS "sumColumn"\n'
            'FROM\n'
            '    test_schema.test_table AS "root"'
        )
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_assign_float_date(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # Float column
        frame = frame.assign(floatcol=lambda x: 3.14)
        expected_sql = dedent('''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                3.14 AS "floatcol"
            FROM
                test_schema.test_table AS "root"''')
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql
        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, floatcol:c|3.14])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        # Date column
        frame = frame.assign(datecol=lambda x: date(2023, 12, 25))
        expected_sql = dedent('''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                3.14 AS "floatcol",
                CAST('2023-12-25' AS DATE) AS "datecol"
            FROM
                test_schema.test_table AS "root"''')
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql
        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, floatcol:c|3.14])\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, floatcol:c|$c.floatcol, datecol:c|%2023-12-25])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

    def test_assign_constant(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.datetime_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame = frame.assign(newcol=lambda x: 2)

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, newcol:c|2])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        expected_sql = dedent('''\
        SELECT
            "root".col1 AS "col1",
            "root".col2 AS "col2",
            2 AS "newcol"
        FROM
            test_schema.test_table AS "root"''')
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql

        frame = frame.assign(datecol=lambda x: datetime(2024, 1, 1, 12, 30, 0))

        assert frame.to_sql_query(FrameToSqlConfig()) == dedent('''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                2 AS "newcol",
                CAST('2024-01-01T12:30:00' AS TIMESTAMP) AS "datecol"
            FROM
                test_schema.test_table AS "root"''')

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, newcol:c|2])\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, newcol:c|$c.newcol, datecol:c|%2024-01-01T12:30:00])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

    def test_apply(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        def add_offset(series, offset, *, scale=1, label=None):  # type: ignore
            return series * scale + offset

        frame = frame.apply(add_offset, args=(2,), scale=3, label="bump")

        expected_sql = dedent('''\
            SELECT
                (("root".col1 * 3) + 2) AS "col1",
                (("root".col2 * 3) + 2) AS "col2"
            FROM
                test_schema.test_table AS "root"''')
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|((toOne($c.col1) * 3) + 2), col2:c|((toOne($c.col2) * 3) + 2)])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

    def test_e2e_assign_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.assign(fullName=lambda x: x.get_string("First Name") + " " + x.get_string("Last Name"))
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name', 'fullName'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X', 'Peter Smith']},
                             {'values': ['John', 'Johnson', 22, 'Firm X', 'John Johnson']},
                             {'values': ['John', 'Hill', 12, 'Firm X', 'John Hill']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X', 'Anthony Allen']},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A', 'Fabrice Roberts']},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B', 'Oliver Hill']},
                             {'values': ['David', 'Harris', 35, 'Firm C', 'David Harris']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        frame = frame.assign(newcol=lambda x: 100)
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name', 'fullName', 'newcol'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X', 'Peter Smith', 100]},
                             {'values': ['John', 'Johnson', 22, 'Firm X', 'John Johnson', 100]},
                             {'values': ['John', 'Hill', 12, 'Firm X', 'John Hill', 100]},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X', 'Anthony Allen', 100]},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A', 'Fabrice Roberts', 100]},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B', 'Oliver Hill', 100]},
                             {'values': ['David', 'Harris', 35, 'Firm C', 'David Harris', 100]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_apply_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame = frame.filter(items=['First Name', 'Last Name', 'Firm/Legal Name'])

        def add_suffix(series, suffix, *, uppercase=False, label=None):  # type: ignore
            result = series + suffix
            if uppercase:
                result = result.upper()
            return result

        frame = frame.apply(add_suffix, args=(" Esq.",), uppercase=True, label="suffixing")
        expected = {'columns': ['First Name', 'Last Name', 'Firm/Legal Name'],
                    'rows': [{'values': ['PETER ESQ.', 'SMITH ESQ.', 'FIRM X ESQ.']},
                             {'values': ['JOHN ESQ.', 'JOHNSON ESQ.', 'FIRM X ESQ.']},
                             {'values': ['JOHN ESQ.', 'HILL ESQ.', 'FIRM X ESQ.']},
                             {'values': ['ANTHONY ESQ.', 'ALLEN ESQ.', 'FIRM X ESQ.']},
                             {'values': ['FABRICE ESQ.', 'ROBERTS ESQ.', 'FIRM A ESQ.']},
                             {'values': ['OLIVER ESQ.', 'HILL ESQ.', 'FIRM B ESQ.']},
                             {'values': ['DAVID ESQ.', 'HARRIS ESQ.', 'FIRM C ESQ.']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # lambda
        frame = frame.apply(lambda x: x.lower())  # type: ignore
        expected = {'columns': ['First Name', 'Last Name', 'Firm/Legal Name'],
                    'rows': [{'values': ['peter esq.', 'smith esq.', 'firm x esq.']},
                             {'values': ['john esq.', 'johnson esq.', 'firm x esq.']},
                             {'values': ['john esq.', 'hill esq.', 'firm x esq.']},
                             {'values': ['anthony esq.', 'allen esq.', 'firm x esq.']},
                             {'values': ['fabrice esq.', 'roberts esq.', 'firm a esq.']},
                             {'values': ['oliver esq.', 'hill esq.', 'firm b esq.']},
                             {'values': ['david esq.', 'harris esq.', 'firm c esq.']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
