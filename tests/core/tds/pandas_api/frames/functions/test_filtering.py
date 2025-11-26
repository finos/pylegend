# Copyright 2025 Goldman Sachs
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
import datetime

import pytest

from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.database.sql_to_string import (
    SqlToStringConfig,
    SqlToStringFormat
)


class TestFilteringFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_filtering_function_invalid_operator_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)  # noqa: F841

        # Logical expression
        with pytest.raises(TypeError) as v:
            frame[(frame['col1'] > 10) + (frame['col2'] == 2)]  # type: ignore
        assert v.value.args[0] == "unsupported operand type(s) for +: 'PyLegendBoolean' and 'PyLegendBoolean'"

        with pytest.raises(TypeError) as v:
            frame[(frame['col1'] > 10) ^ (frame['col2'] == 2)]  # type: ignore
        assert v.value.args[0] == "unsupported operand type(s) for ^: 'PyLegendBoolean' and 'PyLegendBoolean'"

        # Comparator expression
        with pytest.raises(TypeError) as v:
            frame[(frame['col1'] >> 10)]  # type: ignore
        assert v.value.args[0] == "unsupported operand type(s) for >>: 'IntegerSeries' and 'int'"

        with pytest.raises(TypeError) as v:
            frame[(frame['col1'] + 10) & (frame['col2'] == 2)]  # type: ignore
        assert v.value.args[0].startswith(
            "Boolean AND (&) parameter should be a bool or a boolean expression (PyLegendBoolean). Got value "
            "<pylegend.core.language.shared.primitives.integer.PyLegendInteger object"
        )

    def test_filtering_function_error_on_invalid_key(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)  # noqa: F841

        # str input
        with pytest.raises(KeyError) as v:
            frame['col6']
        assert v.value.args[0] == "['col6'] not in index"

        # list input
        with pytest.raises(KeyError) as v:
            frame[['col1', 'col7']]
        assert v.value.args[0] == "['col7'] not in index"

        with pytest.raises(KeyError) as v:
            frame[['col6', 'col7']]
        assert v.value.args[0] == "['col6', 'col7'] not in index"

        # expression input
        with pytest.raises(KeyError) as v:
            frame[(frame['col8'] > 10) & (frame['col1'] == 2) | (frame['col6'] == 'test')]  # type: ignore
        assert v.value.args[0] == "['col8'] not in index"

        # invalid key type
        with pytest.raises(TypeError) as v2:
            frame[123]  # type: ignore
        assert v2.value.args[0] == "Invalid key type: <class 'int'>. Expected str, list, or boolean expression"

    def test_filtering_function_on_sql_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.datetime_column("col3"),
            PrimitiveTdsColumn.strictdate_column("col4")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # Columns
        col = frame['col3']
        assert [c.get_name() for c in col.columns()] == ['col3']

        # Pure Query
        pure = col.to_pure_query()
        assert pure == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col3])'''
        )

        # SQL Query
        config = FrameToSqlConfig()
        sql_object = col.to_sql_query_object(config)  # type: ignore
        sql_to_string_config = SqlToStringConfig(format_=SqlToStringFormat(pretty=config.pretty))
        sql = config.sql_to_string_generator().generate_sql_string(sql_object, sql_to_string_config)
        expected = '''SELECT\n    "root".col3 AS "col3"\nFROM\n    test_schema.test_table AS "root"'''
        assert sql == expected

    def test_filtering_function_on_column_types(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.datetime_column("col3"),
            PrimitiveTdsColumn.strictdate_column("col4")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        newframe = frame[
            (frame['col3'] > datetime.datetime(2025, 1, 1)) &  # type: ignore
            (frame['col4'] == datetime.date(2025, 1, 2))
            ]

        expected = '''\
                     SELECT
                         "root".col1 AS "col1",
                         "root".col2 AS "col2",
                         "root".col3 AS "col3",
                         "root".col4 AS "col4"
                     FROM
                         test_schema.test_table AS "root"
                     WHERE
                         (("root".col3 > CAST('2025-01-01T00:00:00' AS TIMESTAMP)) AND ("root".col4 = CAST('2025-01-02' AS DATE)))'''  # noqa: E501
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->filter(c|(($c.col3 > %2025-01-01T00:00:00) && ($c.col4 == %2025-01-02)))'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->filter(c|(($c.col3 > %2025-01-01T00:00:00) && ($c.col4 == %2025-01-02)))"

    def test_filtering_function_on_str_input(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        newframe = frame['col1']
        expected = '''\
                   SELECT
                       "root".col1 AS "col1"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col1])"

    def test_filtering_function_on_list_input(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # Single column
        newframe = frame[['col1']]
        expected = '''\
                   SELECT
                       "root".col1 AS "col1"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col1])"

        # Multiple columns
        newframe = frame[['col1', 'col3', 'col5']]
        expected = '''\
                   SELECT
                       "root".col1 AS "col1",
                       "root".col3 AS "col3",
                       "root".col5 AS "col5"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1, col3, col5])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col1, col3, col5])"

    def test_filtering_function_on_expression_input(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.date_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # Simple expression
        newframe = frame[(frame['col1'] > 10)]  # type: ignore
        expected = '''\
                   SELECT
                       "root".col1 AS "col1",
                       "root".col2 AS "col2",
                       "root".col3 AS "col3",
                       "root".col4 AS "col4",
                       "root".col5 AS "col5"
                   FROM
                       test_schema.test_table AS "root"
                   WHERE
                       ("root".col1 > 10)'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->filter(c|($c.col1 > 10))'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->filter(c|($c.col1 > 10))"

        # Simple expression with column comparison
        newframe = frame[(frame['col1'] > frame['col2'])]  # type: ignore
        expected = '''\
                   SELECT
                       "root".col1 AS "col1",
                       "root".col2 AS "col2",
                       "root".col3 AS "col3",
                       "root".col4 AS "col4",
                       "root".col5 AS "col5"
                   FROM
                       test_schema.test_table AS "root"
                   WHERE
                       ("root".col1 > "root".col2)'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->filter(c|($c.col1 > $c.col2))'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->filter(c|($c.col1 > $c.col2))"

        # Complex expression
        newframe = frame[(frame['col1'] > 10) & (frame['col3'] < 5.5) | (frame['col5'] >= datetime.date(2003, 11, 10))]  # type: ignore  # noqa: E501
        expected = '''\
                   SELECT
                       "root".col1 AS "col1",
                       "root".col2 AS "col2",
                       "root".col3 AS "col3",
                       "root".col4 AS "col4",
                       "root".col5 AS "col5"
                   FROM
                       test_schema.test_table AS "root"
                   WHERE
                       ((("root".col1 > 10) AND ("root".col3 < 5.5)) OR ("root".col5 >= CAST('2003-11-10' AS DATE)))'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->filter(c|((($c.col1 > 10) && ($c.col3 < 5.5)) || ($c.col5 >= %2003-11-10)))'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->filter(c|((($c.col1 > 10) && ($c.col3 < 5.5)) || ($c.col5 >= %2003-11-10)))"

        # Complex expression with negation
        newframe = frame[
            ~(  # type: ignore
                    (
                            (frame['col1'] == 2) &
                            (frame['col2'] != frame['col3'])
                    ) |
                    (frame['col1'] > 9) |  # type: ignore
                    (frame['col5'] >= datetime.date(2003, 11, 10))  # type: ignore
            )
        ]
        expected = '''\
                   SELECT
                       "root".col1 AS "col1",
                       "root".col2 AS "col2",
                       "root".col3 AS "col3",
                       "root".col4 AS "col4",
                       "root".col5 AS "col5"
                   FROM
                       test_schema.test_table AS "root"
                   WHERE
                       NOT(((("root".col1 = 2) AND ("root".col2 <> "root".col3)) OR ("root".col1 > 9)) OR ("root".col5 >= CAST('2003-11-10' AS DATE)))'''  # noqa: E501
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->filter(c|(((($c.col1 == 2) && ($c.col2 != $c.col3)) || ($c.col1 > 9)) || ($c.col5 >= %2003-11-10))->not())'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->filter(c|(((($c.col1 == 2) && ($c.col2 != $c.col3)) || ($c.col1 > 9)) || ($c.col5 >= %2003-11-10))->not())"  # noqa: E501

    def test_filtering_function_chained(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.date_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # Nested filter
        newframe = frame[(frame['col1'] > 10)]  # type: ignore
        newframe = newframe[newframe['col3'] < 5.5]  # type: ignore
        expected = '''\
                   SELECT
                       "root".col1 AS "col1",
                       "root".col2 AS "col2",
                       "root".col3 AS "col3",
                       "root".col4 AS "col4",
                       "root".col5 AS "col5"
                   FROM
                       test_schema.test_table AS "root"
                   WHERE
                       (("root".col1 > 10) AND ("root".col3 < 5.5))'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->filter(c|($c.col1 > 10))
              ->filter(c|($c.col3 < 5.5))'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->filter(c|($c.col1 > 10))->filter(c|($c.col3 < 5.5))"

        # Truncate
        newframe = frame.truncate(before=1, after=3)
        newframe = newframe[newframe['col1'] > newframe['col2']]  # type: ignore
        expected = '''\
                   SELECT
                       "root"."col1" AS "col1",
                       "root"."col2" AS "col2",
                       "root"."col3" AS "col3",
                       "root"."col4" AS "col4",
                       "root"."col5" AS "col5"
                   FROM
                       (
                           SELECT
                               "root".col1 AS "col1",
                               "root".col2 AS "col2",
                               "root".col3 AS "col3",
                               "root".col4 AS "col4",
                               "root".col5 AS "col5"
                           FROM
                               test_schema.test_table AS "root"
                           LIMIT 3
                           OFFSET 1
                       ) AS "root"
                   WHERE
                       ("root"."col1" > "root"."col2")'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->slice(1, 4)
              ->filter(c|($c.col1 > $c.col2))'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->slice(1, 4)->filter(c|($c.col1 > $c.col2))"

        # Filter
        newframe = frame[
            ~(  # type: ignore
                    (
                            (frame['col1'] == 2) &
                            (frame['col2'] != frame['col3'])
                    ) |
                    (frame['col1'] > 9) |  # type: ignore
                    (frame['col5'] >= datetime.date(2003, 11, 10))  # type: ignore
            )
        ]
        newframe = newframe.filter(items=['col1', 'col5'])  # type: ignore
        expected = '''\
                   SELECT
                       "root".col1 AS "col1",
                       "root".col5 AS "col5"
                   FROM
                       test_schema.test_table AS "root"
                   WHERE
                       NOT(((("root".col1 = 2) AND ("root".col2 <> "root".col3)) OR ("root".col1 > 9)) OR ("root".col5 >= CAST('2003-11-10' AS DATE)))'''  # noqa: E501
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->filter(c|(((($c.col1 == 2) && ($c.col2 != $c.col3)) || ($c.col1 > 9)) || ($c.col5 >= %2003-11-10))->not())
              ->select(~[col1, col5])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->filter(c|(((($c.col1 == 2) && ($c.col2 != $c.col3)) || ($c.col1 > 9)) || ($c.col5 >= %2003-11-10))->not())->select(~[col1, col5])"  # noqa: E501

    def test_e2e_filtering_function_str_input(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        newframe = frame['Age']
        expected = {
            "columns": ["Age"],
            "rows": [
                {"values": [23]},
                {"values": [22]},
                {"values": [12]},
                {"values": [22]},
                {"values": [34]},
                {"values": [32]},
                {"values": [35]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_filtering_function_list_input(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        # Single column
        newframe = frame[['Age']]
        expected = {
            "columns": ["Age"],
            "rows": [
                {"values": [23]},
                {"values": [22]},
                {"values": [12]},
                {"values": [22]},
                {"values": [34]},
                {"values": [32]},
                {"values": [35]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # Multiple columns
        newframe = frame[['First Name', 'Age']]
        expected = {
            "columns": ["First Name", "Age"],
            "rows": [
                {"values": ["Peter", 23]},
                {"values": ["John", 22]},
                {"values": ["John", 12]},
                {"values": ["Anthony", 22]},
                {"values": ["Fabrice", 34]},
                {"values": ["Oliver", 32]},
                {"values": ["David", 35]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_filtering_function_expression_input(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        # Simple expression
        newframe = frame[(frame['Age'] > 22)]  # type: ignore
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B"]},
                {"values": ["David", "Harris", 35, "Firm C"]}
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # Complex expression
        newframe = frame[(frame['Age'] >= 22) & (frame['Firm/Legal Name'] == 'Firm X') | (frame['First Name'] == 'John')]  # type: ignore  # noqa: E501
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X"]}
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # Complex expression with negation
        newframe = frame[~((frame['Age'] >= 22) & (frame['Firm/Legal Name'] == 'Firm X') | (frame['First Name'] == 'John'))]  # type: ignore  # noqa: E501
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B"]},
                {"values": ["David", "Harris", 35, "Firm C"]}
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_filtering_function_chained(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        # Truncate
        newframe = frame.truncate(before=2, after=5)
        newframe = newframe[newframe['Age'] > 22]  # type: ignore
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B"]}
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # Filter
        newframe = frame[  # type: ignore
            ~((frame['Age'] >= 22) & (frame['Firm/Legal Name'] == 'Firm X') | (frame['First Name'] == 'John'))]  # type: ignore
        newframe = newframe.filter(items=['First Name', 'Age'])
        expected = {
            "columns": ["First Name", "Age"],
            "rows": [
                {"values": ["Fabrice", 34]},
                {"values": ["Oliver", 32]},
                {"values": ["David", 35]}
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
