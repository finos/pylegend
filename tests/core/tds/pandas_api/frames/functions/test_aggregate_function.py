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

import numpy as np
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
import pytest
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import (
    simple_person_service_frame_pandas_api,
    simple_trade_service_frame_pandas_api
)


class TestAggregateFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_aggregate_error_invalid_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.string_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.aggregate(func=lambda x: 0, axis=1)
        assert v.value.args[0] == "The 'axis' parameter of the aggregate function must be 0 or 'index', but got: 1"

    def test_aggregate_error_invalid_args_kwargs(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.string_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.aggregate('sum', 0, 12, dummy_arg=23)
        assert v.value.args[0] == "AggregateFunction currently does not support additional positional " +\
            "or keyword arguments. Please remove extra *args/**kwargs."

    def test_aggregate_error_dict_invalid_key_type(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.aggregate({1: "sum"})

        expected_msg = (
            "Invalid `func` argument for the aggregate function.\n"
            "When a dictionary is provided, all keys must be strings.\n"
            "But got key: 1 (type: int)\n"
        )
        assert v.value.args[0] == expected_msg

    def test_aggregate_error_dict_unknown_column(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as v:
            frame.aggregate({"col2": "sum"})

        expected_msg = (
            "Invalid `func` argument for the aggregate function.\n"
            "When a dictionary is provided, all keys must be column names.\n"
            "Available columns are: ['col1']\n"
            "But got key: 'col2' (type: str)\n"
        )
        assert expected_msg == v.value.args[0]

    def test_aggregate_error_dict_value_list_invalid_length(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as v:
            frame.aggregate({"col1": ["min", "max"]})

        expected_msg = (
            "Invalid `func` argument for the aggregate function.\n"
            "When providing a list of functions for a specific column, "
            "the list must contain exactly one element (single aggregation only).\n"
            "Column: 'col1'\n"
            "List Length: 2\n"
            "Value: ['min', 'max']\n"
        )
        assert v.value.args[0] == expected_msg

    def test_aggregate_error_dict_value_list_invalid_content_type(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.aggregate({"col1": [123]})  # type: ignore

        expected_msg = (
            "Invalid `func` argument for the aggregate function.\n"
            "The single element in the list for key 'col1' must be a callable, str, or np.ufunc.\n"
            "But got element: 123 (type: int)\n"
        )
        assert v.value.args[0] == expected_msg

    def test_aggregate_error_dict_value_scalar_invalid_type(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.aggregate({"col1": 123})  # type: ignore

        expected_msg = (
            "Invalid `func` argument for the aggregate function.\n"
            "When a dictionary is provided, the value must be a callable, str, or np.ufunc "
            "(or a list containing exactly one of these).\n"
            "But got value for key 'col1': 123 (type: int)\n"
        )
        assert v.value.args[0] == expected_msg

    def test_aggregate_error_list_input_invalid_length(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as v:
            frame.aggregate(["min", "max"])

        expected_msg = (
            "Invalid `func` argument for the aggregate function.\n"
            "When providing a list as the func argument, it must contain exactly one element "
            "(which will be applied to all columns).\n"
            "Multiple functions are not supported.\n"
            "List Length: 2\n"
            "Input: ['min', 'max']\n"
        )
        assert v.value.args[0] == expected_msg

    def test_aggregate_error_list_input_empty(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as v:
            frame.aggregate([])

        expected_msg = (
            "Invalid `func` argument for the aggregate function.\n"
            "When providing a list as the func argument, it must contain exactly one element "
            "(which will be applied to all columns).\n"
            "Multiple functions are not supported.\n"
            "List Length: 0\n"
            "Input: []\n"
        )
        assert v.value.args[0] == expected_msg

    def test_aggregate_error_list_input_invalid_content_type(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.aggregate([123])  # type: ignore

        expected_msg = (
            "Invalid `func` argument for the aggregate function.\n"
            "The single element in the top-level list must be a callable, str, or np.ufunc.\n"
            "But got element: 123 (type: int)\n"
        )
        assert v.value.args[0] == expected_msg

    def test_aggregate_error_scalar_input_invalid_type(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.aggregate(123)  # type: ignore

        expected_msg = (
            "Invalid `func` argument for aggregate function. "
            "Expected a callable, str, np.ufunc, a list containing exactly one of these, "
            "or a mapping[str -> callable/str/ufunc/a list containing exactly one of these]. "
            "But got: 123 (type: int)"
        )
        assert v.value.args[0] == expected_msg

    def test_aggregate_simple_query_generation(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.date_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.aggregate({'col1': ['min'], 'col2': ['count']})
        expected = """\
                    SELECT
                        MIN("root".col1) AS "col1",
                        COUNT("root".col2) AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)[:-1]
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->aggregate(
                ~[col1:{r | $r.col1}:{c | $c->min()}, col2:{r | $r.col2}:{c | $c->count()}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->aggregate(~[col1:{r | $r.col1}:{c | $c->min()}, col2:{r | $r.col2}:{c | $c->count()}])"
        )

    def test_aggregate_for_bool_and_datetime_column(self) -> None:
        columns = [PrimitiveTdsColumn.boolean_column("col1"), PrimitiveTdsColumn.datetime_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.aggregate({'col1': ['count'], 'col2': ['min']})
        expected = """\
                    SELECT
                        COUNT("root".col1) AS "col1",
                        MIN("root".col2) AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)[:-1]

    def test_aggregate_fewer_metrics_than_columns(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1"),
                   PrimitiveTdsColumn.number_column("col2"),
                   PrimitiveTdsColumn.float_column("col3")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.aggregate({'col3': ['var'], 'col2': ['std']})
        expected = """\
                    SELECT
                        VAR_SAMP("root".col3) AS "col3",
                        STDDEV_SAMP("root".col2) AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)[:-1]
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->aggregate(
                ~[col3:{r | $r.col3}:{c | $c->varianceSample()}, col2:{r | $r.col2}:{c | $c->stdDevSample()}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->aggregate(~[col3:{r | $r.col3}:{c | $c->varianceSample()}, col2:{r | $r.col2}:{c | $c->stdDevSample()}])"
        )

    def test_aggregate_repeat_column(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1"),
                   PrimitiveTdsColumn.number_column("col2"),
                   PrimitiveTdsColumn.float_column("col3")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.aggregate({'col3': ['var'], 'col3': ['std']})  # noqa
        expected = """\
                    SELECT
                        STDDEV_SAMP("root".col3) AS "col3"
                    FROM
                        test_schema.test_table AS "root"
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)[:-1]
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->aggregate(
                ~[col3:{r | $r.col3}:{c | $c->stdDevSample()}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->aggregate(~[col3:{r | $r.col3}:{c | $c->stdDevSample()}])"
        )

    def test_aggregate_subquery_generation(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.integer_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.truncate(5, 10).aggregate({'col1': ['min'], 'col2': ['count']})
        expected = """\
                    SELECT
                        MIN("root"."col1") AS "col1",
                        COUNT("root"."col2") AS "col2"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2"
                            FROM
                                test_schema.test_table AS "root"
                            LIMIT 6
                            OFFSET 5
                        ) AS "root"
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)[:-1]
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->slice(5, 11)
              ->aggregate(
                ~[col1:{r | $r.col1}:{c | $c->min()}, col2:{r | $r.col2}:{c | $c->count()}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->slice(5, 11)"
            "->aggregate(~[col1:{r | $r.col1}:{c | $c->min()}, col2:{r | $r.col2}:{c | $c->count()}])"
        )

    def test_e2e_aggregate_single_column(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.aggregate({'Age': 'sum'})
        expected = {
            "columns": ["Age"],
            "rows": [{"values": [180]}],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_aggregate_single_column_list_input(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.aggregate({'Age': ['sum']})
        expected = {
            "columns": ["Age"],
            "rows": [{"values": [180]}]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_aggregate_numpy_ufunc_and_aliases(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.aggregate({
            'Age': np.min,
            'First Name': 'len'
        })
        expected = {
            "columns": ["Age", "First Name"],
            "rows": [{"values": [12, 7]}]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_aggregate_custom_lambda(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.aggregate({'Age': lambda x: x.max()})
        expected = {
            "columns": ["Age"],
            "rows": [{"values": [35]}]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_aggregate_multi_column_strings(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.aggregate({
            'First Name': 'min',
            'Last Name': 'max'
        })
        expected = {
            "columns": ["First Name", "Last Name"],
            "rows": [{"values": ["Anthony", "Smith"]}]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_aggregate_broadcast_scalar(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.aggregate('count')
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [{"values": [7, 7, 7, 7]}]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_aggregate_broadcast_ufunc_in_list(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.aggregate([np.maximum])
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [{"values": ["Peter", "Smith", 35, "Firm X"]}]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_aggregate_standard_stats(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame_min = frame.aggregate({'Age': 'min'})
        expected_min = {
            "columns": ["Age"],
            "rows": [{"values": [12]}]
        }
        assert json.loads(frame_min.execute_frame_to_string())["result"] == expected_min

        frame_max = frame.aggregate({'Age': 'max'})
        expected_max = {
            "columns": ["Age"],
            "rows": [{"values": [35]}]
        }
        assert json.loads(frame_max.execute_frame_to_string())["result"] == expected_max

        frame_mean = frame.aggregate({'Age': 'mean'})
        res_mean = json.loads(frame_mean.execute_frame_to_string())["result"]
        assert res_mean["columns"] == ["Age"]
        assert abs(res_mean["rows"][0]["values"][0] - 25.7142857143) < 0.0001

    def test_e2e_aggregate_aliases_count(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame_len = frame.aggregate({'First Name': 'len'})
        expected_count = {
            "columns": ["First Name"],
            "rows": [{"values": [7]}]
        }
        assert json.loads(frame_len.execute_frame_to_string())["result"] == expected_count

        frame_size = frame.aggregate({'Last Name': 'size'})
        expected_count = {
            "columns": ["Last Name"],
            "rows": [{"values": [7]}]
        }
        assert json.loads(frame_size.execute_frame_to_string())["result"] == expected_count

        frame_count = frame.aggregate({'Age': 'count'})
        expected_count = {
            "columns": ["Age"],
            "rows": [{"values": [7]}]
        }
        assert json.loads(frame_count.execute_frame_to_string())["result"]["rows"][0]["values"][0] == 7

    def test_e2e_aggregate_numpy_and_builtin(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame_np_sum = frame.aggregate({'Age': np.sum})
        expected_sum = {
            "columns": ["Age"],
            "rows": [{"values": [180]}]
        }
        assert json.loads(frame_np_sum.execute_frame_to_string())["result"] == expected_sum

        frame_np_min = frame.aggregate({'Age': np.min})
        expected_min = {
            "columns": ["Age"],
            "rows": [{"values": [12]}]
        }
        assert json.loads(frame_np_min.execute_frame_to_string())["result"] == expected_min

        frame_builtin_len = frame.aggregate({'Firm/Legal Name': len})
        expected_len = {
            "columns": ["Firm/Legal Name"],
            "rows": [{"values": [7]}]
        }
        assert json.loads(frame_builtin_len.execute_frame_to_string())["result"] == expected_len

    def test_e2e_aggregate_multi_column(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame_multi = frame.aggregate({'Age': 'sum', 'First Name': 'count'})

        expected = {
            "columns": ["Age", "First Name"],
            "rows": [
                {"values": [180, 7]},
            ],
        }

        res = json.loads(frame_multi.execute_frame_to_string())["result"]
        assert res == expected

    def test_e2e_aggregate_string_lexicographical(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame_str_min = frame.aggregate({'First Name': 'min'})
        expected_min = {
            "columns": ["First Name"],
            "rows": [{"values": ["Anthony"]}]
        }
        assert json.loads(frame_str_min.execute_frame_to_string())["result"] == expected_min

        frame_str_max = frame.aggregate({'First Name': 'max'})
        expected_max = {
            "columns": ["First Name"],
            "rows": [{"values": ["Peter"]}]
        }
        assert json.loads(frame_str_max.execute_frame_to_string())["result"] == expected_max

    def test_e2e_aggregate_lambda_functions(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame_lambda_sum = frame.aggregate({'Age': lambda x: x.sum()})
        expected_sum = {
            "columns": ["Age"],
            "rows": [{"values": [180]}]
        }
        assert json.loads(frame_lambda_sum.execute_frame_to_string())["result"] == expected_sum

        frame_lambda_count = frame.agg({'Last Name': lambda x: x.count()})
        expected_count = {
            "columns": ["Last Name"],
            "rows": [{"values": [7]}]
        }
        assert json.loads(frame_lambda_count.execute_frame_to_string())["result"] == expected_count

    def test_e2e_aggregate_date_time(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.aggregate({'Date': lambda x: x.min(), 'Settlement Date Time': 'max'})
        expected = {
            "columns": ["Date", "Settlement Date Time"],
            "rows": [{"values": ['2014-12-01', '2014-12-05T21:00:00.000000000+0000']}]
        }
        assert json.loads(frame.execute_frame_to_string())["result"] == expected
