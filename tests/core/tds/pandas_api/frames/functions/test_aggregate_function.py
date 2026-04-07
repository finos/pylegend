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

# type: ignore
# flake8: noqa

import json
from textwrap import dedent

import numpy as np
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
import pytest

from pylegend.core.language.pandas_api.pandas_api_series import Series
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunctionTdsFrame
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

    def test_aggregate_error_dict_value_list_invalid_content_type(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.aggregate({"col1": [123]})  # type: ignore

        expected_msg = (
            "Invalid `func` argument for the aggregate function.\n"
            "When a list is provided for a column, all elements must be callable, str, or np.ufunc.\n"
            "But got element at index 0: 123 (type: int)\n"
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
            "(or a list containing these).\n"
            "But got value for key 'col1': 123 (type: int)\n"
        )
        assert v.value.args[0] == expected_msg

    def test_aggregate_error_list_input_invalid_content_type(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.aggregate([123])  # type: ignore

        expected_msg = (
            "Invalid `func` argument for the aggregate function.\n"
            "When a list is provided as the main argument, all elements must be callable, str, or np.ufunc.\n"
            "But got element at index 0: 123 (type: int)\n"
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

    def test_convenience_methods_error_invalid_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        series = frame["col1"]

        methods = ['sum', 'mean', 'min', 'max', 'std', 'var', 'count']
        for method in methods:
            with pytest.raises(NotImplementedError) as v:
                getattr(frame, method)(axis=1)
            assert f"The 'axis' parameter must be 0 or 'index' in {method} function, but got: 1" in v.value.args[0]

            with pytest.raises(NotImplementedError) as v:
                getattr(series, method)(axis=1)
            assert f"The 'axis' parameter must be 0 or 'index' in {method} function, but got: 1" in v.value.args[0]

    def test_convenience_methods_error_skipna_false(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        series = frame["col1"]

        methods = ['sum', 'mean', 'min', 'max', 'std', 'var']
        for method in methods:
            with pytest.raises(NotImplementedError) as v:
                getattr(frame, method)(skipna=False)
            assert f"skipna=False is not currently supported in {method} function" in v.value.args[0]

            with pytest.raises(NotImplementedError) as v:
                getattr(series, method)(skipna=False)
            assert f"skipna=False is not currently supported in {method} function" in v.value.args[0]

    def test_convenience_methods_error_numeric_only_true(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        series = frame["col1"]

        methods = ['sum', 'mean', 'min', 'max', 'std', 'var', 'count']
        for method in methods:
            with pytest.raises(NotImplementedError) as v:
                getattr(frame, method)(numeric_only=True)
            assert f"numeric_only=True is not currently supported in {method} function" in v.value.args[0]

            with pytest.raises(NotImplementedError) as v:
                getattr(series, method)(numeric_only=True)
            assert f"numeric_only=True is not currently supported in {method} function" in v.value.args[0]

    def test_convenience_methods_error_extra_kwargs(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        series = frame["col1"]

        methods = ['sum', 'mean', 'min', 'max', 'std', 'var', 'count']
        for method in methods:
            with pytest.raises(NotImplementedError) as v:
                getattr(frame, method)(dummy_arg=1)
            assert f"Additional keyword arguments not supported in {method} function: ['dummy_arg']" in v.value.args[0]

            with pytest.raises(NotImplementedError) as v:
                getattr(series, method)(dummy_arg=1)
            assert f"Additional keyword arguments not supported in {method} function: ['dummy_arg']" in v.value.args[0]

    def test_sum_error_min_count_nonzero(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        series = frame["col1"]

        with pytest.raises(NotImplementedError) as v:
            frame.sum(min_count=5)
        assert "min_count must be 0 in sum function, but got: 5" in v.value.args[0]

        with pytest.raises(NotImplementedError) as v:
            series.sum(min_count=5)
        assert "min_count must be 0 in sum function, but got: 5" in v.value.args[0]

    def test_std_error_ddof_not_one(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        series = frame["col1"]

        with pytest.raises(NotImplementedError) as v:
            frame.std(ddof=0)
        assert "Only ddof=1 (Sample Standard Deviation) is supported in std function, but got: 0" in v.value.args[0]

        with pytest.raises(NotImplementedError) as v:
            series.std(ddof=0)
        assert "Only ddof=1 (Sample Standard Deviation) is supported in std function, but got: 0" in v.value.args[0]

    def test_var_error_ddof_not_one(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        series = frame["col1"]

        with pytest.raises(NotImplementedError) as v:
            frame.var(ddof=2)
        assert "Only ddof=1 (Sample Variance) is supported in var function, but got: 2" in v.value.args[0]

        with pytest.raises(NotImplementedError) as v:
            series.var(ddof=2)
        assert "Only ddof=1 (Sample Variance) is supported in var function, but got: 2" in v.value.args[0]

    def test_aggregate_simple_query_generation(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.date_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.aggregate({'col1': ['min'], 'col2': ['count']})
        expected = """\
                    SELECT
                        MIN("root".col1) AS "min(col1)",
                        COUNT("root".col2) AS "count(col2)"
                    FROM
                        test_schema.test_table AS "root"
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)[:-1]
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->aggregate(
                ~['min(col1)':{r | $r.col1}:{c | $c->min()}, 'count(col2)':{r | $r.col2}:{c | $c->count()}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->aggregate(~['min(col1)':{r | $r.col1}:{c | $c->min()}, 'count(col2)':{r | $r.col2}:{c | $c->count()}])"
        )

    def test_aggregate_for_bool_and_datetime_column(self) -> None:
        columns = [PrimitiveTdsColumn.boolean_column("col1"), PrimitiveTdsColumn.datetime_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.aggregate({'col1': ['count'], 'col2': ['min']})
        expected = """\
                    SELECT
                        COUNT("root".col1) AS "count(col1)",
                        MIN("root".col2) AS "min(col2)"
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
                        VAR_SAMP("root".col3) AS "var(col3)",
                        STDDEV_SAMP("root".col2) AS "std(col2)"
                    FROM
                        test_schema.test_table AS "root"
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)[:-1]
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->aggregate(
                ~['var(col3)':{r | $r.col3}:{c | $c->varianceSample()->cast(@Float)}, 'std(col2)':{r | $r.col2}:{c | $c->stdDevSample()->cast(@Float)}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->aggregate(~['var(col3)':{r | $r.col3}:{c | $c->varianceSample()->cast(@Float)}, "
            "'std(col2)':{r | $r.col2}:{c | $c->stdDevSample()->cast(@Float)}])"
        )

    def test_aggregate_repeat_column(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1"),
                   PrimitiveTdsColumn.number_column("col2"),
                   PrimitiveTdsColumn.float_column("col3")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.aggregate({'col3': ['var'], 'col3': ['std']})  # noqa
        expected = """\
                    SELECT
                        STDDEV_SAMP("root".col3) AS "std(col3)"
                    FROM
                        test_schema.test_table AS "root"
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)[:-1]
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->aggregate(
                ~['std(col3)':{r | $r.col3}:{c | $c->stdDevSample()->cast(@Float)}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->aggregate(~['std(col3)':{r | $r.col3}:{c | $c->stdDevSample()->cast(@Float)}])"
        )

    def test_aggregate_subquery_generation(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.integer_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.truncate(5, 10).aggregate({'col1': ['min'], 'col2': ['count']})
        expected = """\
                    SELECT
                        MIN("root"."col1") AS "min(col1)",
                        COUNT("root"."col2") AS "count(col2)"
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
                ~['min(col1)':{r | $r.col1}:{c | $c->min()}, 'count(col2)':{r | $r.col2}:{c | $c->count()}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->slice(5, 11)"
            "->aggregate(~['min(col1)':{r | $r.col1}:{c | $c->min()}, 'count(col2)':{r | $r.col2}:{c | $c->count()}])"
        )

    def test_aggregate_convenience_methods_all(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        res = frame.sum()
        res_series = frame["col1"].sum()
        expected_sql = """\
            SELECT
                SUM("root".col1) AS "col1"
            FROM
                test_schema.test_table AS "root\""""
        assert res.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert res_series.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(res, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->aggregate(~[col1:{r | $r.col1}:{c | $c->sum()}])"
        )
        assert generate_pure_query_and_compile(res_series, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->select(~[col1])->aggregate(~[col1:{r | $r.col1}:{c | $c->sum()}])"
        )

        res = frame.mean()
        res_series = frame["col1"].mean()
        expected_sql = """\
            SELECT
                AVG("root".col1) AS "col1"
            FROM
                test_schema.test_table AS "root\""""
        assert res.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert res_series.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(res, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->aggregate(~[col1:{r | $r.col1}:{c | $c->average()}])"
        )
        assert generate_pure_query_and_compile(res_series, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->select(~[col1])->aggregate(~[col1:{r | $r.col1}:{c | $c->average()}])"
        )

        res = frame.min()
        res_series = frame["col1"].min()
        expected_sql = """\
            SELECT
                MIN("root".col1) AS "col1"
            FROM
                test_schema.test_table AS "root\""""
        assert res.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert res_series.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(res, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->aggregate(~[col1:{r | $r.col1}:{c | $c->min()}])"
        )
        assert generate_pure_query_and_compile(res_series, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->select(~[col1])->aggregate(~[col1:{r | $r.col1}:{c | $c->min()}])"
        )

        res = frame.max()
        res_series = frame["col1"].max()
        expected_sql = """\
            SELECT
                MAX("root".col1) AS "col1"
            FROM
                test_schema.test_table AS "root\""""
        assert res.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert res_series.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(res, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->aggregate(~[col1:{r | $r.col1}:{c | $c->max()}])"
        )
        assert generate_pure_query_and_compile(res_series, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->select(~[col1])->aggregate(~[col1:{r | $r.col1}:{c | $c->max()}])"
        )

        res = frame.std()
        res_series = frame["col1"].std()
        expected_sql = """\
            SELECT
                STDDEV_SAMP("root".col1) AS "col1"
            FROM
                test_schema.test_table AS "root\""""
        assert res.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert res_series.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(res, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->aggregate(~[col1:{r | $r.col1}:{c | $c->stdDevSample()->cast(@Float)}])"
        )
        assert generate_pure_query_and_compile(res_series, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->select(~[col1])->aggregate(~[col1:{r | $r.col1}:{c | $c->stdDevSample()->cast(@Float)}])"
        )

        res = frame.var()
        res_series = frame["col1"].var()
        expected_sql = """\
            SELECT
                VAR_SAMP("root".col1) AS "col1"
            FROM
                test_schema.test_table AS "root\""""
        assert res.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert res_series.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(res, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->aggregate(~[col1:{r | $r.col1}:{c | $c->varianceSample()->cast(@Float)}])"
        )
        assert generate_pure_query_and_compile(res_series, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->select(~[col1])->aggregate(~[col1:{r | $r.col1}:{c | $c->varianceSample()->cast(@Float)}])"
        )

        res = frame.count()
        res_series = frame["col1"].count()
        expected_sql = """\
            SELECT
                COUNT("root".col1) AS "col1"
            FROM
                test_schema.test_table AS "root\""""
        assert res.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert res_series.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(res, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->aggregate(~[col1:{r | $r.col1}:{c | $c->count()}])"
        )
        assert generate_pure_query_and_compile(res_series, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->select(~[col1])->aggregate(~[col1:{r | $r.col1}:{c | $c->count()}])"
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
            "columns": ["sum(Age)"],
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
            "columns": ["maximum(First Name)", "maximum(Last Name)", "maximum(Age)", "maximum(Firm/Legal Name)"],
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


class TestAggregateFunctionOnSeries:

    def test_error_for_invalid_column_on_series(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("int_col"),
            PrimitiveTdsColumn.string_column("str_col"),
            PrimitiveTdsColumn.date_column("date_col")
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as v:
            frame["int_col"].agg({"int_col": "sum", "str_col": "count"})
        expected = '''\
            Invalid `func` argument for the aggregate function.
            When a dictionary is provided, all keys must be column names.
            Available columns are: ['int_col']
            But got key: 'str_col' (type: str)
        '''
        expected = dedent(expected)
        assert v.value.args[0] == expected

        with pytest.raises(ValueError) as v:
            frame["int_col"].agg({"date_col": "count"})
        expected = '''\
            Invalid `func` argument for the aggregate function.
            When a dictionary is provided, all keys must be column names.
            Available columns are: ['int_col']
            But got key: 'date_col' (type: str)
        '''
        expected = dedent(expected)
        assert v.value.args[0] == expected

    def test_single_aggregation_on_series(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("int_col"),
            PrimitiveTdsColumn.string_column("str_col"),
            PrimitiveTdsColumn.date_column("date_col")
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame1 = frame["int_col"].sum()
        frame2 = frame["int_col"].agg({"int_col": "sum"})

        expected = '''
            SELECT
                SUM("root".int_col) AS "int_col"
            FROM
                test_schema.test_table AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame1.to_sql_query(FrameToSqlConfig()) == expected
        assert frame2.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->select(~[int_col])
              ->aggregate(
                ~[int_col:{r | $r.int_col}:{c | $c->sum()}]
              )
        '''
        expected = dedent(expected).strip()
        assert frame1.to_pure_query(FrameToPureConfig()) == expected
        assert frame2.to_pure_query(FrameToPureConfig()) == expected

    def test_multiple_aggregations_on_series(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("int_col"),
            PrimitiveTdsColumn.string_column("str_col"),
            PrimitiveTdsColumn.date_column("date_col")
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame1 = frame["int_col"].agg(["mean", "min", "var"])
        frame2 = frame["int_col"].agg({"int_col": ["mean", "min", "var"]})

        assert isinstance(frame1, PandasApiAppliedFunctionTdsFrame)

        expected = '''
            SELECT
                AVG("root".int_col) AS "mean(int_col)",
                MIN("root".int_col) AS "min(int_col)",
                VAR_SAMP("root".int_col) AS "var(int_col)"
            FROM
                test_schema.test_table AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame1.to_sql_query(FrameToSqlConfig()) == expected
        assert frame2.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->select(~[int_col])
              ->aggregate(
                ~['mean(int_col)':{r | $r.int_col}:{c | $c->average()}, 'min(int_col)':{r | $r.int_col}:{c | $c->min()}, 'var(int_col)':{r | $r.int_col}:{c | $c->varianceSample()->cast(@Float)}]
              )
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame1.to_pure_query(FrameToPureConfig()) == expected
        assert frame2.to_pure_query(FrameToPureConfig()) == expected


class TestAggregateFunctionAssignment:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_simple_assignment(self) -> None:
        """Basic: assign an aggregated series to a new column; verify isinstance, SQL, and Pure."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1_int"),
            PrimitiveTdsColumn.string_column("col2_str"),
            PrimitiveTdsColumn.date_column("col3_date")
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        aggregated_series = frame["col1_int"].sum()

        assert isinstance(aggregated_series, Series)
        frame["col1_sum"] = aggregated_series
        expected_sql = dedent('''
            SELECT
                "root"."col1_int" AS "col1_int",
                "root"."col2_str" AS "col2_str",
                "root"."col3_date" AS "col3_date",
                "root"."col1_sum__pylegend_olap_column__" AS "col1_sum"
            FROM
                (
                    SELECT
                        "root"."col1_int" AS "col1_int",
                        "root"."col2_str" AS "col2_str",
                        "root"."col3_date" AS "col3_date",
                        SUM("root"."col1_int") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1_int" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col1_sum__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1_int AS "col1_int",
                                "root".col2_str AS "col2_str",
                                "root".col3_date AS "col3_date",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        ''').strip()  # noqa: E501
        assert frame.to_sql_query() == expected_sql

        expected_pure_frame = dedent('''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1_int)], rows(unbounded(), unbounded())), ~col1_int__pylegend_olap_column__:{p,w,r | $r.col1_int}:{c | $c->sum()})
              ->project(~[col1_int:c|$c.col1_int, col2_str:c|$c.col2_str, col3_date:c|$c.col3_date, col1_sum:c|$c.col1_int__pylegend_olap_column__])
        ''').strip()  # noqa: E501
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure_frame

    def test_assign_multiple_aggregates_and_overwrite(self) -> None:
        """Multiple aggregate assignments including overwriting an existing column."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame["col1_sum"] = frame["col1"].sum()
        frame["col2_mean"] = frame["col2"].mean()
        frame["col3_count"] = frame["col3"].count()

        expected_pure = dedent('''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), unbounded())), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, col3:c|$c.col3, col1_sum:c|$c.col1__pylegend_olap_column__])
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col2)], rows(unbounded(), unbounded())), ~col2__pylegend_olap_column__:{p,w,r | $r.col2}:{c | $c->average()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, col3:c|$c.col3, col1_sum:c|$c.col1_sum, col2_mean:c|$c.col2__pylegend_olap_column__])
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col3)], rows(unbounded(), unbounded())), ~col3__pylegend_olap_column__:{p,w,r | $r.col3}:{c | $c->count()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, col3:c|$c.col3, col1_sum:c|$c.col1_sum, col2_mean:c|$c.col2_mean, col3_count:c|$c.col3__pylegend_olap_column__])
        ''').strip()  # noqa: E501
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure
        col_names = [c.get_name() for c in frame.columns()]
        assert col_names == ["col1", "col2", "col3", "col1_sum", "col2_mean", "col3_count"]

        frame["col1"] = frame["col1"].max()

        expected_pure_overwrite = dedent('''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), unbounded())), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, col3:c|$c.col3, col1_sum:c|$c.col1__pylegend_olap_column__])
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col2)], rows(unbounded(), unbounded())), ~col2__pylegend_olap_column__:{p,w,r | $r.col2}:{c | $c->average()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, col3:c|$c.col3, col1_sum:c|$c.col1_sum, col2_mean:c|$c.col2__pylegend_olap_column__])
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col3)], rows(unbounded(), unbounded())), ~col3__pylegend_olap_column__:{p,w,r | $r.col3}:{c | $c->count()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, col3:c|$c.col3, col1_sum:c|$c.col1_sum, col2_mean:c|$c.col2_mean, col3_count:c|$c.col3__pylegend_olap_column__])
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), unbounded())), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->max()})
              ->project(~[col1:c|$c.col1__pylegend_olap_column__, col2:c|$c.col2, col3:c|$c.col3, col1_sum:c|$c.col1_sum, col2_mean:c|$c.col2_mean, col3_count:c|$c.col3_count])
        ''').strip()  # noqa: E501
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure_overwrite

        expected_sql = dedent('''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2" AS "col2",
                "root"."col3" AS "col3",
                "root"."col1_sum" AS "col1_sum",
                "root"."col2_mean" AS "col2_mean",
                "root"."col3_count" AS "col3_count"
            FROM
                (
                    SELECT
                        MAX("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col1__pylegend_olap_column__",
                        "root"."col2" AS "col2",
                        "root"."col3" AS "col3",
                        "root"."col1_sum" AS "col1_sum",
                        "root"."col2_mean" AS "col2_mean",
                        "root"."col3_count" AS "col3_count"
                    FROM
                        (
                            SELECT
                                "root"."col1" AS "col1",
                                "root"."col2" AS "col2",
                                "root"."col3" AS "col3",
                                "root"."col1_sum" AS "col1_sum",
                                "root"."col2_mean" AS "col2_mean",
                                "root"."col3_count__pylegend_olap_column__" AS "col3_count",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                (
                                    SELECT
                                        "root"."col1" AS "col1",
                                        "root"."col2" AS "col2",
                                        "root"."col3" AS "col3",
                                        "root"."col1_sum" AS "col1_sum",
                                        "root"."col2_mean" AS "col2_mean",
                                        COUNT("root"."col3") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col3" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col3_count__pylegend_olap_column__"
                                    FROM
                                        (
                                            SELECT
                                                "root"."col1" AS "col1",
                                                "root"."col2" AS "col2",
                                                "root"."col3" AS "col3",
                                                "root"."col1_sum" AS "col1_sum",
                                                "root"."col2_mean__pylegend_olap_column__" AS "col2_mean",
                                                0 AS "__pylegend_zero_column__"
                                            FROM
                                                (
                                                    SELECT
                                                        "root"."col1" AS "col1",
                                                        "root"."col2" AS "col2",
                                                        "root"."col3" AS "col3",
                                                        "root"."col1_sum" AS "col1_sum",
                                                        AVG("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col2" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col2_mean__pylegend_olap_column__"
                                                    FROM
                                                        (
                                                            SELECT
                                                                "root"."col1" AS "col1",
                                                                "root"."col2" AS "col2",
                                                                "root"."col3" AS "col3",
                                                                "root"."col1_sum__pylegend_olap_column__" AS "col1_sum",
                                                                0 AS "__pylegend_zero_column__"
                                                            FROM
                                                                (
                                                                    SELECT
                                                                        "root"."col1" AS "col1",
                                                                        "root"."col2" AS "col2",
                                                                        "root"."col3" AS "col3",
                                                                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col1_sum__pylegend_olap_column__"
                                                                    FROM
                                                                        (
                                                                            SELECT
                                                                                "root".col1 AS "col1",
                                                                                "root".col2 AS "col2",
                                                                                "root".col3 AS "col3",
                                                                                0 AS "__pylegend_zero_column__"
                                                                            FROM
                                                                                test_schema.test_table AS "root"
                                                                        ) AS "root"
                                                                ) AS "root"
                                                        ) AS "root"
                                                ) AS "root"
                                        ) AS "root"
                                ) AS "root"
                        ) AS "root"
                ) AS "root"
        ''').strip()  # noqa: E501
        assert frame.to_sql_query() == expected_sql

    def test_assign_aggregate_after_truncate(self) -> None:
        """Assignment on a truncated frame wraps the base in a sub-query."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.truncate(0, 10)
        frame["col1_sum"] = frame["col1"].sum()

        expected_sql = dedent('''
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."col1_sum__pylegend_olap_column__" AS "col1_sum"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col1_sum__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                            LIMIT 11
                            OFFSET 0
                        ) AS "root"
                ) AS "root"
        ''').strip()  # noqa: E501
        assert frame.to_sql_query() == expected_sql

        expected_pure = dedent('''
            #Table(test_schema.test_table)#
              ->slice(0, 11)
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), unbounded())), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, col1_sum:c|$c.col1__pylegend_olap_column__])
        ''').strip()  # noqa: E501
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_assign_aggregate_with_arithmetic(self) -> None:
        """Aggregate result combined with arithmetic. Aggregate in inner query, arithmetic in outer."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame["col1_sum_plus"] = frame["col1"].sum() + 42

        expected_sql = dedent('''
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                ("root"."col1_sum_plus__pylegend_olap_column__" + 42) AS "col1_sum_plus"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col1_sum_plus__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        ''').strip()  # noqa: E501
        assert frame.to_sql_query() == expected_sql

        expected_pure = dedent('''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), unbounded())), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, col1_sum_plus:c|(toOne($c.col1__pylegend_olap_column__) + 42)])
        ''').strip()  # noqa: E501
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_assign_aggregate_arithmetic_multiple_ops(self) -> None:
        """Multiple arithmetic operations on aggregated values."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame["col1_shifted"] = frame["col1"].sum() - 10
        frame["col2_scaled"] = frame["col2"].mean() * 2

        expected_sql = dedent('''
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."col1_shifted" AS "col1_shifted",
                ("root"."col2_scaled__pylegend_olap_column__" * 2) AS "col2_scaled"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        "root"."col1_shifted" AS "col1_shifted",
                        AVG("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col2" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col2_scaled__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root"."col1" AS "col1",
                                "root"."col2" AS "col2",
                                ("root"."col1_shifted__pylegend_olap_column__" - 10) AS "col1_shifted",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                (
                                    SELECT
                                        "root"."col1" AS "col1",
                                        "root"."col2" AS "col2",
                                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col1_shifted__pylegend_olap_column__"
                                    FROM
                                        (
                                            SELECT
                                                "root".col1 AS "col1",
                                                "root".col2 AS "col2",
                                                0 AS "__pylegend_zero_column__"
                                            FROM
                                                test_schema.test_table AS "root"
                                        ) AS "root"
                                ) AS "root"
                        ) AS "root"
                ) AS "root"
        ''').strip()  # noqa: E501
        assert frame.to_sql_query() == expected_sql

        expected_pure = dedent('''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), unbounded())), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, col1_shifted:c|(toOne($c.col1__pylegend_olap_column__) - 10)])
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col2)], rows(unbounded(), unbounded())), ~col2__pylegend_olap_column__:{p,w,r | $r.col2}:{c | $c->average()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, col1_shifted:c|$c.col1_shifted, col2_scaled:c|(toOne($c.col2__pylegend_olap_column__) * 2)])
        ''').strip()  # noqa: E501
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_chaining_after_aggregate_assignment(self) -> None:
        """Operations like filter/drop work correctly on a frame with an assigned aggregate."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame["col1_sum"] = frame["col1"].sum()
        filtered = frame.filter(items=["col1", "col1_sum"])
        assert [c.get_name() for c in filtered.columns()] == ["col1", "col1_sum"]
        dropped = frame.drop(columns=["col2"])
        assert [c.get_name() for c in dropped.columns()] == ["col1", "col1_sum"]

    def test_multiple_aggregates_on_same_column(self) -> None:
        """Different aggregates on the same source column each get their own output column."""
        columns = [
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.string_column("label"),
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame["val_sum"] = frame["val"].sum()
        frame["val_min"] = frame["val"].min()
        frame["val_max"] = frame["val"].max()

        expected_sql = dedent('''
            SELECT
                "root"."val" AS "val",
                "root"."label" AS "label",
                "root"."val_sum" AS "val_sum",
                "root"."val_min" AS "val_min",
                "root"."val_max__pylegend_olap_column__" AS "val_max"
            FROM
                (
                    SELECT
                        "root"."val" AS "val",
                        "root"."label" AS "label",
                        "root"."val_sum" AS "val_sum",
                        "root"."val_min" AS "val_min",
                        MAX("root"."val") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."val" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "val_max__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root"."val" AS "val",
                                "root"."label" AS "label",
                                "root"."val_sum" AS "val_sum",
                                "root"."val_min__pylegend_olap_column__" AS "val_min",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                (
                                    SELECT
                                        "root"."val" AS "val",
                                        "root"."label" AS "label",
                                        "root"."val_sum" AS "val_sum",
                                        MIN("root"."val") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."val" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "val_min__pylegend_olap_column__"
                                    FROM
                                        (
                                            SELECT
                                                "root"."val" AS "val",
                                                "root"."label" AS "label",
                                                "root"."val_sum__pylegend_olap_column__" AS "val_sum",
                                                0 AS "__pylegend_zero_column__"
                                            FROM
                                                (
                                                    SELECT
                                                        "root"."val" AS "val",
                                                        "root"."label" AS "label",
                                                        SUM("root"."val") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."val" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "val_sum__pylegend_olap_column__"
                                                    FROM
                                                        (
                                                            SELECT
                                                                "root".val AS "val",
                                                                "root".label AS "label",
                                                                0 AS "__pylegend_zero_column__"
                                                            FROM
                                                                test_schema.test_table AS "root"
                                                        ) AS "root"
                                                ) AS "root"
                                        ) AS "root"
                                ) AS "root"
                        ) AS "root"
                ) AS "root"
        ''').strip()  # noqa: E501
        assert frame.to_sql_query() == expected_sql

        expected_pure = dedent('''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~val)], rows(unbounded(), unbounded())), ~val__pylegend_olap_column__:{p,w,r | $r.val}:{c | $c->sum()})
              ->project(~[val:c|$c.val, label:c|$c.label, val_sum:c|$c.val__pylegend_olap_column__])
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~val)], rows(unbounded(), unbounded())), ~val__pylegend_olap_column__:{p,w,r | $r.val}:{c | $c->min()})
              ->project(~[val:c|$c.val, label:c|$c.label, val_sum:c|$c.val_sum, val_min:c|$c.val__pylegend_olap_column__])
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~val)], rows(unbounded(), unbounded())), ~val__pylegend_olap_column__:{p,w,r | $r.val}:{c | $c->max()})
              ->project(~[val:c|$c.val, label:c|$c.label, val_sum:c|$c.val_sum, val_min:c|$c.val_min, val_max:c|$c.val__pylegend_olap_column__])
        ''').strip()  # noqa: E501
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure
