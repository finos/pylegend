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
    simple_trade_service_frame_pandas_api,
)


class TestGroupbyErrors:

    def test_groupby_error_invalid_level(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.string_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.groupby("col1", level=1)
        assert v.value.args[0] == (
            "The 'level' parameter of the groupby function is not supported yet. "
            "Please specify groupby column names using the 'by' parameter."
        )

    def test_groupby_error_as_index_true(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.groupby("col1", as_index=True)
        assert v.value.args[0] == (
            "The 'as_index' parameter of the groupby function must be False, " "but got: True (type: bool)"
        )

    def test_groupby_error_group_keys_true(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.groupby("col1", group_keys=True)
        assert v.value.args[0] == (
            "The 'group_keys' parameter of the groupby function must be False, " "but got: True (type: bool)"
        )

    def test_groupby_error_observed_true(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.groupby("col1", observed=True)
        assert v.value.args[0] == (
            "The 'observed' parameter of the groupby function must be False, " "but got: True (type: bool)"
        )

    def test_groupby_error_dropna_true(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.groupby("col1", dropna=True)
        assert v.value.args[0] == (
            "The 'dropna' parameter of the groupby function must be False, " "but got: True (type: bool)"
        )

    def test_groupby_error_invalid_by_type(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(TypeError) as v:
            frame.groupby(by=123)  # type: ignore
        assert v.value.args[0] == (
            "The 'by' parameter in groupby function must be a string or a list of strings." "but got: 123 (type: int)"
        )

    def test_groupby_error_empty_by_list(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(ValueError) as v:
            frame.groupby(by=[])
        assert v.value.args[0] == ("The 'by' parameter in groupby function must contain at least one column name.")

    def test_groupby_error_missing_column(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(KeyError) as v:
            frame.groupby(by=["col1", "missing_col"])
        assert v.value.args[0] == (
            "Column(s) ['missing_col'] in groupby function's provided columns list do not exist in the current frame. "
            "Current frame columns: ['col1']"
        )

    def test_groupby_getitem_error_invalid_type(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby("col1")
        with pytest.raises(TypeError) as v:
            gb[123]  # type: ignore
        assert v.value.args[0] == (
            "Column selection after groupby function must be a string or a list of strings, " "but got: 123 (type: int)"
        )

    def test_groupby_getitem_error_empty_list(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby("col1")
        with pytest.raises(ValueError) as v:
            gb[[]]
        assert v.value.args[0] == ("When performing column selection after groupby, at least one column must be selected.")

    def test_groupby_getitem_error_missing_column(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.integer_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby("col1")
        with pytest.raises(KeyError) as v:
            gb[["col2", "missing_col"]]
        assert v.value.args[0] == (
            "Column(s) ['missing_col'] selected after groupby do not exist in the current frame. "
            "Current frame columns: ['col1', 'col2']"
        )

    def test_groupby_convenience_error_numeric_only_true(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby("col1")
        gb_series = frame.groupby("col1")["col1"]

        methods = ["sum", "mean", "min", "max", "std", "var"]
        for method in methods:
            with pytest.raises(NotImplementedError) as v:
                getattr(gb, method)(numeric_only=True)
            assert f"numeric_only=True is not currently supported in {method} function" in v.value.args[0]

            with pytest.raises(NotImplementedError) as v:
                getattr(gb_series, method)(numeric_only=True)
            assert f"numeric_only=True is not currently supported in {method} function" in v.value.args[0]

    def test_groupby_convenience_error_engine_args(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby("col1")
        gb_series = frame.groupby("col1")["col1"]

        methods = ["sum", "mean", "min", "max", "std", "var"]
        for method in methods:
            with pytest.raises(NotImplementedError) as v1:
                getattr(gb, method)(engine="numba")
            assert f"engine parameter is not supported in {method} function" in v1.value.args[0]

            with pytest.raises(NotImplementedError) as v2:
                getattr(gb, method)(engine_kwargs={"nopython": True})
            assert f"engine_kwargs parameter is not supported in {method} function" in v2.value.args[0]

            with pytest.raises(NotImplementedError) as v1:
                getattr(gb_series, method)(engine="numba")
            assert f"engine parameter is not supported in {method} function" in v1.value.args[0]

            with pytest.raises(NotImplementedError) as v2:
                getattr(gb_series, method)(engine_kwargs={"nopython": True})
            assert f"engine_kwargs parameter is not supported in {method} function" in v2.value.args[0]

    def test_groupby_sum_error_min_count_nonzero(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby("col1")
        gb_series = frame.groupby("col1")["col1"]

        with pytest.raises(NotImplementedError) as v:
            gb.sum(min_count=5)
        assert "min_count must be 0 in sum function, but got: 5" in v.value.args[0]

        with pytest.raises(NotImplementedError) as v:
            gb_series.sum(min_count=5)
        assert "min_count must be 0 in sum function, but got: 5" in v.value.args[0]

    def test_groupby_min_max_error_min_count_not_minus_one(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby("col1")
        gb_series = frame.groupby("col1")["col1"]

        with pytest.raises(NotImplementedError) as v_min:
            gb.min(min_count=0)
        assert "min_count must be -1 (default) in min function, but got: 0" in v_min.value.args[0]

        with pytest.raises(NotImplementedError) as v_max:
            gb.max(min_count=5)
        assert "min_count must be -1 (default) in max function, but got: 5" in v_max.value.args[0]

        with pytest.raises(NotImplementedError) as v_min:
            gb_series.min(min_count=0)
        assert "min_count must be -1 (default) in min function, but got: 0" in v_min.value.args[0]

        with pytest.raises(NotImplementedError) as v_max:
            gb_series.max(min_count=5)
        assert "min_count must be -1 (default) in max function, but got: 5" in v_max.value.args[0]

    def test_groupby_std_error_ddof_not_one(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby("col1")
        gb_series = frame.groupby("col1")["col1"]

        with pytest.raises(NotImplementedError) as v:
            gb.std(ddof=0)
        assert "Only ddof=1 (Sample Standard Deviation) is supported in std function, but got: 0" in v.value.args[0]

        with pytest.raises(NotImplementedError) as v:
            gb_series.std(ddof=0)
        assert "Only ddof=1 (Sample Standard Deviation) is supported in std function, but got: 0" in v.value.args[0]

    def test_groupby_var_error_ddof_not_one(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby("col1")
        gb_series = frame.groupby("col1")["col1"]

        with pytest.raises(NotImplementedError) as v:
            gb.var(ddof=2)
        assert "Only ddof=1 (Sample Variance) is supported in var function, but got: 2" in v.value.args[0]

        with pytest.raises(NotImplementedError) as v:
            gb_series.var(ddof=2)
        assert "Only ddof=1 (Sample Variance) is supported in var function, but got: 2" in v.value.args[0]

    def test_groupby_invalid_column_selection_from_series(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2"),
            PrimitiveTdsColumn.integer_column("col3")
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as v:
            frame.groupby("col1")["col2"].agg({"col3": "count", "col2": "sum"})
        expected = '''\
            Invalid `func` argument for the aggregate function.
            When a dictionary is provided, all keys must be column names.
            Available columns are: ['col2']
            But got key: 'col3' (type: str)
        '''
        expected = dedent(expected)
        assert v.value.args[0] == expected

        with pytest.raises(ValueError) as v:
            frame.groupby("col1")["col2"].agg({"col1": "min"})
        expected = '''\
            Invalid `func` argument for the aggregate function.
            When a dictionary is provided, all keys must be column names.
            Available columns are: ['col2']
            But got key: 'col1' (type: str)
        '''
        expected = dedent(expected)
        assert v.value.args[0] == expected

    def test_groupby_series_without_aggregation(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb_series = frame.groupby("col1")["col1"]

        with pytest.raises(RuntimeError) as v:
            gb_series.to_sql_query_object(FrameToSqlConfig())

        assert v.value.args[0] == (
            "The 'groupby' function requires at least one operation to be performed right after it (e.g. aggregate, rank)"
        )


class TestGroupbyFunctionality:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_groupby_simple_query_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.date_column("col2"),
            PrimitiveTdsColumn.integer_column("col3"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="col1").aggregate({"col2": "min", "col3": [np.sum]})
        expected = """\
                    SELECT
                        "root".col1 AS "col1",
                        MIN("root".col2) AS "col2",
                        SUM("root".col3) AS "sum(col3)"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".col1
                    ORDER BY
                        "root".col1
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)[:-1]
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[col2:{r | $r.col2}:{c | $c->min()}, 'sum(col3)':{r | $r.col3}:{c | $c->sum()}]
              )
              ->sort([~col1->ascending()])"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->groupBy(~[col1], ~[col2:{r | $r.col2}:{c | $c->min()}, 'sum(col3)':{r | $r.col3}:{c | $c->sum()}])"
            "->sort([~col1->ascending()])"
        )

    def test_groupby_column_selection_for_aggregation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.date_column("col2"),
            PrimitiveTdsColumn.integer_column("col3"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby("col1")[["col2", "col3"]].aggregate({"col2": ["max"], "col3": [np.sum, np.mean]})
        expected = """\
                    SELECT
                        "root".col1 AS "col1",
                        MAX("root".col2) AS "max(col2)",
                        SUM("root".col3) AS "sum(col3)",
                        AVG("root".col3) AS "mean(col3)"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".col1
                    ORDER BY
                        "root".col1
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)[:-1]
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~['max(col2)':{r | $r.col2}:{c | $c->max()}, 'sum(col3)':{r | $r.col3}:{c | $c->sum()}, """
            """'mean(col3)':{r | $r.col3}:{c | $c->average()}]
              )
              ->sort([~col1->ascending()])"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->groupBy(~[col1], ~['max(col2)':{r | $r.col2}:{c | $c->max()}, 'sum(col3)':{r | $r.col3}:{c | $c->sum()},"
            " 'mean(col3)':{r | $r.col3}:{c | $c->average()}])->sort([~col1->ascending()])"
        )

    def test_groupby_multiple_grouping_columns(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("col1"),
            PrimitiveTdsColumn.strictdate_column("col2"),
            PrimitiveTdsColumn.integer_column("col3"),
            PrimitiveTdsColumn.datetime_column("col4"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(["col1", "col2"]).aggregate({"col3": "sum", "col4": ["min", "max"]})
        expected_sql = """\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                SUM("root".col3) AS "col3",
                MIN("root".col4) AS "min(col4)",
                MAX("root".col4) AS "max(col4)"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1,
                "root".col2
            ORDER BY
                "root".col1,
                "root".col2
            """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]

        expected_pure = dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1, col2],
                ~[col3:{r | $r.col3}:{c | $c->sum()}, 'min(col4)':{r | $r.col4}:{c | $c->min()}, """
            """'max(col4)':{r | $r.col4}:{c | $c->max()}]
              )
              ->sort([~col1->ascending(), ~col2->ascending()])"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=True), self.legend_client) == expected_pure

        expected_pure_pretty_false = (
            "#Table(test_schema.test_table)#"
            "->groupBy(~[col1, col2], ~[col3:{r | $r.col3}:{c | $c->sum()}, 'min(col4)':{r | $r.col4}:{c | $c->min()}, "
            "'max(col4)':{r | $r.col4}:{c | $c->max()}])->sort([~col1->ascending(), ~col2->ascending()])"
        )
        assert (
            generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client)
            == expected_pure_pretty_false
        )

    def test_groupby_broadcast_agg_func_string(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("col1"),
            PrimitiveTdsColumn.integer_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby("col1", sort=False).aggregate("sum")
        expected_sql = """\
            SELECT
                "root".col1 AS "col1",
                SUM("root".col2) AS "col2",
                SUM("root".col3) AS "col3"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1
            """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]

        expected_pure = dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[col2:{r | $r.col2}:{c | $c->sum()}, col3:{r | $r.col3}:{c | $c->sum()}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=True), self.legend_client) == expected_pure

        expected_pure_pretty_false = (
            "#Table(test_schema.test_table)#"
            "->groupBy(~[col1], ~[col2:{r | $r.col2}:{c | $c->sum()}, col3:{r | $r.col3}:{c | $c->sum()}])"
        )
        assert (
            generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client)
            == expected_pure_pretty_false
        )

    def test_groupby_broadcast_agg_func_list(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("col1"),
            PrimitiveTdsColumn.integer_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby("col1", sort=False).aggregate(["sum", "mean"])
        expected_sql = """\
            SELECT
                "root".col1 AS "col1",
                SUM("root".col2) AS "sum(col2)",
                AVG("root".col2) AS "mean(col2)",
                SUM("root".col3) AS "sum(col3)",
                AVG("root".col3) AS "mean(col3)"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1
            """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]

        expected_pure = dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~['sum(col2)':{r | $r.col2}:{c | $c->sum()}, 'mean(col2)':{r | $r.col2}:{c | $c->average()}, """
            """'sum(col3)':{r | $r.col3}:{c | $c->sum()}, 'mean(col3)':{r | $r.col3}:{c | $c->average()}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=True), self.legend_client) == expected_pure

        expected_pure_pretty_false = (
            "#Table(test_schema.test_table)#->groupBy(~[col1], "
            "~['sum(col2)':{r | $r.col2}:{c | $c->sum()}, 'mean(col2)':{r | $r.col2}:{c | $c->average()}, "
            "'sum(col3)':{r | $r.col3}:{c | $c->sum()}, 'mean(col3)':{r | $r.col3}:{c | $c->average()}])"
        )
        assert (
            generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client)
            == expected_pure_pretty_false
        )

    def test_groupby_aggregate_with_string_input(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2"),
            PrimitiveTdsColumn.integer_column("col3"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("col1", sort=False).aggregate("sum")

        expected_sql = """\
                    SELECT
                        "root".col1 AS "col1",
                        SUM("root".col2) AS "col2",
                        SUM("root".col3) AS "col3"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".col1
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]

        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[col2:{r | $r.col2}:{c | $c->sum()}, col3:{r | $r.col3}:{c | $c->sum()}]
              )"""
        )

    def test_groupby_aggregate_with_list_input(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.number_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("col1", sort=False)["col2"].aggregate(["min", "max"])

        expected_sql = """\
                    SELECT
                        "root".col1 AS "col1",
                        MIN("root".col2) AS "min(col2)",
                        MAX("root".col2) AS "max(col2)"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".col1
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]

        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~['min(col2)':{r | $r.col2}:{c | $c->min()}, 'max(col2)':{r | $r.col2}:{c | $c->max()}]
              )"""
        )

    def test_groupby_aggregate_with_mixed_dict_input(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("Region"),
            PrimitiveTdsColumn.integer_column("Sales"),
            PrimitiveTdsColumn.integer_column("Profit"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("Region", sort=False).aggregate({"Sales": ["sum", "mean"], "Profit": "max"})

        expected_sql = """\
                    SELECT
                        "root".Region AS "Region",
                        SUM("root".Sales) AS "sum(Sales)",
                        AVG("root".Sales) AS "mean(Sales)",
                        MAX("root".Profit) AS "Profit"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".Region
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]

        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[Region],
                ~['sum(Sales)':{r | $r.Sales}:{c | $c->sum()}, 'mean(Sales)':{r | $r.Sales}:{c | $c->average()}, """
            """Profit:{r | $r.Profit}:{c | $c->max()}]
              )"""
        )

    def test_groupby_aggregate_with_lambdas(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("id"), PrimitiveTdsColumn.float_column("val")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("id", sort=False).aggregate({"val": [lambda x: x.min(), lambda x: x.max()]})

        expected_sql = """\
                    SELECT
                        "root".id AS "id",
                        MIN("root".val) AS "lambda_1(val)",
                        MAX("root".val) AS "lambda_2(val)"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".id
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]

        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[id],
                ~['lambda_1(val)':{r | $r.val}:{c | $c->min()}, 'lambda_2(val)':{r | $r.val}:{c | $c->max()}]
              )"""
        )

    def test_groupby_multiple_grouping_keys(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("Category"),
            PrimitiveTdsColumn.string_column("SubCategory"),
            PrimitiveTdsColumn.integer_column("Amount"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby(["Category", "SubCategory"], sort=False).aggregate({"Amount": "sum"})

        expected_sql = """\
                    SELECT
                        "root".Category AS "Category",
                        "root".SubCategory AS "SubCategory",
                        SUM("root".Amount) AS "Amount"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".Category,
                        "root".SubCategory
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]

        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[Category, SubCategory],
                ~[Amount:{r | $r.Amount}:{c | $c->sum()}]
              )"""
        )

    def test_groupby_explicit_aggregation_of_grouping_key(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("Type"), PrimitiveTdsColumn.integer_column("Value")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("Type", sort=False).aggregate({"Type": "count", "Value": "sum"})

        expected_sql = """\
                    SELECT
                        "root".Type AS "Type",
                        COUNT("root".Type) AS "count(Type)",
                        SUM("root".Value) AS "Value"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".Type
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]

        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[Type],
                ~['count(Type)':{r | $r.Type}:{c | $c->count()}, Value:{r | $r.Value}:{c | $c->sum()}]
              )"""
        )

    def test_groupby_numpy_functions_integration(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("id"), PrimitiveTdsColumn.float_column("score")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("id", sort=False)["score"].agg([np.min, np.sum])

        expected_sql = """\
                    SELECT
                        "root".id AS "id",
                        MIN("root".score) AS "min(score)",
                        SUM("root".score) AS "sum(score)"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".id
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]

        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[id],
                ~['min(score)':{r | $r.score}:{c | $c->min()}, 'sum(score)':{r | $r.score}:{c | $c->sum()}]
              )"""
        )

    def test_groupby_convenience_methods_all(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.number_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        gb = frame.groupby("col1", sort=False)

        res = gb.sum()
        res_series = gb["col2"].sum()
        expected_sql = """\
            SELECT
                "root".col1 AS "col1",
                SUM("root".col2) AS "col2"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1"""
        assert res.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert res_series.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(res, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->groupBy(~[col1], ~[col2:{r | $r.col2}:{c | $c->sum()}])"
        ) == generate_pure_query_and_compile(res_series, FrameToPureConfig(pretty=False), self.legend_client)

        res = gb.mean()
        res_series = gb["col2"].mean()
        expected_sql = """\
            SELECT
                "root".col1 AS "col1",
                AVG("root".col2) AS "col2"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1"""
        assert res.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert res_series.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(res, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->groupBy(~[col1], ~[col2:{r | $r.col2}:{c | $c->average()}])"
        ) == generate_pure_query_and_compile(res_series, FrameToPureConfig(pretty=False), self.legend_client)

        res = gb.min()
        res_series = gb["col2"].min()
        expected_sql = """\
            SELECT
                "root".col1 AS "col1",
                MIN("root".col2) AS "col2"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1"""
        assert res.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert res_series.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(res, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->groupBy(~[col1], ~[col2:{r | $r.col2}:{c | $c->min()}])"
        ) == generate_pure_query_and_compile(res_series, FrameToPureConfig(pretty=False), self.legend_client)

        res = gb.max()
        res_series = gb["col2"].max()
        expected_sql = """\
            SELECT
                "root".col1 AS "col1",
                MAX("root".col2) AS "col2"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1"""
        assert res.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert res_series.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(res, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->groupBy(~[col1], ~[col2:{r | $r.col2}:{c | $c->max()}])"
        ) == generate_pure_query_and_compile(res_series, FrameToPureConfig(pretty=False), self.legend_client)

        res = gb.std()
        res_series = gb["col2"].std()
        expected_sql = """\
            SELECT
                "root".col1 AS "col1",
                STDDEV_SAMP("root".col2) AS "col2"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1"""
        assert res.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert res_series.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(res, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->groupBy(~[col1], ~[col2:{r | $r.col2}:{c | $c->stdDevSample()}])"
        ) == generate_pure_query_and_compile(res_series, FrameToPureConfig(pretty=False), self.legend_client)

        res = gb.var()
        res_series = gb["col2"].var()
        expected_sql = """\
            SELECT
                "root".col1 AS "col1",
                VAR_SAMP("root".col2) AS "col2"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1"""
        assert res.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert res_series.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(res, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->groupBy(~[col1], ~[col2:{r | $r.col2}:{c | $c->varianceSample()}])"
        ) == generate_pure_query_and_compile(res_series, FrameToPureConfig(pretty=False), self.legend_client)

        res = gb.count()
        res_series = gb["col2"].count()
        expected_sql = """\
            SELECT
                "root".col1 AS "col1",
                COUNT("root".col2) AS "col2"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1"""
        assert res.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert res_series.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(res, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->groupBy(~[col1], ~[col2:{r | $r.col2}:{c | $c->count()}])"
        ) == generate_pure_query_and_compile(res_series, FrameToPureConfig(pretty=False), self.legend_client)

    def test_groupby_single_selection(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.integer_column("col3")
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame1 = frame.groupby("col1")["col2"].max()
        frame2 = frame.groupby("col1")[["col2"]].max()

        expected = '''
            SELECT
                "root".col1 AS "col1",
                MAX("root".col2) AS "col2"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1
            ORDER BY
                "root".col1
        '''
        expected = dedent(expected).strip()
        assert frame1.to_sql_query(FrameToSqlConfig()) == expected
        assert frame2.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[col2:{r | $r.col2}:{c | $c->max()}]
              )
              ->sort([~col1->ascending()])
        '''
        expected = dedent(expected).strip()
        assert frame1.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame1, FrameToPureConfig(), self.legend_client) == expected
        assert frame2.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame2, FrameToPureConfig(), self.legend_client) == expected

    def test_groupby_self_selection(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.strictdate_column("col2"),
            PrimitiveTdsColumn.date_column("col3")
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame1 = frame.groupby("col1")["col1"].mean()
        frame2 = frame.groupby("col1")[["col1"]].mean()

        expected = '''
            SELECT
                "root".col1 AS "col1",
                AVG("root".col1) AS "mean(col1)"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1
            ORDER BY
                "root".col1
        '''
        expected = dedent(expected).strip()
        assert frame1.to_sql_query(FrameToSqlConfig()) == expected
        assert frame2.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~['mean(col1)':{r | $r.col1}:{c | $c->average()}]
              )
              ->sort([~col1->ascending()])
        '''
        expected = dedent(expected).strip()
        assert frame1.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame1, FrameToPureConfig(), self.legend_client) == expected
        assert frame2.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame2, FrameToPureConfig(), self.legend_client) == expected

        frame3 = frame.groupby(["col1", "col2"])["col2"].min()

        expected = '''
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                MIN("root".col2) AS "min(col2)"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1,
                "root".col2
            ORDER BY
                "root".col1,
                "root".col2
        '''
        expected = dedent(expected).strip()
        assert frame3.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1, col2],
                ~['min(col2)':{r | $r.col2}:{c | $c->min()}]
              )
              ->sort([~col1->ascending(), ~col2->ascending()])
        '''
        expected = dedent(expected).strip()
        assert frame3.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame3, FrameToPureConfig(), self.legend_client) == expected

        frame4 = frame.groupby(["col1", "col3"])["col3"].agg(["min", "max"])

        expected = '''
            SELECT
                "root".col1 AS "col1",
                "root".col3 AS "col3",
                MIN("root".col3) AS "min(col3)",
                MAX("root".col3) AS "max(col3)"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1,
                "root".col3
            ORDER BY
                "root".col1,
                "root".col3
        '''
        expected = dedent(expected).strip()
        assert frame4.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1, col3],
                ~['min(col3)':{r | $r.col3}:{c | $c->min()}, 'max(col3)':{r | $r.col3}:{c | $c->max()}]
              )
              ->sort([~col1->ascending(), ~col3->ascending()])
        '''
        expected = dedent(expected).strip()
        assert frame4.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame4, FrameToPureConfig(), self.legend_client) == expected

        frame5 = frame.groupby(["col1", "col2"])[["col1", "col3"]].agg({"col1": "sum", "col3": ["min", "max"]})

        expected = '''
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                SUM("root".col1) AS "sum(col1)",
                MIN("root".col3) AS "min(col3)",
                MAX("root".col3) AS "max(col3)"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1,
                "root".col2
            ORDER BY
                "root".col1,
                "root".col2
        '''
        expected = dedent(expected).strip()
        assert frame5.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1, col2],
                ~['sum(col1)':{r | $r.col1}:{c | $c->sum()}, 'min(col3)':{r | $r.col3}:{c | $c->min()}, 'max(col3)':{r | $r.col3}:{c | $c->max()}]
              )
              ->sort([~col1->ascending(), ~col2->ascending()])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame5.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame5, FrameToPureConfig(), self.legend_client) == expected


class TestGroupbyEndtoEnd:
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_e2e_groupby_simple_sum(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame_lambda_sum = frame.groupby("Firm/Legal Name").aggregate({"Age": lambda x: x.sum()})
        expected_sum = {
            "columns": ["Firm/Legal Name", "Age"],
            "rows": [
                {"values": ["Firm A", 34]},
                {"values": ["Firm B", 32]},
                {"values": ["Firm C", 35]},
                {"values": ["Firm X", 79]},
            ],
        }
        assert json.loads(frame_lambda_sum.execute_frame_to_string())["result"] == expected_sum

    def test_e2e_groupby_multiple_aggregations_list(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby("Firm/Legal Name").aggregate({"Age": ["min", "max"]})
        expected = {
            "columns": ["Firm/Legal Name", "min(Age)", "max(Age)"],
            "rows": [
                {"values": ["Firm A", 34, 34]},
                {"values": ["Firm B", 32, 32]},
                {"values": ["Firm C", 35, 35]},
                {"values": ["Firm X", 12, 23]},
            ],
        }
        assert json.loads(frame.execute_frame_to_string())["result"] == expected

    def test_e2e_groupby_multi_column_different_metrics(
        self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]
    ) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby("Firm/Legal Name").aggregate({"Age": "mean", "Last Name": "count"})
        expected = {
            "columns": ["Firm/Legal Name", "Age", "Last Name"],
            "rows": [
                {"values": ["Firm A", 34.0, 1]},
                {"values": ["Firm B", 32.0, 1]},
                {"values": ["Firm C", 35.0, 1]},
                {"values": ["Firm X", 19.75, 4]},
            ],
        }
        assert json.loads(frame.execute_frame_to_string())["result"] == expected

    def test_e2e_groupby_implicit_selection_broadcasting(
        self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]
    ) -> None:
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby("Product/Name")["Quantity"].aggregate(["sum"])

        expected = {
            "columns": ["Product/Name", "sum(Quantity)"],
            "rows": [
                {"values": ["Firm A", 66.0]},
                {"values": ["Firm C", 176.0]},
                {"values": ["Firm X", 345.0]},
                {"values": [None, 5.0]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        res["rows"].sort(key=lambda x: (x["values"][0] is None, x["values"][0]))
        assert res == expected

    def test_e2e_groupby_datetime_column(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby("Product/Name")["Settlement Date Time"].aggregate("min")

        expected = {
            "columns": ["Product/Name", "Settlement Date Time"],
            "rows": [
                {"values": ["Firm A", "2014-12-02T21:00:00.000000000+0000"]},
                {"values": ["Firm C", "2014-12-04T15:22:23.123456789+0000"]},
                {"values": ["Firm X", "2014-12-02T21:00:00.000000000+0000"]},
                {"values": [None, None]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        res["rows"].sort(key=lambda x: (x["values"][0] is None, x["values"][0]))
        assert res == expected

    def test_e2e_groupby_multi_keys(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby(["Firm/Legal Name", "First Name"]).aggregate({"Age": "sum"})

        expected = {
            "columns": ["Firm/Legal Name", "First Name", "Age"],
            "rows": [
                {"values": ["Firm A", "Fabrice", 34]},
                {"values": ["Firm B", "Oliver", 32]},
                {"values": ["Firm C", "David", 35]},
                {"values": ["Firm X", "Anthony", 22]},
                {"values": ["Firm X", "John", 34]},
                {"values": ["Firm X", "Peter", 23]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        res["rows"].sort(key=lambda x: (x["values"][0], x["values"][1]))
        assert res == expected

    def test_e2e_groupby_explicit_key_aggregation(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby("Firm/Legal Name").aggregate({"Firm/Legal Name": "count"})

        expected = {
            "columns": ["Firm/Legal Name", "count(Firm/Legal Name)"],
            "rows": [
                {"values": ["Firm A", 1]},
                {"values": ["Firm B", 1]},
                {"values": ["Firm C", 1]},
                {"values": ["Firm X", 4]},
            ],
        }
        assert json.loads(frame.execute_frame_to_string())["result"] == expected

    def test_e2e_groupby_numpy_functions(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby("Firm/Legal Name")["Age"].aggregate([np.min, np.max])

        expected = {
            "columns": ["Firm/Legal Name", "min(Age)", "max(Age)"],
            "rows": [
                {"values": ["Firm A", 34, 34]},
                {"values": ["Firm B", 32, 32]},
                {"values": ["Firm C", 35, 35]},
                {"values": ["Firm X", 12, 23]},
            ],
        }
        assert json.loads(frame.execute_frame_to_string())["result"] == expected
