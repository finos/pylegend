# Copyright 2026 Goldman Sachs
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

import pytest
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import (
    simple_trade_service_frame_pandas_api,
)


class TestCovarPopulationFunctionQueryGeneration:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_covar_pop_groupby_aggregate_sql_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.float_column("valA"),
            PrimitiveTdsColumn.float_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="grp").aggregate(
            {"valA": lambda c: c.row_mapper(c).covar_population()}
        )
        expected_sql = '''\
            SELECT
                "root".grp AS "grp",
                COVAR_POP("root".valA, "root".valA) AS "valA"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".grp
            ORDER BY
                "root".grp'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_covar_pop_groupby_pure_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.float_column("valA"),
            PrimitiveTdsColumn.float_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="grp").aggregate(
            {"valA": lambda c: c.row_mapper(c).covar_population()}
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[grp], ~[valA:{r | $r.valA}:{c | $c->covarPopulation($c)}])'
            '->sort([~grp->ascending()])'
        )

    def test_covar_pop_window_sql_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        result = gb["valA"].cov(gb["valB"], ddof=0)
        assigned_frame = frame.assign(newCol=lambda r: result)
        expected_sql = '''\
            SELECT
                "root"."id" AS "id",
                "root"."valA" AS "valA",
                "root"."valB" AS "valB",
                "root"."newCol__pylegend_olap_column__" AS "newCol"
            FROM
                (
                    SELECT
                        "root".id AS "id",
                        "root".valA AS "valA",
                        "root".valB AS "valB",
                        COVAR_POP("root".valA, "root".valB) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert assigned_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_covar_pop_window_pure_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        result = gb["valA"].cov(gb["valB"], ddof=0)
        assigned_frame = frame.assign(newCol=lambda r: result)
        assert generate_pure_query_and_compile(assigned_frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[id], []), ~valA__pylegend_olap_column__:{p,w,r | meta::pure::functions::math::mathUtility::rowMapper($r.valA, $r.valB)}:y | $y->meta::pure::functions::math::covarPopulation()->cast(@Float))'
            '->project(~[id:c|$c.id, valA:c|$c.valA, valB:c|$c.valB, newCol:c|$c.valA__pylegend_olap_column__])'
        )

    def test_covar_pop_collection_method(self) -> None:
        from pylegend.core.language import PyLegendFloat
        from pylegend.core.language.shared.literal_expressions import PyLegendFloatLiteralExpression
        val = PyLegendFloat(PyLegendFloatLiteralExpression(1.0))
        pair = val.row_mapper(2.0)
        result = pair.covar_population()
        assert isinstance(result, PyLegendFloat)


class TestCovarSampleFunctionQueryGeneration:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_covar_samp_groupby_aggregate_sql_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.float_column("valA"),
            PrimitiveTdsColumn.float_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="grp").aggregate(
            {"valA": lambda c: c.row_mapper(c).covar_sample()}
        )
        expected_sql = '''\
            SELECT
                "root".grp AS "grp",
                COVAR_SAMP("root".valA, "root".valA) AS "valA"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".grp
            ORDER BY
                "root".grp'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_covar_samp_groupby_pure_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.float_column("valA"),
            PrimitiveTdsColumn.float_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="grp").aggregate(
            {"valA": lambda c: c.row_mapper(c).covar_sample()}
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[grp], ~[valA:{r | $r.valA}:{c | $c->covarSample($c)}])'
            '->sort([~grp->ascending()])'
        )

    def test_covar_samp_window_sql_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        result = gb["valA"].cov(gb["valB"])  # ddof=1 is default = covar_sample
        assigned_frame = frame.assign(newCol=lambda _: result)
        expected_sql = '''\
            SELECT
                "root"."id" AS "id",
                "root"."valA" AS "valA",
                "root"."valB" AS "valB",
                "root"."newCol__pylegend_olap_column__" AS "newCol"
            FROM
                (
                    SELECT
                        "root".id AS "id",
                        "root".valA AS "valA",
                        "root".valB AS "valB",
                        COVAR_SAMP("root".valA, "root".valB) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert assigned_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_covar_samp_window_pure_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        result = gb["valA"].cov(gb["valB"])  # ddof=1 = covar_sample
        assigned_frame = frame.assign(newCol=lambda _: result)
        assert generate_pure_query_and_compile(assigned_frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[id], []), ~valA__pylegend_olap_column__:{p,w,r | meta::pure::functions::math::mathUtility::rowMapper($r.valA, $r.valB)}:y | $y->meta::pure::functions::math::covarSample()->cast(@Float))'
            '->project(~[id:c|$c.id, valA:c|$c.valA, valB:c|$c.valB, newCol:c|$c.valA__pylegend_olap_column__])'
        )

    def test_covar_samp_collection_method(self) -> None:
        from pylegend.core.language import PyLegendFloat
        from pylegend.core.language.shared.literal_expressions import PyLegendFloatLiteralExpression
        val = PyLegendFloat(PyLegendFloatLiteralExpression(1.0))
        pair = val.row_mapper(2.0)
        result = pair.covar_sample()
        assert isinstance(result, PyLegendFloat)

    def test_cov_invalid_ddof(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        with pytest.raises(NotImplementedError) as exc:
            gb["valA"].cov(gb["valB"], ddof=2)
        assert "ddof=2" in str(exc.value)

    def test_corr_window_invalid_func_type(self) -> None:
        from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        with pytest.raises(ValueError) as exc:
            TwoColumnWindowFunction(
                base_frame=gb,
                col_name_a="valA",
                col_name_b="valA",
                result_col_name="newCol",
                func_type="invalid_type",
            )
        assert "invalid_type" in str(exc.value)


class TestCovarFunctionEndToEnd:

    @pytest.mark.skip(reason="Legend engine SQL execution layer does not yet have handlers for COVAR_POP/COVAR_SAMP")  # pragma: no cover
    def test_e2e_covar_pop_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby("Product/Name").aggregate(
            {"Quantity": lambda c: c.row_mapper(c).covar_population()}
        )
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert "Quantity" in res["columns"]

    @pytest.mark.skip(reason="Legend engine SQL execution layer does not yet have handlers for COVAR_POP/COVAR_SAMP")  # pragma: no cover
    def test_e2e_covar_samp_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby("Product/Name").aggregate(
            {"Quantity": lambda c: c.row_mapper(c).covar_sample()}
        )
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert "Quantity" in res["columns"]


class TestTwoColumnWindowFunctionInternals:
    """Tests for TwoColumnWindowFunction internal methods."""

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_two_column_window_function_name(self) -> None:
        """Test that TwoColumnWindowFunction.name() returns the expected value."""
        from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction
        assert TwoColumnWindowFunction.name() == "two_column_window"

    def test_two_column_window_function_get_window(self) -> None:
        """Test get_window() returns the PandasApiWindow."""
        from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction
        from pylegend.core.language.pandas_api.pandas_api_custom_expressions import PandasApiWindow

        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        func = TwoColumnWindowFunction(
            base_frame=gb,
            col_name_a="valA",
            col_name_b="valB",
            result_col_name="result",
            func_type="corr",
        )
        window = func.get_window()
        assert isinstance(window, PandasApiWindow)

    def test_two_column_window_function_get_expr(self) -> None:
        """Test get_expr() returns the PyLegendPrimitive."""
        from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction
        from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive

        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        func = TwoColumnWindowFunction(
            base_frame=gb,
            col_name_a="valA",
            col_name_b="valB",
            result_col_name="result",
            func_type="corr",
        )
        expr = func.get_expr()
        assert isinstance(expr, PyLegendPrimitive)

    def test_two_column_window_function_base_frame(self) -> None:
        """Test base_frame() returns the underlying TDS frame."""
        from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction
        from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        func = TwoColumnWindowFunction(
            base_frame=gb,
            col_name_a="valA",
            col_name_b="valB",
            result_col_name="result",
            func_type="corr",
        )
        base = func.base_frame()
        assert isinstance(base, PandasApiBaseTdsFrame)

    def test_two_column_window_function_tds_frame_parameters(self) -> None:
        """Test tds_frame_parameters() returns empty list."""
        from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction

        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        func = TwoColumnWindowFunction(
            base_frame=gb,
            col_name_a="valA",
            col_name_b="valB",
            result_col_name="result",
            func_type="corr",
        )
        params = func.tds_frame_parameters()
        assert params == []

    def test_two_column_window_function_to_sql(self) -> None:
        """Test to_sql() generates proper QuerySpecification."""
        from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction
        from pylegend.core.sql.metamodel import QuerySpecification

        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        func = TwoColumnWindowFunction(
            base_frame=gb,
            col_name_a="valA",
            col_name_b="valB",
            result_col_name="result",
            func_type="corr",
        )
        query = func.to_sql(FrameToSqlConfig())
        assert isinstance(query, QuerySpecification)

    def test_two_column_window_function_to_pure(self) -> None:
        """Test to_pure() generates proper Pure query string."""
        from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction

        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        func = TwoColumnWindowFunction(
            base_frame=gb,
            col_name_a="valA",
            col_name_b="valB",
            result_col_name="result",
            func_type="corr",
        )
        pure = func.to_pure(FrameToPureConfig())
        assert "extend" in pure
        assert "project" in pure
        assert "result__pylegend_olap_column__" in pure

