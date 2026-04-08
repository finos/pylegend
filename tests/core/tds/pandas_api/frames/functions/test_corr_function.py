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


class TestCorrFunctionQueryGeneration:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_corr_groupby_single_column_sql_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.float_column("valA"),
            PrimitiveTdsColumn.float_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="grp").aggregate(
            {"valA": lambda c: c.row_mapper(c).corr()}
        )
        expected_sql = '''\
            SELECT
                "root".grp AS "grp",
                CORR("root".valA, "root".valA) AS "valA"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".grp
            ORDER BY
                "root".grp'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_corr_row_mapper_types(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.float_column("valA"),
            PrimitiveTdsColumn.float_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
        tds_row = PandasApiTdsRow.from_tds_frame("r", frame)
        val_a = tds_row["valA"]
        val_b = tds_row["valB"]
        pair = val_a.row_mapper(val_b)
        corr_result = pair.corr()

        from pylegend.core.language.shared.primitive_collection import PyLegendNumberPairCollection
        from pylegend.core.language import PyLegendFloat
        assert isinstance(pair, PyLegendNumberPairCollection)
        assert isinstance(corr_result, PyLegendFloat)

    def test_corr_non_groupby_aggregate_sql_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.float_column("valA"),
            PrimitiveTdsColumn.float_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.aggregate(
            {"valA": lambda c: c.row_mapper(c).corr()}
        )
        expected_sql = '''\
            SELECT
                CORR("root".valA, "root".valA) AS "valA"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_corr_groupby_pure_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.float_column("valA"),
            PrimitiveTdsColumn.float_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="grp").aggregate(
            {"valA": lambda c: c.row_mapper(c).corr()}
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[grp],
                ~[valA:{r | $r.valA}:{c | $c->corr($c)}]
              )
              ->sort([~grp->ascending()])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[grp], ~[valA:{r | $r.valA}:{c | $c->corr($c)}])'
            '->sort([~grp->ascending()])'
        )

    def test_corr_non_groupby_pure_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.float_column("valA"),
            PrimitiveTdsColumn.float_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.aggregate(
            {"valA": lambda c: c.row_mapper(c).corr()}
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->aggregate(~[valA:{r | $r.valA}:{c | $c->corr($c)}])"
        )

    def test_corr_sql_expression_rendering(self) -> None:
        """Test that CorrExpression renders correctly as SQL."""
        from pylegend.core.sql.metamodel_extension import CorrExpression
        from pylegend.core.sql.metamodel import StringLiteral
        from pylegend.core.database.sql_to_string.db_extension import SqlToStringDbExtension
        from pylegend.core.database.sql_to_string.config import SqlToStringConfig, SqlToStringFormat

        ext = SqlToStringDbExtension()
        expr = CorrExpression(
            value=StringLiteral(value="col1", quoted=False),
            other=StringLiteral(value="col2", quoted=False)
        )
        result = ext.process_corr_expression(expr, SqlToStringConfig(format_=SqlToStringFormat()))
        assert result == "CORR('col1', 'col2')"

    def test_number_pair_collection_with_literals(self) -> None:
        """Test that row_mapper works with Python literal numbers."""
        from pylegend.core.language import PyLegendFloat
        from pylegend.core.language.shared.literal_expressions import PyLegendFloatLiteralExpression
        val = PyLegendFloat(PyLegendFloatLiteralExpression(1.0))
        pair = val.row_mapper(2.0)
        corr_result = pair.corr()
        assert isinstance(corr_result, PyLegendFloat)

    def test_number_pair_collection_with_integer_collection(self) -> None:
        """Test that row_mapper works with integer collections."""
        from pylegend.core.language.shared.primitive_collection import (
            PyLegendIntegerCollection,
            PyLegendNumberPairCollection,
        )
        from pylegend.core.language import PyLegendInteger

        int_val = PyLegendInteger.__new__(PyLegendInteger)
        col_a = PyLegendIntegerCollection(int_val)
        col_b = PyLegendIntegerCollection(int_val)
        pair = col_a.row_mapper(col_b)
        assert isinstance(pair, PyLegendNumberPairCollection)

    def test_number_pair_collection_with_decimal_collection(self) -> None:
        """Test that row_mapper works with decimal collections."""
        from pylegend.core.language.shared.primitive_collection import (
            PyLegendDecimalCollection,
            PyLegendNumberPairCollection,
        )
        from pylegend.core.language import PyLegendDecimal

        dec_val = PyLegendDecimal.__new__(PyLegendDecimal)
        col_a = PyLegendDecimalCollection(dec_val)
        col_b = PyLegendDecimalCollection(dec_val)
        pair = col_a.row_mapper(col_b)
        assert isinstance(pair, PyLegendNumberPairCollection)

    def test_corr_window_sql_generation(self) -> None:
        """Test window corr: frame.groupby('id')['valA'].corr(frame.groupby('id')['valB'])"""
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        result = gb["valA"].corr(gb["valB"])
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
                        CORR("root".valA, "root".valB) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert assigned_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_corr_window_pure_generation(self) -> None:
        """Test window corr Pure generation."""
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        result = gb["valA"].corr(gb["valB"])
        assigned_frame = frame.assign(newCol=lambda _: result)
        assert generate_pure_query_and_compile(assigned_frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[id], []), ~valA__pylegend_olap_column__:{p,w,r | meta::pure::functions::math::mathUtility::rowMapper($r.valA, $r.valB)}:y | $y->meta::pure::functions::math::corr()->cast(@Float))
              ->project(~[id:c|$c.id, valA:c|$c.valA, valB:c|$c.valB, newCol:c|$c.valA__pylegend_olap_column__])'''
        )  # noqa: E501
        assert generate_pure_query_and_compile(assigned_frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[id], []), ~valA__pylegend_olap_column__:{p,w,r | meta::pure::functions::math::mathUtility::rowMapper($r.valA, $r.valB)}:y | $y->meta::pure::functions::math::corr()->cast(@Float))'
            '->project(~[id:c|$c.id, valA:c|$c.valA, valB:c|$c.valB, newCol:c|$c.valA__pylegend_olap_column__])'
        )

    def test_corr_window_self_correlation_sql(self) -> None:
        """Test window corr of column with itself."""
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        result = gb["valA"].corr(gb["valA"])
        assigned_frame = frame.assign(newCol=lambda _: result)
        expected_sql = '''\
            SELECT
                "root"."id" AS "id",
                "root"."valA" AS "valA",
                "root"."newCol__pylegend_olap_column__" AS "newCol"
            FROM
                (
                    SELECT
                        "root".id AS "id",
                        "root".valA AS "valA",
                        CORR("root".valA, "root".valA) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert assigned_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_corr_window_validate_missing_col_a(self) -> None:
        """Test that TwoColumnWindowFunction raises ValueError for missing column A."""
        from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        with pytest.raises(ValueError) as v:
            TwoColumnWindowFunction(
                base_frame=gb,
                col_name_a="missing_col",
                col_name_b="valA",
                result_col_name="newCol",
            )
        assert "missing_col" in v.value.args[0]
        assert "does not exist" in v.value.args[0]

    def test_corr_window_validate_missing_col_b(self) -> None:
        """Test that TwoColumnWindowFunction raises ValueError for missing column B."""
        from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        with pytest.raises(ValueError) as v:
            TwoColumnWindowFunction(
                base_frame=gb,
                col_name_a="valA",
                col_name_b="missing_col",
                result_col_name="newCol",
            )
        assert "missing_col" in v.value.args[0]
        assert "does not exist" in v.value.args[0]


class TestCorrFunctionEndToEnd:

    @pytest.mark.skip(reason="Legend engine SQL execution layer does not yet have a handler for the CORR function")  # pragma: no cover
    def test_e2e_corr_self_correlation_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """CORR of a column with itself should be 1.0 for groups with > 1 distinct row."""
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby("Product/Name").aggregate(
            {"Quantity": lambda c: c.row_mapper(c).corr()}
        )
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert "Quantity" in res["columns"]
        assert "Product/Name" in res["columns"]
        for row in res["rows"]:
            val = row["values"][res["columns"].index("Quantity")]
            if val is not None:
                assert val == 1.0

    @pytest.mark.skip(reason="Legend engine SQL execution layer does not yet have a handler for the CORR function")  # pragma: no cover
    def test_e2e_corr_two_columns(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """CORR of Quantity with itself across all rows."""
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.aggregate(
            {"Quantity": lambda c: c.row_mapper(c).corr()}
        )
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res["columns"] == ["Quantity"]
        assert res["rows"][0]["values"][0] == 1.0

