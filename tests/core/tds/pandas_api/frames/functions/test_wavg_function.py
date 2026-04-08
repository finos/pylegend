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

from textwrap import dedent

import json
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


class TestWavgFunctionQueryGeneration:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_wavg_groupby_aggregate_sql_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.float_column("quantity"),
            PrimitiveTdsColumn.float_column("weight"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="grp").aggregate(
            {"quantity": lambda c: c.row_mapper(c).wavg()}
        )
        expected_sql = '''\
            SELECT
                "root".grp AS "grp",
                (SUM("root".quantity * "root".quantity) / SUM("root".quantity)) AS "quantity"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".grp
            ORDER BY
                "root".grp'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_wavg_groupby_pure_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.float_column("quantity"),
            PrimitiveTdsColumn.float_column("weight"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="grp").aggregate(
            {"quantity": lambda c: c.row_mapper(c).wavg()}
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[grp], ~[quantity:{r | $r.quantity}:{c | $c->wavg($c)}])'
            '->sort([~grp->ascending()])'
        )

    def test_wavg_non_groupby_aggregate_sql_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.float_column("quantity"),
            PrimitiveTdsColumn.float_column("weight"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.aggregate(
            {"quantity": lambda c: c.row_mapper(c).wavg()}
        )
        expected_sql = '''\
            SELECT
                (SUM("root".quantity * "root".quantity) / SUM("root".quantity)) AS "quantity"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_wavg_collection_method(self) -> None:
        from pylegend.core.language import PyLegendFloat
        from pylegend.core.language.shared.literal_expressions import PyLegendFloatLiteralExpression
        val = PyLegendFloat(PyLegendFloatLiteralExpression(1.0))
        pair = val.row_mapper(2.0)
        result = pair.wavg()
        assert isinstance(result, PyLegendFloat)

    def test_wavg_window_sql_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("quantity"),
            PrimitiveTdsColumn.integer_column("weight"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        frame["newCol"] = gb["quantity"].wavg(gb["weight"])
        expected_sql = '''\
            SELECT
                "root"."id" AS "id",
                "root"."quantity" AS "quantity",
                "root"."weight" AS "weight",
                "root"."newCol__pylegend_olap_column__" AS "newCol"
            FROM
                (
                    SELECT
                        "root".id AS "id",
                        "root".quantity AS "quantity",
                        "root".weight AS "weight",
                        (SUM("root".quantity * "root".weight) / SUM("root".weight)) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_wavg_window_pure_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("quantity"),
            PrimitiveTdsColumn.integer_column("weight"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        frame["newCol"] = gb["quantity"].wavg(gb["weight"])
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[id], []), ~quantity__pylegend_olap_column__:{p,w,r | meta::pure::functions::math::mathUtility::rowMapper($r.quantity, $r.weight)}:y | $y->meta::pure::functions::math::wavg()->cast(@Float))'
            '->project(~[id:c|$c.id, quantity:c|$c.quantity, weight:c|$c.weight, newCol:c|$c.quantity__pylegend_olap_column__])'
        )

    def test_wavg_sql_expression_rendering(self) -> None:
        from pylegend.core.sql.metamodel_extension import WavgExpression
        from pylegend.core.sql.metamodel import StringLiteral
        from pylegend.core.database.sql_to_string.db_extension import SqlToStringDbExtension
        from pylegend.core.database.sql_to_string.config import SqlToStringConfig, SqlToStringFormat

        ext = SqlToStringDbExtension()
        expr = WavgExpression(
            value=StringLiteral(value="col1", quoted=False),
            weight=StringLiteral(value="col2", quoted=False)
        )
        result = ext.process_wavg_expression(expr, SqlToStringConfig(format_=SqlToStringFormat()))
        assert result == "(SUM('col1' * 'col2') / SUM('col2'))"


class TestWavgFunctionEndToEnd:

    @pytest.mark.skip(reason="Legend engine SQL execution layer does not yet have a handler for the WAVG function")  # pragma: no cover
    def test_e2e_wavg_self_weight_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """Weighted average of Quantity with itself as weight, grouped by Product/Name.

        wavg(q, q) = sum(q*q)/sum(q).  For a single-row group with q=v the result is v.
        """
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby("Product/Name").aggregate(
            {"Quantity": lambda c: c.row_mapper(c).wavg()}
        )
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res["columns"] == ["Product/Name", "Quantity"]
        # Every row should have a non-null numeric Quantity
        for row in res["rows"]:
            product_name = row["values"][0]
            wavg_val = row["values"][1]
            assert product_name is not None
            assert isinstance(wavg_val, (int, float))

    @pytest.mark.skip(reason="Legend engine SQL execution layer does not yet have a handler for the WAVG function")  # pragma: no cover
    def test_e2e_wavg_self_weight_non_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """Weighted average of Quantity with itself as weight across all rows.

        wavg(q, q) = sum(q*q)/sum(q) — a single scalar result.
        """
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.aggregate(
            {"Quantity": lambda c: c.row_mapper(c).wavg()}
        )
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res["columns"] == ["Quantity"]
        assert len(res["rows"]) == 1
        assert isinstance(res["rows"][0]["values"][0], (int, float))
