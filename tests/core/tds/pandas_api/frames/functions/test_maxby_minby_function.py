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


class TestMaxByFunctionQueryGeneration:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_max_by_groupby_aggregate_sql_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.integer_column("name"),
            PrimitiveTdsColumn.integer_column("employeeNumber"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="grp").aggregate(
            {"name": lambda c: c.row_mapper(c).max_by_legend_ext()}
        )
        expected_sql = '''\
            SELECT
                "root".grp AS "grp",
                MAX_BY("root".name, "root".name) AS "name"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".grp
            ORDER BY
                "root".grp'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_max_by_groupby_pure_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.integer_column("name"),
            PrimitiveTdsColumn.integer_column("employeeNumber"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="grp").aggregate(
            {"name": lambda c: c.row_mapper(c).max_by_legend_ext()}
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[grp], ~[name:{r | $r.name}:{c | $c->maxBy($c)}])'
            '->sort([~grp->ascending()])'
        )

    def test_max_by_collection_method(self) -> None:
        from pylegend.core.language import PyLegendNumber
        from pylegend.core.language import PyLegendFloat
        from pylegend.core.language.shared.literal_expressions import PyLegendFloatLiteralExpression
        val = PyLegendFloat(PyLegendFloatLiteralExpression(1.0))
        pair = val.row_mapper(2.0)
        result = pair.max_by_legend_ext()
        assert isinstance(result, PyLegendNumber)

    def test_max_by_window_sql_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("name"),
            PrimitiveTdsColumn.integer_column("employeeNumber"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        frame["newCol"] = gb["name"].max_by_legend_ext(gb["employeeNumber"])
        expected_sql = '''\
            SELECT
                "root"."id" AS "id",
                "root"."name" AS "name",
                "root"."employeeNumber" AS "employeeNumber",
                "root"."newCol__pylegend_olap_column__" AS "newCol"
            FROM
                (
                    SELECT
                        "root".id AS "id",
                        "root".name AS "name",
                        "root".employeeNumber AS "employeeNumber",
                        MAX_BY("root".name, "root".employeeNumber) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_max_by_window_pure_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("name"),
            PrimitiveTdsColumn.integer_column("employeeNumber"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        frame["newCol"] = gb["name"].max_by_legend_ext(gb["employeeNumber"])
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[id], []), ~name__pylegend_olap_column__:{p,w,r | meta::pure::functions::math::mathUtility::rowMapper($r.name, $r.employeeNumber)}:y | $y->meta::pure::functions::math::maxBy())'
            '->project(~[id:c|$c.id, name:c|$c.name, employeeNumber:c|$c.employeeNumber, newCol:c|$c.name__pylegend_olap_column__])'
        )

    def test_max_by_sql_expression_rendering(self) -> None:
        from pylegend.core.sql.metamodel_extension import MaxByExpression
        from pylegend.core.sql.metamodel import StringLiteral
        from pylegend.core.database.sql_to_string.db_extension import SqlToStringDbExtension
        from pylegend.core.database.sql_to_string.config import SqlToStringConfig, SqlToStringFormat

        ext = SqlToStringDbExtension()
        expr = MaxByExpression(
            value=StringLiteral(value="col1", quoted=False),
            by=StringLiteral(value="col2", quoted=False)
        )
        result = ext.process_max_by_expression(expr, SqlToStringConfig(format_=SqlToStringFormat()))
        assert result == "MAX_BY('col1', 'col2')"


class TestMinByFunctionQueryGeneration:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_min_by_groupby_aggregate_sql_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.integer_column("name"),
            PrimitiveTdsColumn.integer_column("employeeNumber"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="grp").aggregate(
            {"name": lambda c: c.row_mapper(c).min_by_legend_ext()}
        )
        expected_sql = '''\
            SELECT
                "root".grp AS "grp",
                MIN_BY("root".name, "root".name) AS "name"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".grp
            ORDER BY
                "root".grp'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_min_by_groupby_pure_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.integer_column("name"),
            PrimitiveTdsColumn.integer_column("employeeNumber"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="grp").aggregate(
            {"name": lambda c: c.row_mapper(c).min_by_legend_ext()}
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[grp], ~[name:{r | $r.name}:{c | $c->minBy($c)}])'
            '->sort([~grp->ascending()])'
        )

    def test_min_by_collection_method(self) -> None:
        from pylegend.core.language import PyLegendNumber
        from pylegend.core.language import PyLegendFloat
        from pylegend.core.language.shared.literal_expressions import PyLegendFloatLiteralExpression
        val = PyLegendFloat(PyLegendFloatLiteralExpression(1.0))
        pair = val.row_mapper(2.0)
        result = pair.min_by_legend_ext()
        assert isinstance(result, PyLegendNumber)

    def test_min_by_window_sql_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("name"),
            PrimitiveTdsColumn.integer_column("employeeNumber"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        frame["newCol"] = gb["name"].min_by_legend_ext(gb["employeeNumber"])
        expected_sql = '''\
            SELECT
                "root"."id" AS "id",
                "root"."name" AS "name",
                "root"."employeeNumber" AS "employeeNumber",
                "root"."newCol__pylegend_olap_column__" AS "newCol"
            FROM
                (
                    SELECT
                        "root".id AS "id",
                        "root".name AS "name",
                        "root".employeeNumber AS "employeeNumber",
                        MIN_BY("root".name, "root".employeeNumber) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_min_by_window_pure_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("name"),
            PrimitiveTdsColumn.integer_column("employeeNumber"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="id")
        frame["newCol"] = gb["name"].min_by_legend_ext(gb["employeeNumber"])
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[id], []), ~name__pylegend_olap_column__:{p,w,r | meta::pure::functions::math::mathUtility::rowMapper($r.name, $r.employeeNumber)}:y | $y->meta::pure::functions::math::minBy())'
            '->project(~[id:c|$c.id, name:c|$c.name, employeeNumber:c|$c.employeeNumber, newCol:c|$c.name__pylegend_olap_column__])'
        )

    def test_min_by_sql_expression_rendering(self) -> None:
        from pylegend.core.sql.metamodel_extension import MinByExpression
        from pylegend.core.sql.metamodel import StringLiteral
        from pylegend.core.database.sql_to_string.db_extension import SqlToStringDbExtension
        from pylegend.core.database.sql_to_string.config import SqlToStringConfig, SqlToStringFormat

        ext = SqlToStringDbExtension()
        expr = MinByExpression(
            value=StringLiteral(value="col1", quoted=False),
            by=StringLiteral(value="col2", quoted=False)
        )
        result = ext.process_min_by_expression(expr, SqlToStringConfig(format_=SqlToStringFormat()))
        assert result == "MIN_BY('col1', 'col2')"


class TestMaxByMinByFunctionEndToEnd:

    @pytest.mark.skip(reason="Legend engine SQL execution layer does not yet have a handler for MAX_BY/MIN_BY")  # pragma: no cover
    def test_e2e_max_by_self_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """maxBy(Quantity, Quantity) grouped by Product/Name — returns the max Quantity per group."""
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby("Product/Name").aggregate(
            {"Quantity": lambda c: c.row_mapper(c).max_by_legend_ext()}
        )
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res["columns"] == ["Product/Name", "Quantity"]
        assert len(res["rows"]) > 0
        for row in res["rows"]:
            product_name = row["values"][0]
            max_by_val = row["values"][1]
            assert product_name is not None
            assert isinstance(max_by_val, (int, float))

    @pytest.mark.skip(reason="Legend engine SQL execution layer does not yet have a handler for MAX_BY/MIN_BY")  # pragma: no cover
    def test_e2e_min_by_self_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """minBy(Quantity, Quantity) grouped by Product/Name — returns the min Quantity per group."""
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby("Product/Name").aggregate(
            {"Quantity": lambda c: c.row_mapper(c).min_by_legend_ext()}
        )
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res["columns"] == ["Product/Name", "Quantity"]
        assert len(res["rows"]) > 0
        for row in res["rows"]:
            product_name = row["values"][0]
            min_by_val = row["values"][1]
            assert product_name is not None
            assert isinstance(min_by_val, (int, float))

    @pytest.mark.skip(reason="Legend engine SQL execution layer does not yet have a handler for MAX_BY/MIN_BY")  # pragma: no cover
    def test_e2e_max_by_self_non_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """maxBy(Quantity, Quantity) across all rows — returns the single max Quantity."""
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.aggregate(
            {"Quantity": lambda c: c.row_mapper(c).max_by_legend_ext()}
        )
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res["columns"] == ["Quantity"]
        assert len(res["rows"]) == 1
        assert isinstance(res["rows"][0]["values"][0], (int, float))

    @pytest.mark.skip(reason="Legend engine SQL execution layer does not yet have a handler for MAX_BY/MIN_BY")  # pragma: no cover
    def test_e2e_min_by_self_non_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """minBy(Quantity, Quantity) across all rows — returns the single min Quantity."""
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.aggregate(
            {"Quantity": lambda c: c.row_mapper(c).min_by_legend_ext()}
        )
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res["columns"] == ["Quantity"]
        assert len(res["rows"]) == 1
        assert isinstance(res["rows"][0]["values"][0], (int, float))
