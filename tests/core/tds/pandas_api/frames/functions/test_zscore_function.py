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
from tests.test_helpers.test_legend_service_frames import simple_relation_person_service_frame_pandas_api


# ─────────────────────────────────────────────────────────────────────────────
# Query generation tests
# ─────────────────────────────────────────────────────────────────────────────

class TestZScoreFunctionQueryGeneration:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    # ── SQL generation ────────────────────────────────────────────────────

    def test_zscore_integer_column_sql(self) -> None:
        """zscore on integer column generates correct SQL with AVG/STDDEV_POP window functions."""
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.string_column("name"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        zs = frame.groupby(by="grp")["id"].zscore()
        assigned = frame.assign(zScore=lambda r: zs)
        expected_sql = dedent('''\
            SELECT
                "root"."id" AS "id",
                "root"."grp" AS "grp",
                "root"."name" AS "name",
                "root"."zScore__pylegend_olap_column__" AS "zScore"
            FROM
                (
                    SELECT
                        "root".id AS "id",
                        "root".grp AS "grp",
                        "root".name AS "name",
                        ((1.0 * ("root".id - AVG("root".id) OVER (PARTITION BY "root".grp))) / STDDEV_POP("root".id) OVER (PARTITION BY "root".grp)) AS "zScore__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"''')
        assert assigned.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_zscore_float_column_sql(self) -> None:
        """zscore on float column generates correct SQL."""
        columns = [
            PrimitiveTdsColumn.float_column("val"),
            PrimitiveTdsColumn.integer_column("grp"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        zs = frame.groupby(by="grp")["val"].zscore()
        assigned = frame.assign(zScore=lambda r: zs)
        expected_sql = dedent('''\
            SELECT
                "root"."val" AS "val",
                "root"."grp" AS "grp",
                "root"."zScore__pylegend_olap_column__" AS "zScore"
            FROM
                (
                    SELECT
                        "root".val AS "val",
                        "root".grp AS "grp",
                        ((1.0 * ("root".val - AVG("root".val) OVER (PARTITION BY "root".grp))) / STDDEV_POP("root".val) OVER (PARTITION BY "root".grp)) AS "zScore__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"''')
        assert assigned.to_sql_query(FrameToSqlConfig()) == expected_sql

    # ── Pure generation ───────────────────────────────────────────────────

    def test_zscore_integer_column_pure(self) -> None:
        """zscore generates correct Pure with zScore function call."""
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.string_column("name"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        zs = frame.groupby(by="grp")["id"].zscore()
        assigned = frame.assign(zScore=lambda r: zs)
        assert generate_pure_query_and_compile(assigned, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[grp], []), ~id__pylegend_olap_column__:'
            '{p,w,r | meta::pure::functions::math::zScore($p, $w, $r, ~id)})'
            '->project(~[id:c|$c.id, grp:c|$c.grp, name:c|$c.name, zScore:c|$c.id__pylegend_olap_column__])'
        )

    def test_zscore_integer_column_pure_pretty(self) -> None:
        """zscore generates correct pretty Pure."""
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.string_column("name"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        zs = frame.groupby(by="grp")["id"].zscore()
        assigned = frame.assign(zScore=lambda r: zs)
        assert generate_pure_query_and_compile(assigned, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[grp], []), ~id__pylegend_olap_column__:{p,w,r | meta::pure::functions::math::zScore($p, $w, $r, ~id)})
              ->project(~[id:c|$c.id, grp:c|$c.grp, name:c|$c.name, zScore:c|$c.id__pylegend_olap_column__])'''
        )

    def test_zscore_float_column_pure(self) -> None:
        """zscore on float column generates correct Pure."""
        columns = [
            PrimitiveTdsColumn.float_column("val"),
            PrimitiveTdsColumn.integer_column("grp"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        zs = frame.groupby(by="grp")["val"].zscore()
        assigned = frame.assign(zScore=lambda r: zs)
        assert generate_pure_query_and_compile(assigned, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[grp], []), ~val__pylegend_olap_column__:'
            '{p,w,r | meta::pure::functions::math::zScore($p, $w, $r, ~val)})'
            '->project(~[val:c|$c.val, grp:c|$c.grp, zScore:c|$c.val__pylegend_olap_column__])'
        )

    # ── Multiple partition columns ────────────────────────────────────────

    def test_zscore_multiple_partition_columns_sql(self) -> None:
        """zscore with multiple groupby columns."""
        columns = [
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.integer_column("grp1"),
            PrimitiveTdsColumn.integer_column("grp2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        zs = frame.groupby(by=["grp1", "grp2"])["val"].zscore()
        assigned = frame.assign(zScore=lambda r: zs)
        sql = assigned.to_sql_query(FrameToSqlConfig())
        assert "PARTITION BY \"root\".grp1, \"root\".grp2" in sql
        assert "AVG(\"root\".val) OVER" in sql
        assert "STDDEV_POP(\"root\".val) OVER" in sql

    def test_zscore_multiple_partition_columns_pure(self) -> None:
        """zscore with multiple groupby columns in Pure."""
        columns = [
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.integer_column("grp1"),
            PrimitiveTdsColumn.integer_column("grp2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        zs = frame.groupby(by=["grp1", "grp2"])["val"].zscore()
        assigned = frame.assign(zScore=lambda r: zs)
        assert generate_pure_query_and_compile(assigned, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[grp1, grp2], []), ~val__pylegend_olap_column__:'
            '{p,w,r | meta::pure::functions::math::zScore($p, $w, $r, ~val)})'
            '->project(~[val:c|$c.val, grp1:c|$c.grp1, grp2:c|$c.grp2, zScore:c|$c.val__pylegend_olap_column__])'
        )

    # ── Validation ────────────────────────────────────────────────────────

    def test_zscore_missing_column_raises(self) -> None:
        """ZScoreWindowFunction raises ValueError for non-existent column."""
        from pylegend.core.tds.pandas_api.frames.functions.zscore_window_function import ZScoreWindowFunction
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("grp"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="grp")
        with pytest.raises(ValueError) as exc:
            ZScoreWindowFunction(
                base_frame=gb,
                col_name="nonexistent",
                result_col_name="zs",
            )
        assert "nonexistent" in str(exc.value)
        assert "does not exist" in str(exc.value)

    # ── Return type ───────────────────────────────────────────────────────

    def test_zscore_returns_float_groupby_series(self) -> None:
        """zscore() returns a FloatGroupbySeries."""
        from pylegend.core.language.pandas_api.pandas_api_groupby_series import FloatGroupbySeries
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("grp"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        result = frame.groupby(by="grp")["id"].zscore()
        assert isinstance(result, FloatGroupbySeries)

    # ── Column type inference ─────────────────────────────────────────────

    def test_zscore_result_column_is_float(self) -> None:
        """The assigned zScore column should be Float type."""
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("grp"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        zs = frame.groupby(by="grp")["id"].zscore()
        assigned = frame.assign(zScore=lambda r: zs)
        col_names = [c.get_name() for c in assigned.columns()]
        col_types = [c.get_type() for c in assigned.columns()]
        assert "zScore" in col_names
        idx = col_names.index("zScore")
        assert col_types[idx] == "Float"


# ─────────────────────────────────────────────────────────────────────────────
# End-to-End tests
# ─────────────────────────────────────────────────────────────────────────────

class TestZScoreEndToEnd:

    @pytest.mark.skip(reason="Legend engine SQL layer does not yet support window functions within function calls")  # pragma: no cover
    def test_e2e_zscore_window(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """Broadcast zScore per firm back to every row via assign."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        zs = frame.groupby("Firm/Legal Name")["Age"].zscore()
        frame["Age ZScore"] = zs

        # Firm X has ages: [23, 22, 12, 22]
        # mean = 19.75, stddev_pop = sqrt(((23-19.75)^2 + (22-19.75)^2 + (12-19.75)^2 + (22-19.75)^2)/4)
        #       = sqrt((10.5625 + 5.0625 + 60.0625 + 5.0625)/4)
        #       = sqrt(80.75/4) = sqrt(20.1875) ≈ 4.493052...
        # zscores:  (23-19.75)/4.493 ≈ 0.7234, (22-19.75)/4.493 ≈ 0.5008,
        #           (12-19.75)/4.493 ≈ -1.7234, (22-19.75)/4.493 ≈ 0.5008
        # Single-member groups: stddev_pop = 0, so zscore = NULL

        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age ZScore"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", pytest.approx(0.7234560696552925, abs=1e-6)]},
                {"values": ["John", "Johnson", 22, "Firm X", pytest.approx(0.5008080482537564, abs=1e-6)]},
                {"values": ["John", "Hill", 12, "Firm X", pytest.approx(-1.7250201661628053, abs=1e-6)]},
                {"values": ["Anthony", "Allen", 22, "Firm X", pytest.approx(0.5008080482537564, abs=1e-6)]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", None]},
                {"values": ["Oliver", "Hill", 32, "Firm B", None]},
                {"values": ["David", "Harris", 35, "Firm C", None]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res == expected


class TestZScoreWindowFunctionInternals:
    """Tests for ZScoreWindowFunction internal methods."""

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_zscore_window_function_name(self) -> None:
        """Test that ZScoreWindowFunction.name() returns the expected value."""
        from pylegend.core.tds.pandas_api.frames.functions.zscore_window_function import ZScoreWindowFunction
        assert ZScoreWindowFunction.name() == "zscore_window"

    def test_zscore_window_function_base_frame(self) -> None:
        """Test base_frame() returns the underlying TDS frame."""
        from pylegend.core.tds.pandas_api.frames.functions.zscore_window_function import ZScoreWindowFunction
        from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("grp"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="grp")
        func = ZScoreWindowFunction(
            base_frame=gb,
            col_name="id",
            result_col_name="zScore",
        )
        base = func.base_frame()
        assert isinstance(base, PandasApiBaseTdsFrame)

    def test_zscore_window_function_tds_frame_parameters(self) -> None:
        """Test tds_frame_parameters() returns empty list."""
        from pylegend.core.tds.pandas_api.frames.functions.zscore_window_function import ZScoreWindowFunction

        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("grp"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="grp")
        func = ZScoreWindowFunction(
            base_frame=gb,
            col_name="id",
            result_col_name="zScore",
        )
        params = func.tds_frame_parameters()
        assert params == []

    def test_zscore_window_function_to_sql(self) -> None:
        """Test to_sql() generates proper QuerySpecification."""
        from pylegend.core.tds.pandas_api.frames.functions.zscore_window_function import ZScoreWindowFunction
        from pylegend.core.sql.metamodel import QuerySpecification

        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("grp"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="grp")
        func = ZScoreWindowFunction(
            base_frame=gb,
            col_name="id",
            result_col_name="zScore",
        )
        query = func.to_sql(FrameToSqlConfig())
        assert isinstance(query, QuerySpecification)

    def test_zscore_window_function_to_pure(self) -> None:
        """Test to_pure() generates proper Pure query string."""
        from pylegend.core.tds.pandas_api.frames.functions.zscore_window_function import ZScoreWindowFunction

        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("grp"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby(by="grp")
        func = ZScoreWindowFunction(
            base_frame=gb,
            col_name="id",
            result_col_name="zScore",
        )
        pure = func.to_pure(FrameToPureConfig())
        assert "extend" in pure
        assert "project" in pure
        assert "zScore__pylegend_olap_column__" in pure
        assert "zScore" in pure

