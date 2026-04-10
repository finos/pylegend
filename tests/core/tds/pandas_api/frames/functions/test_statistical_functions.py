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
from tests.test_helpers.test_legend_service_frames import simple_relation_person_service_frame_pandas_api


# ─────────────────────────────────────────────────────────────────────────────
# Median tests
# ─────────────────────────────────────────────────────────────────────────────

class TestMedianFunctionQueryGeneration:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_median_groupby_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").median()
        expected_sql = dedent('''\
            SELECT
                "root".id AS "id",
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "root".val) AS "val"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".id
            ORDER BY
                "root".id''')
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_median_groupby_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").median()
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[id], ~[val:{r | $r.val}:{c | $c->median()->cast(@Float)}])'
            '->sort([~id->ascending()])'
        )

    def test_median_window_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        res = frame.groupby(by="id")["val"].transform("median")
        assigned = frame.assign(newCol=lambda _r: res)
        expected_sql = dedent('''\
            SELECT
                "root"."id" AS "id",
                "root"."val" AS "val",
                "root"."newCol__pylegend_olap_column__" AS "newCol"
            FROM
                (
                    SELECT
                        "root".id AS "id",
                        "root".val AS "val",
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "root".val) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"''')  # noqa: E501
        assert assigned.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_median_window_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        res = frame.groupby(by="id")["val"].transform("median")
        assigned = frame.assign(newCol=lambda _r: res)
        assert generate_pure_query_and_compile(assigned, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[id], []), ~val__pylegend_olap_column__:'
            '{p,w,r | $r.val}:{c | $c->median()->cast(@Float)})'
            '->project(~[id:c|$c.id, val:c|$c.val, newCol:c|$c.val__pylegend_olap_column__])'
        )


# ─────────────────────────────────────────────────────────────────────────────
# Mode tests
# ─────────────────────────────────────────────────────────────────────────────

class TestModeFunctionQueryGeneration:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_mode_groupby_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").mode()
        expected_sql = dedent('''\
            SELECT
                "root".id AS "id",
                MODE() WITHIN GROUP (ORDER BY "root".val) AS "val"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".id
            ORDER BY
                "root".id''')
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_mode_groupby_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").mode()
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[id], ~[val:{r | $r.val}:{c | $c->mode()}])'
            '->sort([~id->ascending()])'
        )

    def test_mode_window_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        res = frame.groupby(by="id")["val"].transform("mode")
        assigned = frame.assign(newCol=lambda _r: res)
        expected_sql = dedent('''\
            SELECT
                "root"."id" AS "id",
                "root"."val" AS "val",
                "root"."newCol__pylegend_olap_column__" AS "newCol"
            FROM
                (
                    SELECT
                        "root".id AS "id",
                        "root".val AS "val",
                        MODE() WITHIN GROUP (ORDER BY "root".val) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"''')  # noqa: E501
        assert assigned.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_mode_window_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        res = frame.groupby(by="id")["val"].transform("mode")
        assigned = frame.assign(newCol=lambda _r: res)
        assert generate_pure_query_and_compile(assigned, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[id], []), ~val__pylegend_olap_column__:'
            '{p,w,r | $r.val}:{c | $c->mode()})'
            '->project(~[id:c|$c.id, val:c|$c.val, newCol:c|$c.val__pylegend_olap_column__])'
        )


# ─────────────────────────────────────────────────────────────────────────────
# Percentile tests
# ─────────────────────────────────────────────────────────────────────────────

class TestPercentileFunctionQueryGeneration:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_percentile_cont_groupby_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").agg(
            {"val": lambda c: c.percentile(0.6, ascending=True, continuous=True)}
        )
        expected_sql = dedent('''\
            SELECT
                "root".id AS "id",
                PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY "root".val) AS "val"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".id
            ORDER BY
                "root".id''')
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_percentile_cont_groupby_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").agg(
            {"val": lambda c: c.percentile(0.6, ascending=True, continuous=True)}
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[id], ~[val:{r | $r.val}:{c | $c->percentile(0.6, true, true)->cast(@Float)}])'
            '->sort([~id->ascending()])'
        )

    def test_percentile_disc_groupby_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").agg(
            {"val": lambda c: c.percentile(0.6, ascending=True, continuous=False)}
        )
        expected_sql = dedent('''\
            SELECT
                "root".id AS "id",
                PERCENTILE_DISC(0.6) WITHIN GROUP (ORDER BY "root".val) AS "val"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".id
            ORDER BY
                "root".id''')
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_percentile_disc_groupby_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").agg(
            {"val": lambda c: c.percentile(0.6, ascending=True, continuous=False)}
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[id], ~[val:{r | $r.val}:{c | $c->percentile(0.6, true, false)->cast(@Float)}])'
            '->sort([~id->ascending()])'
        )

    def test_percentile_descending_groupby_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").agg(
            {"val": lambda c: c.percentile(0.75, ascending=False, continuous=True)}
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[id], ~[val:{r | $r.val}:{c | $c->percentile(0.75, false, true)->cast(@Float)}])'
            '->sort([~id->ascending()])'
        )

    def test_percentile_cont_window_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries
        wf = PandasApiWindowTdsFrame(base_frame=frame.groupby(by="id"), partition_only=True)
        ws = WindowSeries(window_frame=wf, column_name="val")
        wr = ws.aggregate(lambda c: c.percentile(0.6, ascending=True, continuous=True), 0)
        assigned = frame.assign(newCol=lambda _r: wr)
        expected_sql = dedent('''\
            SELECT
                "root"."id" AS "id",
                "root"."val" AS "val",
                "root"."newCol__pylegend_olap_column__" AS "newCol"
            FROM
                (
                    SELECT
                        "root".id AS "id",
                        "root".val AS "val",
                        PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY "root".val) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"''')  # noqa: E501
        assert assigned.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_percentile_cont_window_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries
        wf = PandasApiWindowTdsFrame(base_frame=frame.groupby(by="id"), partition_only=True)
        ws = WindowSeries(window_frame=wf, column_name="val")
        wr = ws.aggregate(lambda c: c.percentile(0.6, ascending=True, continuous=True), 0)
        assigned = frame.assign(newCol=lambda _r: wr)
        assert generate_pure_query_and_compile(assigned, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[id], []), ~val__pylegend_olap_column__:'
            '{p,w,r | $r.val}:{c | $c->percentile(0.6, true, true)->cast(@Float)})'
            '->project(~[id:c|$c.id, val:c|$c.val, newCol:c|$c.val__pylegend_olap_column__])'
        )

    def test_percentile_disc_window_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries
        wf = PandasApiWindowTdsFrame(base_frame=frame.groupby(by="id"), partition_only=True)
        ws = WindowSeries(window_frame=wf, column_name="val")
        wr = ws.aggregate(lambda c: c.percentile(0.6, ascending=True, continuous=False), 0)
        assigned = frame.assign(newCol=lambda _r: wr)
        expected_sql = dedent('''\
            SELECT
                "root"."id" AS "id",
                "root"."val" AS "val",
                "root"."newCol__pylegend_olap_column__" AS "newCol"
            FROM
                (
                    SELECT
                        "root".id AS "id",
                        "root".val AS "val",
                        PERCENTILE_DISC(0.6) WITHIN GROUP (ORDER BY "root".val) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"''')  # noqa: E501
        assert assigned.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_percentile_disc_window_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries
        wf = PandasApiWindowTdsFrame(base_frame=frame.groupby(by="id"), partition_only=True)
        ws = WindowSeries(window_frame=wf, column_name="val")
        wr = ws.aggregate(lambda c: c.percentile(0.6, ascending=True, continuous=False), 0)
        assigned = frame.assign(newCol=lambda _r: wr)
        assert generate_pure_query_and_compile(assigned, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[id], []), ~val__pylegend_olap_column__:'
            '{p,w,r | $r.val}:{c | $c->percentile(0.6, true, false)->cast(@Float)})'
            '->project(~[id:c|$c.id, val:c|$c.val, newCol:c|$c.val__pylegend_olap_column__])'
        )


# ─────────────────────────────────────────────────────────────────────────────
# End-to-End tests
# ─────────────────────────────────────────────────────────────────────────────

class TestMedianModePercentileEndToEnd:

    # ── median e2e ───────────────────────────────────────────────────────

    def test_e2e_median_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """groupby('Firm/Legal Name').median() on Age column."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame[["Firm/Legal Name", "Age"]]  # type: ignore
        frame = frame.groupby(by="Firm/Legal Name").median()

        # Firm X ages: 23, 22, 12, 22 => sorted: 12, 22, 22, 23 => median = (22+22)/2 = 22.0
        # Firm A (34) => 34.0, Firm B (32) => 32.0, Firm C (35) => 35.0
        expected = {
            "columns": ["Firm/Legal Name", "Age"],
            "rows": [
                {"values": ["Firm A", pytest.approx(34.0, abs=1e-6)]},
                {"values": ["Firm B", pytest.approx(32.0, abs=1e-6)]},
                {"values": ["Firm C", pytest.approx(35.0, abs=1e-6)]},
                {"values": ["Firm X", pytest.approx(22.0, abs=1e-6)]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res == expected

    def test_e2e_median_window_transform(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """Broadcast median per firm back to every row via transform."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["Age Median"] = frame.groupby("Firm/Legal Name")["Age"].transform("median")

        median_x = pytest.approx(22.0, abs=1e-6)
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age Median"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", median_x]},
                {"values": ["John", "Johnson", 22, "Firm X", median_x]},
                {"values": ["John", "Hill", 12, "Firm X", median_x]},
                {"values": ["Anthony", "Allen", 22, "Firm X", median_x]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", pytest.approx(34.0, abs=1e-6)]},
                {"values": ["Oliver", "Hill", 32, "Firm B", pytest.approx(32.0, abs=1e-6)]},
                {"values": ["David", "Harris", 35, "Firm C", pytest.approx(35.0, abs=1e-6)]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res == expected

    # ── mode e2e (skipped — no SQL handler) ──────────────────────────────

    @pytest.mark.skip(reason="Legend engine SQL execution layer does not yet have a handler for the MODE function")  # pragma: no cover
    def test_e2e_mode_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """groupby('Firm/Legal Name').mode() on Age column."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame[["Firm/Legal Name", "Age"]]  # type: ignore
        frame = frame.groupby(by="Firm/Legal Name").mode()

        expected = {
            "columns": ["Firm/Legal Name", "Age"],
            "rows": [
                {"values": ["Firm A", 34]},
                {"values": ["Firm B", 32]},
                {"values": ["Firm C", 35]},
                {"values": ["Firm X", 22]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res == expected

    @pytest.mark.skip(reason="Legend engine SQL execution layer does not yet have a handler for the MODE function")  # pragma: no cover
    def test_e2e_mode_window_transform(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """Broadcast mode per firm back to every row via transform."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["Age Mode"] = frame.groupby("Firm/Legal Name")["Age"].transform("mode")

        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age Mode"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", 22]},
                {"values": ["John", "Johnson", 22, "Firm X", 22]},
                {"values": ["John", "Hill", 12, "Firm X", 22]},
                {"values": ["Anthony", "Allen", 22, "Firm X", 22]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", 34]},
                {"values": ["Oliver", "Hill", 32, "Firm B", 32]},
                {"values": ["David", "Harris", 35, "Firm C", 35]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res == expected

    # ── percentile_cont e2e ──────────────────────────────────────────────

    def test_e2e_percentile_cont_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """groupby('Firm/Legal Name').agg(percentile(0.75, continuous)) on Age."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame[["Firm/Legal Name", "Age"]]  # type: ignore
        frame = frame.groupby(by="Firm/Legal Name").agg(
            {"Age": lambda c: c.percentile(0.75, ascending=True, continuous=True)}
        )

        # Firm X ages sorted: 12, 22, 22, 23
        # PERCENTILE_CONT(0.75): pos = 0.75 * 3 = 2.25 => 22 + 0.25*(23-22) = 22.25
        expected = {
            "columns": ["Firm/Legal Name", "Age"],
            "rows": [
                {"values": ["Firm A", pytest.approx(34.0, abs=1e-6)]},
                {"values": ["Firm B", pytest.approx(32.0, abs=1e-6)]},
                {"values": ["Firm C", pytest.approx(35.0, abs=1e-6)]},
                {"values": ["Firm X", pytest.approx(22.25, abs=1e-6)]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res == expected

    def test_e2e_percentile_cont_window_transform(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """Broadcast PERCENTILE_CONT(0.75) per firm back to every row."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries
        wf = PandasApiWindowTdsFrame(base_frame=frame.groupby("Firm/Legal Name"), partition_only=True)
        ws = WindowSeries(window_frame=wf, column_name="Age")
        wr = ws.aggregate(lambda c: c.percentile(0.75, ascending=True, continuous=True), 0)
        frame["Age P75"] = wr

        pct_x = pytest.approx(22.25, abs=1e-6)
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age P75"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", pct_x]},
                {"values": ["John", "Johnson", 22, "Firm X", pct_x]},
                {"values": ["John", "Hill", 12, "Firm X", pct_x]},
                {"values": ["Anthony", "Allen", 22, "Firm X", pct_x]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", pytest.approx(34.0, abs=1e-6)]},
                {"values": ["Oliver", "Hill", 32, "Firm B", pytest.approx(32.0, abs=1e-6)]},
                {"values": ["David", "Harris", 35, "Firm C", pytest.approx(35.0, abs=1e-6)]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res == expected

    # ── percentile_disc e2e ──────────────────────────────────────────────

    def test_e2e_percentile_disc_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """groupby('Firm/Legal Name').agg(percentile(0.75, discrete)) on Age."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame[["Firm/Legal Name", "Age"]]  # type: ignore
        frame = frame.groupby(by="Firm/Legal Name").agg(
            {"Age": lambda c: c.percentile(0.75, ascending=True, continuous=False)}
        )

        # Firm X ages sorted: 12, 22, 22, 23
        # PERCENTILE_DISC(0.75): nearest-rank => 22 (the value at the 75th percentile position)
        expected = {
            "columns": ["Firm/Legal Name", "Age"],
            "rows": [
                {"values": ["Firm A", pytest.approx(34.0, abs=1e-6)]},
                {"values": ["Firm B", pytest.approx(32.0, abs=1e-6)]},
                {"values": ["Firm C", pytest.approx(35.0, abs=1e-6)]},
                {"values": ["Firm X", pytest.approx(22.0, abs=1e-6)]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res == expected

    def test_e2e_percentile_disc_window_transform(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """Broadcast PERCENTILE_DISC(0.75) per firm back to every row."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries
        wf = PandasApiWindowTdsFrame(base_frame=frame.groupby("Firm/Legal Name"), partition_only=True)
        ws = WindowSeries(window_frame=wf, column_name="Age")
        wr = ws.aggregate(lambda c: c.percentile(0.75, ascending=True, continuous=False), 0)
        frame["Age P75 Disc"] = wr

        pct_x = pytest.approx(22.0, abs=1e-6)
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age P75 Disc"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", pct_x]},
                {"values": ["John", "Johnson", 22, "Firm X", pct_x]},
                {"values": ["John", "Hill", 12, "Firm X", pct_x]},
                {"values": ["Anthony", "Allen", 22, "Firm X", pct_x]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", pytest.approx(34.0, abs=1e-6)]},
                {"values": ["Oliver", "Hill", 32, "Firm B", pytest.approx(32.0, abs=1e-6)]},
                {"values": ["David", "Harris", 35, "Firm C", pytest.approx(35.0, abs=1e-6)]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res == expected

