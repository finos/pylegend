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
# StdDev tests
# ─────────────────────────────────────────────────────────────────────────────

class TestStdDevFunctionQueryGeneration:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    # ── groupby aggregate (sample) ────────────────────────────────────────

    def test_std_dev_sample_groupby_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").std()
        expected_sql = dedent('''\
            SELECT
                "root".id AS "id",
                STDDEV_SAMP("root".val) AS "val"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".id
            ORDER BY
                "root".id''')
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_std_dev_sample_groupby_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").std()
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[id], ~[val:{r | $r.val}:{c | $c->stdDevSample()->cast(@Float)}])'
            '->sort([~id->ascending()])'
        )

    # ── groupby aggregate (population) ────────────────────────────────────

    def test_std_dev_population_groupby_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").std(ddof=0)
        expected_sql = dedent('''\
            SELECT
                "root".id AS "id",
                STDDEV_POP("root".val) AS "val"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".id
            ORDER BY
                "root".id''')
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_std_dev_population_groupby_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").std(ddof=0)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[id], ~[val:{r | $r.val}:{c | $c->stdDevPopulation()->cast(@Float)}])'
            '->sort([~id->ascending()])'
        )

    # ── window / transform (sample) ──────────────────────────────────────

    def test_std_dev_sample_window_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        res = frame.groupby(by="id")["val"].transform("std_dev_sample")
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
                        STDDEV_SAMP("root".val) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"''')
        assert assigned.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_std_dev_sample_window_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        res = frame.groupby(by="id")["val"].transform("std_dev_sample")
        assigned = frame.assign(newCol=lambda _r: res)
        assert generate_pure_query_and_compile(assigned, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[id], []), ~val__pylegend_olap_column__:'
            '{p,w,r | $r.val}:{c | $c->stdDevSample()->cast(@Float)})'
            '->project(~[id:c|$c.id, val:c|$c.val, newCol:c|$c.val__pylegend_olap_column__])'
        )

    # ── window / transform (population) ──────────────────────────────────

    def test_std_dev_population_window_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        res = frame.groupby(by="id")["val"].transform("std_dev_population")
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
                        STDDEV_POP("root".val) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"''')
        assert assigned.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_std_dev_population_window_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        res = frame.groupby(by="id")["val"].transform("std_dev_population")
        assigned = frame.assign(newCol=lambda _r: res)
        assert generate_pure_query_and_compile(assigned, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[id], []), ~val__pylegend_olap_column__:'
            '{p,w,r | $r.val}:{c | $c->stdDevPopulation()->cast(@Float)})'
            '->project(~[id:c|$c.id, val:c|$c.val, newCol:c|$c.val__pylegend_olap_column__])'
        )

    # ── invalid ddof ─────────────────────────────────────────────────────

    def test_std_invalid_ddof_groupby_tds_frame(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as exc:
            frame.groupby(by="id").std(ddof=2)
        assert "but got: 2" in str(exc.value)

    def test_std_invalid_ddof_groupby_series(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as exc:
            frame.groupby(by="id")["val"].std(ddof=2)
        assert "but got: 2" in str(exc.value)


# ─────────────────────────────────────────────────────────────────────────────
# Variance tests
# ─────────────────────────────────────────────────────────────────────────────

class TestVarianceFunctionQueryGeneration:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    # ── groupby aggregate (sample) ────────────────────────────────────────

    def test_variance_sample_groupby_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").var()
        expected_sql = dedent('''\
            SELECT
                "root".id AS "id",
                VAR_SAMP("root".val) AS "val"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".id
            ORDER BY
                "root".id''')
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_variance_sample_groupby_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").var()
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[id], ~[val:{r | $r.val}:{c | $c->varianceSample()->cast(@Float)}])'
            '->sort([~id->ascending()])'
        )

    # ── groupby aggregate (population) ────────────────────────────────────

    def test_variance_population_groupby_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").var(ddof=0)
        expected_sql = dedent('''\
            SELECT
                "root".id AS "id",
                VAR_POP("root".val) AS "val"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".id
            ORDER BY
                "root".id''')
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_variance_population_groupby_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="id").var(ddof=0)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->groupBy(~[id], ~[val:{r | $r.val}:{c | $c->variancePopulation()->cast(@Float)}])'
            '->sort([~id->ascending()])'
        )

    # ── window / transform (sample) ──────────────────────────────────────

    def test_variance_sample_window_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        res = frame.groupby(by="id")["val"].transform("variance_sample")
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
                        VAR_SAMP("root".val) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"''')
        assert assigned.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_variance_sample_window_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        res = frame.groupby(by="id")["val"].transform("variance_sample")
        assigned = frame.assign(newCol=lambda _r: res)
        assert generate_pure_query_and_compile(assigned, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[id], []), ~val__pylegend_olap_column__:'
            '{p,w,r | $r.val}:{c | $c->varianceSample()->cast(@Float)})'
            '->project(~[id:c|$c.id, val:c|$c.val, newCol:c|$c.val__pylegend_olap_column__])'
        )

    # ── window / transform (population) ──────────────────────────────────

    def test_variance_population_window_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        res = frame.groupby(by="id")["val"].transform("variance_population")
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
                        VAR_POP("root".val) OVER (PARTITION BY "root".id) AS "newCol__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"''')
        assert assigned.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_variance_population_window_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        res = frame.groupby(by="id")["val"].transform("variance_population")
        assigned = frame.assign(newCol=lambda _r: res)
        assert generate_pure_query_and_compile(assigned, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#'
            '->extend(over(~[id], []), ~val__pylegend_olap_column__:'
            '{p,w,r | $r.val}:{c | $c->variancePopulation()->cast(@Float)})'
            '->project(~[id:c|$c.id, val:c|$c.val, newCol:c|$c.val__pylegend_olap_column__])'
        )

    # ── invalid ddof ─────────────────────────────────────────────────────

    def test_var_invalid_ddof_groupby_tds_frame(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as exc:
            frame.groupby(by="id").var(ddof=2)
        assert "but got: 2" in str(exc.value)

    def test_var_invalid_ddof_groupby_series(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as exc:
            frame.groupby(by="id")["val"].var(ddof=2)
        assert "but got: 2" in str(exc.value)


# ─────────────────────────────────────────────────────────────────────────────
# End-to-End tests
# ─────────────────────────────────────────────────────────────────────────────

class TestStdVarEndToEnd:

    def test_e2e_std_dev_sample_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """groupby('Firm/Legal Name').std() on Age column."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame[["Firm/Legal Name", "Age"]]  # type: ignore
        frame = frame.groupby(by="Firm/Legal Name").std()

        expected = {
            "columns": ["Firm/Legal Name", "Age"],
            "rows": [
                {"values": ["Firm A", None]},
                {"values": ["Firm B", None]},
                {"values": ["Firm C", None]},
                {"values": ["Firm X", pytest.approx(5.188127472091127, abs=1e-6)]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res == expected

    def test_e2e_std_dev_sample_window_transform(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """Broadcast stdDevSample per firm back to every row via transform."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["Age StdDev"] = frame.groupby("Firm/Legal Name")["Age"].transform("std_dev_sample")

        # Original row order preserved, stddev broadcast per group
        stddev_x = pytest.approx(5.188127472091127, abs=1e-6)
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age StdDev"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", stddev_x]},
                {"values": ["John", "Johnson", 22, "Firm X", stddev_x]},
                {"values": ["John", "Hill", 12, "Firm X", stddev_x]},
                {"values": ["Anthony", "Allen", 22, "Firm X", stddev_x]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", None]},
                {"values": ["Oliver", "Hill", 32, "Firm B", None]},
                {"values": ["David", "Harris", 35, "Firm C", None]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res == expected

    def test_e2e_variance_sample_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """groupby('Firm/Legal Name').var() on Age column."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame[["Firm/Legal Name", "Age"]]  # type: ignore
        frame = frame.groupby(by="Firm/Legal Name").var()

        expected = {
            "columns": ["Firm/Legal Name", "Age"],
            "rows": [
                {"values": ["Firm A", None]},
                {"values": ["Firm B", None]},
                {"values": ["Firm C", None]},
                {"values": ["Firm X", pytest.approx(26.916666666666668, abs=1e-6)]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res == expected

    def test_e2e_variance_sample_window_transform(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """Broadcast varianceSample per firm back to every row via transform."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["Age Var"] = frame.groupby("Firm/Legal Name")["Age"].transform("variance_sample")

        var_x = pytest.approx(26.916666666666668, abs=1e-6)
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age Var"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", var_x]},
                {"values": ["John", "Johnson", 22, "Firm X", var_x]},
                {"values": ["John", "Hill", 12, "Firm X", var_x]},
                {"values": ["Anthony", "Allen", 22, "Firm X", var_x]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", None]},
                {"values": ["Oliver", "Hill", 32, "Firm B", None]},
                {"values": ["David", "Harris", 35, "Firm C", None]},
            ],
        }
        res = json.loads(frame.execute_frame_to_string())["result"]
        assert res == expected
