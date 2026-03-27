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

# flake8: noqa

import json
from textwrap import dedent
import pytest

from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from tests.test_helpers import generate_pure_query_and_compile


class TestDropDuplicatesFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_drop_duplicates_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # keep
        with pytest.raises(NotImplementedError) as n:
            frame.drop_duplicates(keep='last')
        assert n.value.args[0] == (
            "keep='last' is not supported yet in Pandas API drop_duplicates. "
            "Only keep='first' is supported."
        )

        with pytest.raises(NotImplementedError) as n:
            frame.drop_duplicates(keep=False)  # type: ignore
        assert n.value.args[0] == (
            "keep='False' is not supported yet in Pandas API drop_duplicates. "
            "Only keep='first' is supported."
        )

        # inplace
        with pytest.raises(NotImplementedError) as n:
            frame.drop_duplicates(inplace=True)
        assert n.value.args[0] == "inplace=True is not supported yet in Pandas API drop_duplicates"

        # ignore_index
        with pytest.raises(NotImplementedError) as n:
            frame.drop_duplicates(ignore_index=True)
        assert n.value.args[0] == "ignore_index=True is not supported yet in Pandas API drop_duplicates"

        # subset type error
        with pytest.raises(TypeError) as t:
            frame.drop_duplicates(subset=123)  # type: ignore
        assert t.value.args[0] == (
            "subset must be a column label or list of column labels, "
            "but got <class 'int'>"
        )

        # subset invalid column
        with pytest.raises(KeyError) as k:
            frame.drop_duplicates(subset=["col3"])
        assert k.value.args[0] == "['col3']"

        with pytest.raises(KeyError) as k:
            frame.drop_duplicates(subset="col3")
        assert k.value.args[0] == "['col3']"

    def test_drop_duplicates_sql_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # default (all columns)
        newframe = frame.drop_duplicates()
        expected_sql = dedent("""\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        "root"."__INTERNAL_PYLEGEND_ROW_NUM__" AS "__INTERNAL_PYLEGEND_ROW_NUM__"
                    FROM
                        (
                            SELECT
                                "root"."col1" AS "col1",
                                "root"."col2" AS "col2",
                                row_number() OVER (PARTITION BY "root"."col1", "root"."col2") AS "__INTERNAL_PYLEGEND_ROW_NUM__"
                            FROM
                                (
                                    SELECT
                                        "root".col1 AS "col1",
                                        "root".col2 AS "col2"
                                    FROM
                                        test_schema.test_table AS "root"
                                ) AS "root"
                        ) AS "root"
                    WHERE
                        ("root"."__INTERNAL_PYLEGEND_ROW_NUM__" = 1)
                ) AS "root\"""")
        assert newframe.to_sql_query(FrameToSqlConfig()) == expected_sql

        expected_pure = dedent("""\
            #Table(test_schema.test_table)#
              ->extend(over(~[col1, col2], []), ~__INTERNAL_PYLEGEND_ROW_NUM__:{p,w,r | $p->rowNumber($r)})
              ->filter(c|$c.__INTERNAL_PYLEGEND_ROW_NUM__ == 1)
              ->project(~[col1:p|$p.col1, col2:p|$p.col2])""")
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == expected_pure

        # subset single column
        newframe_subset = frame.drop_duplicates(subset=["col1"])
        expected_sql_subset = dedent("""\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        "root"."__INTERNAL_PYLEGEND_ROW_NUM__" AS "__INTERNAL_PYLEGEND_ROW_NUM__"
                    FROM
                        (
                            SELECT
                                "root"."col1" AS "col1",
                                "root"."col2" AS "col2",
                                row_number() OVER (PARTITION BY "root"."col1") AS "__INTERNAL_PYLEGEND_ROW_NUM__"
                            FROM
                                (
                                    SELECT
                                        "root".col1 AS "col1",
                                        "root".col2 AS "col2"
                                    FROM
                                        test_schema.test_table AS "root"
                                ) AS "root"
                        ) AS "root"
                    WHERE
                        ("root"."__INTERNAL_PYLEGEND_ROW_NUM__" = 1)
                ) AS "root\"""")
        assert newframe_subset.to_sql_query(FrameToSqlConfig()) == expected_sql_subset

        expected_pure_subset = dedent("""\
            #Table(test_schema.test_table)#
              ->extend(over(~[col1], []), ~__INTERNAL_PYLEGEND_ROW_NUM__:{p,w,r | $p->rowNumber($r)})
              ->filter(c|$c.__INTERNAL_PYLEGEND_ROW_NUM__ == 1)
              ->project(~[col1:p|$p.col1, col2:p|$p.col2])""")
        assert generate_pure_query_and_compile(newframe_subset, FrameToPureConfig(), self.legend_client) == expected_pure_subset

        # subset as string
        newframe_str = frame.drop_duplicates(subset="col2")
        expected_sql_str = dedent("""\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        "root"."__INTERNAL_PYLEGEND_ROW_NUM__" AS "__INTERNAL_PYLEGEND_ROW_NUM__"
                    FROM
                        (
                            SELECT
                                "root"."col1" AS "col1",
                                "root"."col2" AS "col2",
                                row_number() OVER (PARTITION BY "root"."col2") AS "__INTERNAL_PYLEGEND_ROW_NUM__"
                            FROM
                                (
                                    SELECT
                                        "root".col1 AS "col1",
                                        "root".col2 AS "col2"
                                    FROM
                                        test_schema.test_table AS "root"
                                ) AS "root"
                        ) AS "root"
                    WHERE
                        ("root"."__INTERNAL_PYLEGEND_ROW_NUM__" = 1)
                ) AS "root\"""")
        assert newframe_str.to_sql_query(FrameToSqlConfig()) == expected_sql_str

        expected_pure_str = dedent("""\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], []), ~__INTERNAL_PYLEGEND_ROW_NUM__:{p,w,r | $p->rowNumber($r)})
              ->filter(c|$c.__INTERNAL_PYLEGEND_ROW_NUM__ == 1)
              ->project(~[col1:p|$p.col1, col2:p|$p.col2])""")
        assert generate_pure_query_and_compile(newframe_str, FrameToPureConfig(), self.legend_client) == expected_pure_str

    def test_drop_duplicates_columns_preserved(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        newframe = frame.drop_duplicates(subset=["col1"])
        result_cols = [c.get_name() for c in newframe.columns()]
        assert result_cols == ["col1", "col2", "col3"]

    def test_e2e_drop_duplicates(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        # drop_duplicates on all columns - person data has no exact duplicates so all 7 rows should remain
        newframe = frame.drop_duplicates()
        res = json.loads(newframe.execute_frame_to_string())["result"]
        assert res["columns"] == ['First Name', 'Last Name', 'Age', 'Firm/Legal Name']
        assert len(res["rows"]) == 7
        result_tuples = sorted([tuple(r["values"]) for r in res["rows"]])
        expected_tuples = sorted([
            ('Anthony', 'Allen', 22, 'Firm X'),
            ('David', 'Harris', 35, 'Firm C'),
            ('Fabrice', 'Roberts', 34, 'Firm A'),
            ('John', 'Hill', 12, 'Firm X'),
            ('John', 'Johnson', 22, 'Firm X'),
            ('Oliver', 'Hill', 32, 'Firm B'),
            ('Peter', 'Smith', 23, 'Firm X'),
        ])
        assert result_tuples == expected_tuples

        # drop_duplicates on 'Firm/Legal Name' - should keep one row per firm (4 unique firms)
        newframe_firm = frame.drop_duplicates(subset=["Firm/Legal Name"])
        res_firm = json.loads(newframe_firm.execute_frame_to_string())["result"]
        assert res_firm["columns"] == ['First Name', 'Last Name', 'Age', 'Firm/Legal Name']
        assert len(res_firm["rows"]) == 4
        firm_names = sorted([r["values"][3] for r in res_firm["rows"]])
        assert firm_names == ['Firm A', 'Firm B', 'Firm C', 'Firm X']

        # drop_duplicates on 'Last Name' - "Hill" appears twice so should go from 7 to 6
        newframe_last = frame.drop_duplicates(subset=["Last Name"])
        res_last = json.loads(newframe_last.execute_frame_to_string())["result"]
        assert res_last["columns"] == ['First Name', 'Last Name', 'Age', 'Firm/Legal Name']
        assert len(res_last["rows"]) == 6
        last_names = sorted([r["values"][1] for r in res_last["rows"]])
        assert last_names == ['Allen', 'Harris', 'Hill', 'Johnson', 'Roberts', 'Smith']
