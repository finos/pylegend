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
    simple_trade_service_frame_pandas_api
)



class TestGroupbyFunction:

    # @pytest.fixture(autouse=True)
    # def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
    #     self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_groupby_simple_query_generation(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"),
                   PrimitiveTdsColumn.date_column("col2"),
                   PrimitiveTdsColumn.integer_column("col3")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="col1").aggregate({"col2": "min", "col3": [np.sum]})
        expected = """\
                    SELECT
                        "root".col1 AS "col1",
                        MIN("root".col2) AS "col2",
                        SUM("root".col3) AS "col3"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".col1
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)[:-1]
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[col2:{r | $r.col2}:{c | $c->min()}, col3:{r | $r.col3}:{c | $c->sum()}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->groupBy(~[col1], ~[col2:{r | $r.col2}:{c | $c->min()}, col3:{r | $r.col3}:{c | $c->sum()}])"
        )

    def test_groupby_column_selection_for_aggregation(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"),
                   PrimitiveTdsColumn.date_column("col2"),
                   PrimitiveTdsColumn.integer_column("col3")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="col1")[["col1", "col2", "col3"]].aggregate("min")
        expected = """\
                    SELECT
                        "root".col1 AS "col1",
                        MIN("root".col2) AS "col2",
                        SUM("root".col3) AS "col3"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".col1
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)[:-1]
        # assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
        #     """\
        #     #Table(test_schema.test_table)#
        #       ->groupBy(
        #         ~[col1],
        #         ~[col2:{r | $r.col2}:{c | $c->min()}, col3:{r | $r.col3}:{c | $c->sum()}]
        #       )"""
        # )
        # assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
        #     "#Table(test_schema.test_table)#"
        #     "->groupBy(~[col1], ~[col2:{r | $r.col2}:{c | $c->min()}, col3:{r | $r.col3}:{c | $c->sum()}])"
        # )
