# Copyright 2023 Goldman Sachs
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

import pylegend
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
import pandas as pd


class TestLegendApiTdsClient:

    def test_legend_api_tds_client(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:

        tds_client = pylegend.LegendApiTdsClient(
            legend_client=pylegend.LegendClient(
                host="localhost",
                port=legend_test_server["engine_port"],
                secure_http=False
            )
        )

        frame = tds_client.legend_service_frame(
            service_pattern="/simplePersonService",
            project_coordinates=pylegend.VersionedProjectCoordinates(
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-test-models",
                version="0.0.1-SNAPSHOT"
            )
        )

        df = frame.execute_frame_to_pandas_df()
        expected = pd.DataFrame(
            columns=[
                "First Name", "Last Name", "Age", "Firm/Legal Name"
            ],
            data=[
                ["Peter", "Smith", 23, "Firm X"],
                ["John", "Johnson", 22, "Firm X"],
                ["John", "Hill", 12, "Firm X"],
                ["Anthony", "Allen", 22, "Firm X"],
                ["Fabrice", "Roberts", 34, "Firm A"],
                ["Oliver", "Hill", 32, "Firm B"],
                ["David", "Harris", 35, "Firm C"]
            ]
        ).astype({
            "Age": "Int64"
        })
        pd.testing.assert_frame_equal(expected, df)

        filtered_frame = frame.filter(lambda x: (x.get_integer("Age") >= 22) & (x["Firm/Legal Name"] == "Firm X"))
        df = filtered_frame.execute_frame_to_pandas_df()
        expected = pd.DataFrame(
            columns=[
                "First Name", "Last Name", "Age", "Firm/Legal Name"
            ],
            data=[
                ["Peter", "Smith", 23, "Firm X"],
                ["John", "Johnson", 22, "Firm X"],
                ["Anthony", "Allen", 22, "Firm X"],
            ]
        ).astype({
            "Age": "Int64"
        })
        pd.testing.assert_frame_equal(expected, df)

        grouped_frame = filtered_frame.group_by(
            ["Age"],
            [pylegend.agg(lambda x: x["First Name"], lambda y: y.join(';'), 'First Names')]  # type: ignore
        )
        df = grouped_frame.execute_frame_to_pandas_df()
        expected = pd.DataFrame(
            columns=[
                "Age", "First Names"
            ],
            data=[
                [22, "John;Anthony"],
                [23, "Peter"],
            ]
        ).astype({
            "Age": "Int64"
        })
        pd.testing.assert_frame_equal(expected, df)
