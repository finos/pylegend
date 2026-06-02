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

import pylegend
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.tds.tds_frame import FrameToPureConfig


class TestLegendQLApiTdsClient:

    def test_legendql_api_tds_client_pure_query(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:

        tds_client = pylegend.LegendQLApiTdsClient(
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

        # Verify the Pure query string is generated correctly
        pure_query = frame.to_pure_query(FrameToPureConfig(pretty=False))
        assert "simplePersonService" in pure_query

        # Verify filter operation composes a valid Pure query
        filtered_frame = frame.filter(lambda x: (x.get_integer("Age") >= 22) & (x["Firm/Legal Name"] == "Firm X"))
        filtered_pure_query = filtered_frame.to_pure_query(FrameToPureConfig(pretty=False))
        assert "filter" in filtered_pure_query

        # Verify group_by composes a valid Pure query
        grouped_frame = filtered_frame.group_by(
            ["Age"],
            ("First Names", lambda r: r["First Name"], lambda c: c.join(";"))  # type: ignore
        )
        grouped_pure_query = grouped_frame.to_pure_query(FrameToPureConfig(pretty=False))
        assert "groupBy" in grouped_pure_query
