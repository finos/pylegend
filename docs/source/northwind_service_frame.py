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


from pylegend._typing import PyLegendSequence
from pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame import LegendQLApiTdsFrame
from legendql_api_local_tds_client import legendql_api_local_tds_client


__all__: PyLegendSequence[str] = [
    "northwind_service_frame",
]


def northwind_service_frame() -> LegendQLApiTdsFrame:
    tds_client = legendql_api_local_tds_client()
    frame = tds_client.legend_service_frame(
        service_pattern="/allOrders",
        group_id="org.finos.legend.pylegend",
        artifact_id="pylegend-northwind-models",
        version="0.0.1-SNAPSHOT"
    )
    return frame

