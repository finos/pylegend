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
from pylegend.extensions.tds.legendql_api.frames.legendql_api_legend_service_input_frame import (
    LegendQLApiLegendServiceInputFrame
)
from pylegend.samples.local_legend_env import get_local_legend_env, NORTHWIND_PROJECT_COORDINATES


__all__: PyLegendSequence[str] = [
    "northwind_orders_frame",
]


def northwind_orders_frame() -> LegendQLApiTdsFrame:
    local_legend_env = get_local_legend_env()
    return LegendQLApiLegendServiceInputFrame(
        pattern="/allOrders",
        project_coordinates=NORTHWIND_PROJECT_COORDINATES,
        legend_client=local_legend_env.legend_client,
    )
