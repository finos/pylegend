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

from pylegend._typing import (
    PyLegendSequence
)
from pylegend.core.tds.legacy_api.frames.legacy_api_input_tds_frame import LegacyApiExecutableInputTdsFrame
from pylegend.core.project_cooridnates import ProjectCoordinates
from pylegend.core.request.legend_client import LegendClient
from pylegend.extensions.tds.abstract.legend_service_input_frame import LegendServiceInputFrameAbstract


__all__: PyLegendSequence[str] = [
    "LegacyApiLegendServiceInputFrame"
]


class LegacyApiLegendServiceInputFrame(LegendServiceInputFrameAbstract, LegacyApiExecutableInputTdsFrame):

    def __init__(
            self,
            pattern: str,
            project_coordinates: ProjectCoordinates,
            legend_client: LegendClient,
    ) -> None:
        LegendServiceInputFrameAbstract.__init__(self, pattern=pattern, project_coordinates=project_coordinates)
        LegacyApiExecutableInputTdsFrame.__init__(
            self,
            legend_client=legend_client,
            columns=legend_client.get_sql_string_schema(self.to_sql_query())
        )
        LegacyApiLegendServiceInputFrame.set_initialized(self, True)

    def __str__(self) -> str:
        return f"LegacyApiLegendServiceInputFrame({'.'.join(self.get_pattern())})"
