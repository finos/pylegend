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
    PyLegendSequence,
)
from pylegend.core.request import LegendClient
from pylegend.core.project_cooridnates import ProjectCoordinates
from pylegend.core.tds.legacy_api.frames.legacy_api_tds_frame import LegacyApiTdsFrame


__all__: PyLegendSequence[str] = [
    "LegacyApiTdsClient",
    "legacy_api_tds_client",
]


class LegacyApiTdsClient:
    __legend_client: LegendClient

    def __init__(
            self,
            legend_client: LegendClient
    ) -> None:
        self.__legend_client = legend_client

    def legend_service_frame(
            self,
            service_pattern: str,
            project_coordinates: ProjectCoordinates
    ) -> LegacyApiTdsFrame:
        from pylegend.extensions.tds.legacy_api.frames.legacy_api_legend_service_input_frame import (
            LegacyApiLegendServiceInputFrame
        )
        return LegacyApiLegendServiceInputFrame(
            pattern=service_pattern,
            project_coordinates=project_coordinates,
            legend_client=self.__legend_client
        )

    def legend_function_frame(
            self,
            function_path: str,
            project_coordinates: ProjectCoordinates
    ) -> LegacyApiTdsFrame:
        from pylegend.extensions.tds.legacy_api.frames.legacy_api_legend_function_input_frame import (
            LegacyApiLegendFunctionInputFrame
        )
        return LegacyApiLegendFunctionInputFrame(
            path=function_path,
            project_coordinates=project_coordinates,
            legend_client=self.__legend_client
        )


def legacy_api_tds_client(
        legend_client: LegendClient
) -> LegacyApiTdsClient:
    return LegacyApiTdsClient(
        legend_client=legend_client
    )
