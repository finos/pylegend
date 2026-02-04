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


import os
from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.project_cooridnates import VersionedProjectCoordinates
from pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame import LegendQLApiTdsFrame


__all__: PyLegendSequence[str] = [
    "LegendQLApiLocalTdsClient",
    "legendql_api_local_tds_client",
]


class LegendQLApiLocalTdsClient:

    def __init__(
            self,
            host: str = "localhost",
            port: PyLegendOptional[int] = None,
            secure_http: bool = False
    ) -> None:
        if port is None:
            port_str = os.environ.get('PYLEGEND_DOC_GEN_ENGINE_PORT')
            if port_str:
                port = int(port_str)
            else:
                raise ValueError(
                    "Port must be provided either as an argument or via "
                    "PYLEGEND_DOC_GEN_ENGINE_PORT environment variable"
                )

        self.__legend_client = LegendClient(host, port, secure_http=secure_http)

    def legend_service_frame(
            self,
            service_pattern: str,
            group_id: str,
            artifact_id: str,
            version: str
    ) -> LegendQLApiTdsFrame:
        from pylegend.extensions.tds.legendql_api.frames.legendql_api_legend_service_input_frame import (
            LegendQLApiLegendServiceInputFrame
        )

        project_coordinates = VersionedProjectCoordinates(
            group_id=group_id,
            artifact_id=artifact_id,
            version=version
        )
        return LegendQLApiLegendServiceInputFrame(
            pattern=service_pattern,
            project_coordinates=project_coordinates,
            legend_client=self.__legend_client
        )

    def legend_function_frame(
            self,
            function_path: str,
            group_id: str,
            artifact_id: str,
            version: str
    ) -> LegendQLApiTdsFrame:
        from pylegend.extensions.tds.legendql_api.frames.legendql_api_legend_function_input_frame import (
            LegendQLApiLegendFunctionInputFrame
        )

        project_coordinates = VersionedProjectCoordinates(
            group_id=group_id,
            artifact_id=artifact_id,
            version=version
        )
        return LegendQLApiLegendFunctionInputFrame(
            path=function_path,
            project_coordinates=project_coordinates,
            legend_client=self.__legend_client
        )

def legendql_api_local_tds_client(
        host: str = "localhost",
        port: PyLegendOptional[int] = None,
        secure_http: bool = False
) -> LegendQLApiLocalTdsClient:
    return LegendQLApiLocalTdsClient(
        host=host,
        port=port,
        secure_http=secure_http
    )
