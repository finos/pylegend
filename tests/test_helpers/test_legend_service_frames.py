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
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.project_cooridnates import VersionedProjectCoordinates
from pylegend.extensions.tds.legacy_api.frames.legacy_api_legend_service_input_frame import (
    LegacyApiLegendServiceInputFrame
)
from pylegend.extensions.tds.legendql_api.frames.legendql_api_legend_service_input_frame import (
    LegendQLApiLegendServiceInputFrame
)
from pylegend.extensions.tds.pandas_api.frames.pandas_api_legend_service_input_frame import (
    PandasApiLegendServiceInputFrame
)

__all__: PyLegendSequence[str] = [
    "simple_person_service_frame_legacy_api",
    "simple_trade_service_frame_legacy_api",
    "simple_product_service_frame_legacy_api",
    "simple_person_service_frame_pandas_api",
    "simple_trade_service_frame_pandas_api",
    "simple_person_service_frame_legendql_api",
    "simple_trade_service_frame_legendql_api",
    "simple_product_service_frame_legendql_api",
    "simple_relation_person_service_frame_legendql_api",
    "simple_relation_trade_service_frame_legendql_api",
]


def simple_person_service_frame_legacy_api(engine_port: int) -> LegacyApiLegendServiceInputFrame:
    return LegacyApiLegendServiceInputFrame(
        pattern="/simplePersonService",
        project_coordinates=VersionedProjectCoordinates(
            group_id="org.finos.legend.pylegend",
            artifact_id="pylegend-test-models",
            version="0.0.1-SNAPSHOT"
        ),
        legend_client=LegendClient("localhost", engine_port, secure_http=False)
    )


def simple_trade_service_frame_legacy_api(engine_port: int) -> LegacyApiLegendServiceInputFrame:
    return LegacyApiLegendServiceInputFrame(
        pattern="/simpleTradeService",
        project_coordinates=VersionedProjectCoordinates(
            group_id="org.finos.legend.pylegend",
            artifact_id="pylegend-test-models",
            version="0.0.1-SNAPSHOT"
        ),
        legend_client=LegendClient("localhost", engine_port, secure_http=False)
    )


def simple_product_service_frame_legacy_api(engine_port: int) -> LegacyApiLegendServiceInputFrame:
    return LegacyApiLegendServiceInputFrame(
        pattern="/simpleProductService",
        project_coordinates=VersionedProjectCoordinates(
            group_id="org.finos.legend.pylegend",
            artifact_id="pylegend-test-models",
            version="0.0.1-SNAPSHOT"
        ),
        legend_client=LegendClient("localhost", engine_port, secure_http=False)
    )


def simple_person_service_frame_pandas_api(engine_port: int) -> PandasApiLegendServiceInputFrame:
    return PandasApiLegendServiceInputFrame(
        pattern="/simplePersonService",
        project_coordinates=VersionedProjectCoordinates(
            group_id="org.finos.legend.pylegend",
            artifact_id="pylegend-test-models",
            version="0.0.1-SNAPSHOT"
        ),
        legend_client=LegendClient("localhost", engine_port, secure_http=False)
    )


def simple_person_service_frame_legendql_api(engine_port: int) -> LegendQLApiLegendServiceInputFrame:
    return LegendQLApiLegendServiceInputFrame(
        pattern="/simplePersonService",
        project_coordinates=VersionedProjectCoordinates(
            group_id="org.finos.legend.pylegend",
            artifact_id="pylegend-test-models",
            version="0.0.1-SNAPSHOT"
        ),
        legend_client=LegendClient("localhost", engine_port, secure_http=False)
    )


def simple_trade_service_frame_pandas_api(engine_port: int) -> PandasApiLegendServiceInputFrame:
    return PandasApiLegendServiceInputFrame(
        pattern="/simpleTradeService",
        project_coordinates=VersionedProjectCoordinates(
            group_id="org.finos.legend.pylegend",
            artifact_id="pylegend-test-models",
            version="0.0.1-SNAPSHOT"
        ),
        legend_client=LegendClient("localhost", engine_port, secure_http=False)
    )


def simple_trade_service_frame_legendql_api(engine_port: int) -> LegendQLApiLegendServiceInputFrame:
    return LegendQLApiLegendServiceInputFrame(
        pattern="/simpleTradeService",
        project_coordinates=VersionedProjectCoordinates(
            group_id="org.finos.legend.pylegend",
            artifact_id="pylegend-test-models",
            version="0.0.1-SNAPSHOT"
        ),
        legend_client=LegendClient("localhost", engine_port, secure_http=False)
    )


def simple_product_service_frame_legendql_api(engine_port: int) -> LegendQLApiLegendServiceInputFrame:
    return LegendQLApiLegendServiceInputFrame(
        pattern="/simpleProductService",
        project_coordinates=VersionedProjectCoordinates(
            group_id="org.finos.legend.pylegend",
            artifact_id="pylegend-test-models",
            version="0.0.1-SNAPSHOT"
        ),
        legend_client=LegendClient("localhost", engine_port, secure_http=False)
    )


def simple_relation_person_service_frame_legendql_api(engine_port: int) -> LegendQLApiLegendServiceInputFrame:
    return LegendQLApiLegendServiceInputFrame(
        pattern="/simpleRelationPersonService",
        project_coordinates=VersionedProjectCoordinates(
            group_id="org.finos.legend.pylegend",
            artifact_id="pylegend-test-models",
            version="0.0.1-SNAPSHOT"
        ),
        legend_client=LegendClient("localhost", engine_port, secure_http=False)
    )


def simple_relation_trade_service_frame_legendql_api(engine_port: int) -> LegendQLApiLegendServiceInputFrame:
    return LegendQLApiLegendServiceInputFrame(
        pattern="/simpleRelationTradeService",
        project_coordinates=VersionedProjectCoordinates(
            group_id="org.finos.legend.pylegend",
            artifact_id="pylegend-test-models",
            version="0.0.1-SNAPSHOT"
        ),
        legend_client=LegendClient("localhost", engine_port, secure_http=False)
    )


def simple_relation_person_service_frame_pandas_api(engine_port: int) -> PandasApiLegendServiceInputFrame:
    return PandasApiLegendServiceInputFrame(
        pattern="/simpleRelationPersonService",
        project_coordinates=VersionedProjectCoordinates(
            group_id="org.finos.legend.pylegend",
            artifact_id="pylegend-test-models",
            version="0.0.1-SNAPSHOT"
        ),
        legend_client=LegendClient("localhost", engine_port, secure_http=False)
    )


def simple_relation_trade_service_frame_pandas_api(engine_port: int) -> PandasApiLegendServiceInputFrame:
    return PandasApiLegendServiceInputFrame(
        pattern="/simpleRelationTradeService",
        project_coordinates=VersionedProjectCoordinates(
            group_id="org.finos.legend.pylegend",
            artifact_id="pylegend-test-models",
            version="0.0.1-SNAPSHOT"
        ),
        legend_client=LegendClient("localhost", engine_port, secure_http=False)
    )
