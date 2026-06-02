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

import os
import json
import requests
import pytest
from pylegend._typing import (
    PyLegendDict,
    PyLegendSequence,
    PyLegendUnion,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.project_cooridnates import VersionedProjectCoordinates
from pylegend.extensions.tds.abstract.legend_service_input_frame import LegendServiceInputFrameAbstract
from pylegend.core.tds.abstract.frames.base_tds_frame import BaseTdsFrame


_TEST_COORDINATES = VersionedProjectCoordinates(
    "org.finos.legend.pylegend", "pylegend-test-models", "0.0.1-SNAPSHOT"
)


class _PureOnlyServiceFrame(LegendServiceInputFrameAbstract):
    """Minimal concrete subclass of LegendServiceInputFrameAbstract for unit testing.

    Only exists to allow instantiation of the abstract class in tests.
    NOT exported — use only within this test module.
    """

    def __init__(self, pattern: str) -> None:
        super().__init__(pattern=pattern, project_coordinates=_TEST_COORDINATES)

    def columns(self) -> PyLegendSequence[TdsColumn]:
        return []

    def get_all_tds_frames(self) -> PyLegendSequence[BaseTdsFrame]:
        return [self]

    def to_pure_query(self, config: FrameToPureConfig = FrameToPureConfig()) -> str:
        return self.to_pure(config)


class TestLegendServiceInputFramePure:

    def test_to_pure_person_service_unit(self) -> None:
        frame = _PureOnlyServiceFrame(pattern="/simplePersonService")
        result = frame.to_pure(FrameToPureConfig())
        assert result == "|pylegend::test::SimplePersonService.all()"

    def test_to_pure_trade_service_unit(self) -> None:
        frame = _PureOnlyServiceFrame(pattern="/simpleTradeService")
        result = frame.to_pure(FrameToPureConfig())
        assert result == "|pylegend::test::SimpleTradeService.all()"

    @pytest.mark.skipif(
        os.environ.get("JAVA_HOME") is None,
        reason="JAVA_HOME not set — Legend test server unavailable"
    )
    def test_to_pure_person_service_grammar_round_trip(
        self,
        legend_test_server: PyLegendDict[str, PyLegendUnion[int, str]]
    ) -> None:
        frame = _PureOnlyServiceFrame(pattern="/simplePersonService")
        pure_str = frame.to_pure(FrameToPureConfig())
        engine_port = legend_test_server["engine_port"]
        response = requests.post(
            f"http://localhost:{engine_port}/api/pure/v1/grammar/grammarToJson/lambda",
            data=pure_str,
            headers={"Content-Type": "text/plain"},
        )
        assert response.status_code == 200
        parsed = json.loads(response.text)
        assert parsed["_type"] == "lambda"
