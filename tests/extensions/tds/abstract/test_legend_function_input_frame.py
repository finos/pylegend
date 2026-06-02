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
from pylegend.extensions.tds.abstract.legend_function_input_frame import LegendFunctionInputFrameAbstract
from pylegend.core.tds.abstract.frames.base_tds_frame import BaseTdsFrame


_TEST_COORDINATES = VersionedProjectCoordinates(
    "org.finos.legend.pylegend", "pylegend-test-models", "0.0.1-SNAPSHOT"
)

_SIMPLE_PERSON_FUNCTION_PATH = "pylegend::test::function::SimplePersonFunction__TabularDataSet_1_"


class _PureOnlyFunctionFrame(LegendFunctionInputFrameAbstract):
    """Minimal concrete subclass of LegendFunctionInputFrameAbstract for unit testing.

    Only exists to allow instantiation of the abstract class in tests.
    NOT exported — use only within this test module.
    """

    def __init__(self, path: str) -> None:
        super().__init__(path=path, project_coordinates=_TEST_COORDINATES)

    def columns(self) -> PyLegendSequence[TdsColumn]:
        return []

    def get_all_tds_frames(self) -> PyLegendSequence[BaseTdsFrame]:
        return [self]

    def to_pure_query(self, config: FrameToPureConfig = FrameToPureConfig()) -> str:
        return self.to_pure(config)


class TestLegendFunctionInputFramePure:

    def test_to_pure_function_unit(self) -> None:
        frame = _PureOnlyFunctionFrame(path=_SIMPLE_PERSON_FUNCTION_PATH)
        result = frame.to_pure(FrameToPureConfig())
        assert result == f"|{_SIMPLE_PERSON_FUNCTION_PATH}()"

    @pytest.mark.skipif(
        os.environ.get("JAVA_HOME") is None,
        reason="JAVA_HOME not set — Legend test server unavailable"
    )
    def test_to_pure_function_grammar_round_trip(
        self,
        legend_test_server: PyLegendDict[str, PyLegendUnion[int, str]]
    ) -> None:
        frame = _PureOnlyFunctionFrame(path=_SIMPLE_PERSON_FUNCTION_PATH)
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
