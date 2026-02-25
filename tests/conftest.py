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

import pytest
import datetime
import logging

from pylegend._typing import (
    PyLegendGenerator,
    PyLegendDict,
    PyLegendUnion
)
from pylegend.core.project_cooridnates import VersionedProjectCoordinates
from pylegend.samples.local_legend_env import get_local_legend_env
import tests

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def legend_test_server() -> PyLegendGenerator[PyLegendDict[str, PyLegendUnion[int, ]], None, None]:
    LOGGER.info("Start Legend Test Server ....")
    start = datetime.datetime.now()

    relative_path = tests.__file__.replace("\\", "/")[0:tests.__file__.replace("\\", "/").rindex("/")]

    test_models_coords = VersionedProjectCoordinates(
        group_id="org.finos.legend.pylegend",
        artifact_id="pylegend-test-models",
        version="0.0.1-SNAPSHOT"
    )

    metadata_path = (relative_path +
                     "/resources/legend/metadata/org.finos.legend.pylegend_pylegend-test-models_0.0.1-SNAPSHOT.json")
    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata_content = f.read()

    local_env = get_local_legend_env(
        metadata_resources={
            test_models_coords: metadata_content
        }
    )

    LOGGER.info(f"Legend Test Server started in {(datetime.datetime.now() - start).seconds} seconds ...")

    yield {"engine_port": local_env.engine_port}

    LOGGER.info("Terminate Legend Test Server ....")
    local_env.stop()
    LOGGER.info("Legend Test Server terminated ....")
