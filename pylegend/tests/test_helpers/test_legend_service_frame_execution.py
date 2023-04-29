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

import importlib
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.tests.test_helpers.legend_service_frame import LegendServiceFrame
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.tds_column import PyLegendTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig

postgres_ext = 'pylegend.extensions.database.vendors.postgres.postgres_sql_to_string'
importlib.import_module(postgres_ext)


class TestLegendServiceFrameExecution:

    def test_legend_service_frame_execution(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        legend_client = LegendClient("localhost", legend_test_server["engine_port"], False)

        frame = LegendServiceFrame(
            legend_client=legend_client,
            pattern="/simplePersonService",
            group_id="org.finos.legend.pylegend",
            artifact_id="pylegend-test-models",
            version="0.0.1-SNAPSHOT",
            columns=[
                PyLegendTdsColumn("First Name", "String"),
                PyLegendTdsColumn("Last Name", "String"),
                PyLegendTdsColumn("Age", "Integer"),
                PyLegendTdsColumn("Firm/Legal Name", "String"),
            ]
        )

        sql = frame.to_sql_query(FrameToSqlConfig())
        expected = (
            'select\n'
            '    "root".*\n'
            'from\n'
            "    legend_service('/simplePersonService', groupId => 'org.finos.legend.pylegend', "
            "artifactId => 'pylegend-test-models', version => '0.0.1-SNAPSHOT') as \"root\""
        )

        assert sql == expected
