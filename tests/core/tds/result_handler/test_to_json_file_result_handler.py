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

import json
import pathlib
from pylegend.core.tds.result_handler import ToJsonFileResultHandler
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_legacy_api
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)


class TestToJsonFileResultHandler:

    def test_to_json_file_result_handler(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]],
            tmp_path: pathlib.Path
    ) -> None:
        file = str(tmp_path / "result.json")
        frame = simple_person_service_frame_legacy_api(legend_test_server["engine_port"])
        frame.execute_frame(ToJsonFileResultHandler(file))

        with open(file, "r") as r:
            res = r.read()

        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X']},
                             {'values': ['John', 'Johnson', 22, 'Firm X']},
                             {'values': ['John', 'Hill', 12, 'Firm X']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X']},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A']},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B']},
                             {'values': ['David', 'Harris', 35, 'Firm C']}]}
        assert json.loads(res)["result"] == expected
