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
from textwrap import dedent
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)


class TestAssignFunction:

    def test_assign(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.assign(sumColumn=lambda x: x.get_integer("col1") + x.get_integer("col2"))

        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                ("root".col1 + "root".col2) AS "sumColumn"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_e2e_assign_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.assign(fullName=lambda x: x.get_string("First Name") + " " + x.get_string("Last Name"))
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name', 'fullName'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X', 'Peter Smith']},
                             {'values': ['John', 'Johnson', 22, 'Firm X', 'John Johnson']},
                             {'values': ['John', 'Hill', 12, 'Firm X', 'John Hill']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X', 'Anthony Allen']},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A', 'Fabrice Roberts']},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B', 'Oliver Hill']},
                             {'values': ['David', 'Harris', 35, 'Firm C', 'David Harris']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
