# Copyright 2025 Goldman Sachs
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

from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient


class TestDescribeFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_describe_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # percentiles
        with pytest.raises(TypeError) as t:
            frame.describe(percentiles='not a list')  # type: ignore
        assert t.value.args[0] == "percentiles must be a list, tuple, or set of numbers. Got class<str>"

        with pytest.raises(ValueError) as v:
            frame.describe(percentiles=[-0.1, 1.1])
        assert v.value.args[0] == "percentiles must all be in the interval [0, 1]."

        # include, exclude
        with pytest.raises(TypeError) as t:
            frame.describe(include=123)  # type: ignore
        assert t.value.args[0] == "data type 123 not understood"

        with pytest.raises(TypeError) as t:
            frame.describe(exclude='str')
        assert t.value.args[0] == "data type str not understood"

        with pytest.raises(ValueError) as v:
            frame.describe(include=float)  # type: ignore
        assert v.value.args[0] == "No objects to concatenate"

    # flake8: noqa
    def test_e2e_describe_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:  # noqa
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        res = frame.describe(include='all')
        expected = (
            '        First Name  Last Name  Age        Firm/Legal Name\n'
 ' count           7          7   7.000000                7\n'
 'unique           6          6        NaN                4\n'
 '   top        John       Hill        NaN           Firm X\n'
 '  freq           2          2        NaN                4\n'
 '  mean         NaN        NaN  25.714286              NaN\n'
 '   std         NaN        NaN   8.340949              NaN\n'
 '   min         NaN        NaN  12.000000              NaN\n'
 '   25%         NaN        NaN  22.000000              NaN\n'
 '   50%         NaN        NaN  23.000000              NaN\n'
 '   75%         NaN        NaN  33.000000              NaN\n'
 '   max         NaN        NaN  35.000000              NaN\n'
        )
        assert res == expected

        res = frame.describe()
        exp_num = (
            '       Age      \n'
 'count   7.000000\n'
 ' mean  25.714286\n'
 '  std   8.340949\n'
 '  min  12.000000\n'
 '  25%  22.000000\n'
 '  50%  23.000000\n'
 '  75%  33.000000\n'
 '  max  35.000000\n'
        )
        assert res == exp_num

        res = frame.describe(percentiles=(0.1, 0.9))
        exp_per = (
            '       Age      \n'
 'count   7.000000\n'
 ' mean  25.714286\n'
 '  std   8.340949\n'
 '  min  12.000000\n'
 '  10%  18.000000\n'
 '  50%  23.000000\n'
 '  90%  34.400000\n'
 '  max  35.000000\n'
        )
        assert res == exp_per

        # Include only object columns
        res = frame.describe(include=['O'])
        exp_exc_num = (
            '        First Name  Last Name  Firm/Legal Name\n'
            ' count           7          7                7\n'
            'unique           6          6                4\n'
            '   top        John       Hill           Firm X\n'
            '  freq           2          2                4\n'
        )
        assert res == exp_exc_num

        # Exclude integer columns
        res = frame.describe(exclude=['integer'])
        assert res == exp_exc_num

        # Other types
        res = frame.describe(include=[int])  # type: ignore
        assert res == exp_num

        res = frame.describe(include=object)  # type: ignore
        assert res == exp_exc_num
