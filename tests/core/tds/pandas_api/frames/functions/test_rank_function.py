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

import json

import pytest
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from tests.test_helpers.test_legend_service_frames import simple_relation_person_service_frame_pandas_api


class TestRankFunctionErrors:
    def test_rank_error_invaild_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(
            ["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rank(axis=1)

        expected_msg = "The 'axis' parameter of the rank function must be 0 or 'index', but got: axis=1"
        assert v.value.args[0] == expected_msg

    def test_rank_error_invalid_method(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame = PandasApiTableSpecInputFrame(
            ["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rank(method="average")

        expected_msg = "The 'method' parameter of the rank function must be one of ['dense', 'first', 'min'], but got: method='average'"  # noqa: E501
        assert v.value.args[0] == expected_msg

    def test_rank_error_pct_with_invalid_method(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame = PandasApiTableSpecInputFrame(
            ["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rank(pct=True, method='dense')

        expected_msg = "The 'pct=True' parameter of the rank function is only supported with method='min', but got: method='dense'."  # noqa: E501
        assert v.value.args[0] == expected_msg

    def test_rank_error_invalid_na_option(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame = PandasApiTableSpecInputFrame(
            ["test_schema", "test_table"], columns)

        invalid_na = "top"
        with pytest.raises(NotImplementedError) as v:
            frame.rank(na_option=invalid_na)

        valid_na_options = {'keep', 'bottom'}
        expected_msg = f"The 'na_option' parameter of the rank function must be one of {valid_na_options!r}, but got: na_option={invalid_na!r}"  # noqa: E501
        assert v.value.args[0] == expected_msg


class TestRankFunctionEndtoEnd:
    def test_e2e_rank_no_arguments(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.rank(na_option='bottom')
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": [7, 7, 4, 4]},  # Peter, Smith, 23, Firm X
                {"values": [4, 5, 2, 4]},  # John, Johnson, 22, Firm X
                {"values": [4, 3, 1, 4]},  # John, Hill, 12, Firm X
                {"values": [1, 1, 2, 4]},  # Anthony, Allen, 22, Firm X
                {"values": [3, 6, 6, 1]},  # Fabrice, Roberts, 34, Firm A
                {"values": [6, 3, 5, 2]},  # Oliver, Hill, 32, Firm B
                {"values": [2, 2, 7, 3]},  # David, Harris, 35, Firm C
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_dense_rank(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.rank(method='dense')
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": [6, 6, 3, 4]},  # Peter, Smith, 23, Firm X
                {"values": [4, 4, 2, 4]},  # John, Johnson, 22, Firm X
                {"values": [4, 3, 1, 4]},  # John, Hill, 12, Firm X
                {"values": [1, 1, 2, 4]},  # Anthony, Allen, 22, Firm X
                {"values": [3, 5, 5, 1]},  # Fabrice, Roberts, 34, Firm A
                {"values": [5, 3, 4, 2]},  # Oliver, Hill, 32, Firm B
                {"values": [2, 2, 6, 3]},  # David, Harris, 35, Firm C
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_pct_rank(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.rank(method='min', pct=True, ascending=False, na_option='bottom')
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                # Peter (0.0), Smith (0.0), 23 (3/6=0.5), Firm X (0.0)
                {"values": [0.0, 0.0, 0.5, 0.0]},
                # John (2/6=0.33..), Johnson (2/6=0.33..), 22 (4/6=0.66..), Firm X (0.0)
                {"values": [0.3333333333333333, 0.3333333333333333, 0.6666666666666666, 0.0]},
                # John (0.33..), Hill (3/6=0.5), 12 (6/6=1.0), Firm X (0.0)
                {"values": [0.3333333333333333, 0.5, 1.0, 0.0]},
                # Anthony (6/6=1.0), Allen (6/6=1.0), 22 (4/6=0.66..), Firm X (0.0)
                {"values": [1.0, 1.0, 0.6666666666666666, 0.0]},
                # Fabrice (4/6=0.66..), Roberts (1/6=0.16..), 34 (1/6=0.16..), Firm A (6/6=1.0)
                {"values": [0.6666666666666666, 0.16666666666666666, 0.16666666666666666, 1.0]},
                # Oliver (1/6=0.16..), Hill (3/6=0.5), 32 (2/6=0.33..), Firm B (5/6=0.83..)
                {"values": [0.16666666666666666, 0.5, 0.3333333333333333, 0.8333333333333334]},
                # David (5/6=0.83..), Harris (5/6=0.83..), 35 (0.0), Firm C (4/6=0.66..)
                {"values": [0.8333333333333334, 0.8333333333333334, 0.0, 0.6666666666666666]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_groupby_no_selection(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby("Firm/Legal Name").rank(na_option='bottom')
        expected = {
            "columns": ["First Name", "Last Name", "Age"],
            "rows": [
                {"values": [4, 4, 4]},  # Peter, Smith, 23 (Firm X)
                {"values": [2, 3, 2]},  # John, Johnson, 22 (Firm X)
                {"values": [2, 2, 1]},  # John, Hill, 12 (Firm X)
                {"values": [1, 1, 2]},  # Anthony, Allen, 22 (Firm X)
                {"values": [1, 1, 1]},  # Fabrice, Roberts, 34 (Firm A)
                {"values": [1, 1, 1]},  # Oliver, Hill, 32 (Firm B)
                {"values": [1, 1, 1]},  # David, Harris, 35 (Firm C)
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_groupby_with_selection(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.groupby("Firm/Legal Name")[["Age", "Last Name"]].rank(na_option='bottom')
        expected = {
            "columns": ["Age", "Last Name"],
            "rows": [
                {"values": [4, 4]},  # Peter, Smith, 23 (Firm X)
                {"values": [2, 3]},  # John, Johnson, 22 (Firm X)
                {"values": [1, 2]},  # John, Hill, 12 (Firm X)
                {"values": [2, 1]},  # Anthony, Allen, 22 (Firm X)
                {"values": [1, 1]},  # Fabrice, Roberts, 34 (Firm A)
                {"values": [1, 1]},  # Oliver, Hill, 32 (Firm B)
                {"values": [1, 1]},  # David, Harris, 35 (Firm C)
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
