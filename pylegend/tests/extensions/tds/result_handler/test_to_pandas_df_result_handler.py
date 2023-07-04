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

import pathlib
import pandas as pd
from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
from pylegend.tests.test_helpers.legend_service_frame import simple_person_service_frame
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)


class TestToPandasDfResultHandler:

    def test_to_pandas_df_result_handler(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]],
            tmp_path: pathlib.Path
    ) -> None:
        frame = simple_person_service_frame(legend_test_server["engine_port"])
        df = frame.execute_frame(ToPandasDfResultHandler())

        expected = pd.DataFrame(
            columns=[
                "First Name", "Last Name", "Age", "Firm/Legal Name"
            ],
            data=[
                ["Peter", "Smith", 23, "Firm X"],
                ["John", "Johnson", 22, "Firm X"],
                ["John", "Hill", 12, "Firm X"],
                ["Anthony", "Allen", 22, "Firm X"],
                ["Fabrice", "Roberts", 34, "Firm A"],
                ["Oliver", "Hill", 32, "Firm B"],
                ["David", "Harris", 35, "Firm C"]
            ]
        ).astype({
            "Age": "Int64"
        })
        pd.testing.assert_frame_equal(expected, df)
