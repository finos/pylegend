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
from tests.test_helpers.test_legend_service_frames import (
    simple_person_service_frame_legacy_api,
    simple_trade_service_frame_legacy_api,
    simple_product_service_frame_legacy_api,
)
from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import PandasDfReadConfig
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
        frame = simple_person_service_frame_legacy_api(legend_test_server["engine_port"])
        df = frame.execute_frame_to_pandas_df()

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

    def test_to_pandas_df_result_handler_rows_per_batch_config(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]],
            tmp_path: pathlib.Path
    ) -> None:
        frame = simple_person_service_frame_legacy_api(legend_test_server["engine_port"])
        df = frame.execute_frame_to_pandas_df(pandas_df_read_config=PandasDfReadConfig(rows_per_batch=1))

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

    def test_to_pandas_df_result_handler_trade_service(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]],
            tmp_path: pathlib.Path
    ) -> None:
        frame = simple_trade_service_frame_legacy_api(legend_test_server["engine_port"])
        df = frame.take(4).execute_frame_to_pandas_df()

        expected = pd.DataFrame(
            columns=[
                'Id',
                'Date',
                'Quantity',
                'Settlement Date Time',
                'Product/Name',
                'Account/Name'
            ],
            data=[
                [1,
                 '2014-12-01',
                 25.0,
                 '2014-12-02T21:00:00.000000000+0000',
                 'Firm X',
                 'Account 1'],
                [2,
                 '2014-12-01',
                 320.0,
                 '2014-12-02T21:00:00.000000000+0000',
                 'Firm X',
                 'Account 2'],
                [3,
                 '2014-12-01',
                 11.0,
                 '2014-12-02T21:00:00.000000000+0000',
                 'Firm A',
                 'Account 1'],
                [4,
                 '2014-12-02',
                 23.0,
                 '2014-12-03T21:00:00.000000000+0000',
                 'Firm A',
                 'Account 2'],
            ]
        ).astype({
            "Id": "Int64",
            "Quantity": "Float64"
        })
        expected['Date'] = pd.to_datetime(expected['Date'])
        expected['Settlement Date Time'] = pd.to_datetime(expected['Settlement Date Time'])
        pd.testing.assert_frame_equal(expected, df)

    def test_to_pandas_df_result_handler_product_service(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]],
            tmp_path: pathlib.Path
    ) -> None:
        frame = simple_product_service_frame_legacy_api(legend_test_server["engine_port"])
        df = frame.execute_frame_to_pandas_df(pandas_df_read_config=PandasDfReadConfig(rows_per_batch=1))

        expected = pd.DataFrame(
            columns=['Name', 'Synonyms/Name', 'Synonyms/Type'],
            data=[
                ['Firm X', 'CUSIP1', 'CUSIP'],
                ['Firm X', 'ISIN1', 'ISIN'],
                ['Firm A', 'CUSIP2', 'CUSIP'],
                ['Firm A', 'ISIN2', 'ISIN'],
                ['Firm C', 'CUSIP3', 'CUSIP'],
                ['Firm C', 'ISIN3', 'ISIN'],
                ['Firm D', None, None]]
        )
        pd.testing.assert_frame_equal(expected, df)
