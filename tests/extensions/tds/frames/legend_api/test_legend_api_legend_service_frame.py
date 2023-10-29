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
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from tests.test_helpers.test_legend_service_frames import (
    simple_person_service_frame,
    simple_trade_service_frame,
    simple_product_service_frame,
)


class TestLegendApiLegendServiceFrame:

    def test_legend_api_legend_service_frame_sql_gen(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = simple_person_service_frame(legend_test_server["engine_port"])
        sql = frame.to_sql_query(FrameToSqlConfig())

        expected = '''\
        SELECT
            "root"."First Name" AS "First Name",
            "root"."Last Name" AS "Last Name",
            "root"."Age" AS "Age",
            "root"."Firm/Legal Name" AS "Firm/Legal Name"
        FROM
            service(
                pattern => '/simplePersonService',
                coordinates => 'org.finos.legend.pylegend:pylegend-test-models:0.0.1-SNAPSHOT'
            ) AS "root"'''

        assert sql == dedent(expected)

    def test_legend_api_legend_person_service_frame_execution(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = simple_person_service_frame(legend_test_server["engine_port"])
        res = frame.execute_frame_to_string()
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X']},
                             {'values': ['John', 'Johnson', 22, 'Firm X']},
                             {'values': ['John', 'Hill', 12, 'Firm X']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X']},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A']},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B']},
                             {'values': ['David', 'Harris', 35, 'Firm C']}]}
        assert json.loads(res)["result"] == expected

    def test_legend_api_legend_trade_service_frame_execution(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = simple_trade_service_frame(legend_test_server["engine_port"])
        res = frame.execute_frame_to_string()
        expected = {'columns': ['Id',
                                'Date',
                                'Quantity',
                                'Settlement Date Time',
                                'Product/Name',
                                'Account/Name'],
                    'rows': [{'values': [1,
                                         '2014-12-01',
                                         25.0,
                                         '2014-12-02T21:00:00.000000000+0000',
                                         'Firm X',
                                         'Account 1']},
                             {'values': [2,
                                         '2014-12-01',
                                         320.0,
                                         '2014-12-02T21:00:00.000000000+0000',
                                         'Firm X',
                                         'Account 2']},
                             {'values': [3,
                                         '2014-12-01',
                                         11.0,
                                         '2014-12-02T21:00:00.000000000+0000',
                                         'Firm A',
                                         'Account 1']},
                             {'values': [4,
                                         '2014-12-02',
                                         23.0,
                                         '2014-12-03T21:00:00.000000000+0000',
                                         'Firm A',
                                         'Account 2']},
                             {'values': [5,
                                         '2014-12-02',
                                         32.0,
                                         '2014-12-03T21:00:00.000000000+0000',
                                         'Firm A',
                                         'Account 1']},
                             {'values': [6,
                                         '2014-12-03',
                                         27.0,
                                         '2014-12-04T21:00:00.000000000+0000',
                                         'Firm C',
                                         'Account 1']},
                             {'values': [7,
                                         '2014-12-03',
                                         44.0,
                                         '2014-12-04T15:22:23.123456789+0000',
                                         'Firm C',
                                         'Account 1']},
                             {'values': [8,
                                         '2014-12-04',
                                         22.0,
                                         '2014-12-05T21:00:00.000000000+0000',
                                         'Firm C',
                                         'Account 2']},
                             {'values': [9,
                                         '2014-12-04',
                                         45.0,
                                         '2014-12-05T21:00:00.000000000+0000',
                                         'Firm C',
                                         'Account 2']},
                             {'values': [10,
                                         '2014-12-04',
                                         38.0,
                                         None,
                                         'Firm C',
                                         'Account 2']},
                             {'values': [11,
                                         '2014-12-05',
                                         5.0,
                                         None,
                                         None,
                                         None]}]}
        assert json.loads(res)["result"] == expected

    def test_legend_api_legend_product_service_frame_execution(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = simple_product_service_frame(legend_test_server["engine_port"])
        res = frame.execute_frame_to_string()
        expected = {'columns': ['Name', 'Synonyms/Name', 'Synonyms/Type'],
                    'rows': [{'values': ['Firm X', 'CUSIP1', 'CUSIP']},
                             {'values': ['Firm X', 'ISIN1', 'ISIN']},
                             {'values': ['Firm A', 'CUSIP2', 'CUSIP']},
                             {'values': ['Firm A', 'ISIN2', 'ISIN']},
                             {'values': ['Firm C', 'CUSIP3', 'CUSIP']},
                             {'values': ['Firm C', 'ISIN3', 'ISIN']},
                             {'values': ['Firm D', None, None]}]}
        assert json.loads(res)["result"] == expected
