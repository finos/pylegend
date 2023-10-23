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

import csv
import pathlib
from textwrap import dedent
from pylegend.core.tds.result_handler import ToCsvFileResultHandler
from tests.test_helpers.legend_service_frame import simple_person_service_frame
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)


class TestToCsvFileResultHandler:

    def test_to_csv_file_result_handler(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]],
            tmp_path: pathlib.Path
    ) -> None:
        file = str(tmp_path / "result.csv")
        frame = simple_person_service_frame(legend_test_server["engine_port"])
        frame.execute_frame(ToCsvFileResultHandler(file))

        with open(file, "r") as r:
            res = r.read()

        expected = """
        First Name,Last Name,Age,Firm/Legal Name
        Peter,Smith,23,Firm X
        John,Johnson,22,Firm X
        John,Hill,12,Firm X
        Anthony,Allen,22,Firm X
        Fabrice,Roberts,34,Firm A
        Oliver,Hill,32,Firm B
        David,Harris,35,Firm C
        """
        assert res == dedent(expected)[1:]

    def test_to_csv_file_result_handler_with_custom_csv_writer(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]],
            tmp_path: pathlib.Path
    ) -> None:
        file = str(tmp_path / "result.csv")
        frame = simple_person_service_frame(legend_test_server["engine_port"])
        with open(file, "w", newline="") as f:
            writer = csv.writer(f, delimiter="|", quoting=csv.QUOTE_NONNUMERIC)
            frame.execute_frame(ToCsvFileResultHandler(writer))

        with open(file, "r") as r:
            res = r.read()

        expected = """
        "First Name"|"Last Name"|"Age"|"Firm/Legal Name"
        "Peter"|"Smith"|23|"Firm X"
        "John"|"Johnson"|22|"Firm X"
        "John"|"Hill"|12|"Firm X"
        "Anthony"|"Allen"|22|"Firm X"
        "Fabrice"|"Roberts"|34|"Firm A"
        "Oliver"|"Hill"|32|"Firm B"
        "David"|"Harris"|35|"Firm C"
        """
        assert res == dedent(expected)[1:]
