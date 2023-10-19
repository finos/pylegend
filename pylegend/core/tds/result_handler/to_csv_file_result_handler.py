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
import ijson  # type: ignore
from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
    TYPE_CHECKING
)
if TYPE_CHECKING:
    from pylegend.core.tds.tds_frame import PyLegendTdsFrame  # pragma: no cover
from pylegend.core.tds.result_handler.result_handler import ResultHandler
from pylegend.core.request.response_reader import ResponseReader

__all__: PyLegendSequence[str] = [
    "ToCsvFileResultHandler"
]


class ToCsvFileResultHandler(ResultHandler[None]):

    def __init__(self, file_or_writer, parse_float_as_decimal: bool = False) -> None:  # type: ignore
        self.__file: PyLegendOptional[str] = file_or_writer if isinstance(file_or_writer, str) else None
        self.__csv_writer = None if isinstance(file_or_writer, str) else file_or_writer
        self.__parse_float_as_decimal = parse_float_as_decimal

    def handle_result(self, frame: "PyLegendTdsFrame", result: ResponseReader) -> None:
        if self.__file is None:
            self.__write_result(self.__csv_writer, frame, result, self.__parse_float_as_decimal)
        else:
            with open(self.__file, 'w', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                self.__write_result(csv_writer, frame, result, self.__parse_float_as_decimal)

    @staticmethod
    def __write_result(  # type: ignore
            csv_writer,
            frame,
            result,
            parse_as_decimal
    ) -> None:
        csv_writer.writerow([col.get_name() for col in frame.columns()])
        for next_row in ijson.items(result, "result.rows.item.values", use_float=not parse_as_decimal):
            csv_writer.writerow(next_row)
        return
