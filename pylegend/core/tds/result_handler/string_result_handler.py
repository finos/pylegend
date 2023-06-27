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

from pylegend._typing import (
    PyLegendIterator,
    PyLegendSequence,
)
from pylegend.core.tds.result_handler.result_handler import ResultHandler

__all__: PyLegendSequence[str] = [
    "StringResultHandler"
]


class StringResultHandler(ResultHandler[str]):
    def handle_result(self, result: PyLegendIterator[bytes]) -> str:
        return b"".join(result).decode("utf-8")
