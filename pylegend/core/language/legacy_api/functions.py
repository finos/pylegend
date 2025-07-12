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
    PyLegendSequence,
)
from pylegend.core.language.legacy_api.primitives.strictdate import LegacyApiStrictDate
from pylegend.core.language.legacy_api.primitives.datetime import LegacyApiDateTime
from pylegend.core.language.shared.operations.date_operation_expressions import (
    PyLegendTodayExpression,
    PyLegendNowExpression,
)


__all__: PyLegendSequence[str] = [
    "today",
    "now",
]


def today() -> LegacyApiStrictDate:
    return LegacyApiStrictDate(PyLegendTodayExpression())


def now() -> LegacyApiDateTime:
    return LegacyApiDateTime(PyLegendNowExpression())
