# Copyright 2026 Goldman Sachs
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

"""
Time-of-day primitive type for the PyLegend expression language.

``PyLegendTime`` represents a TIME column value (hour, minute, second,
optional fractional seconds) with no date component.  It maps to the
``Time`` primitive type in Legend's Pure type system, and to the SQL
``TIME`` type in Snowflake, DuckDB, and Databricks.

Instances are produced when a column whose Legend type is ``Time`` is
accessed on a TDS frame.
"""

from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.language.shared.expression import PyLegendExpressionTimeReturn
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig


__all__: PyLegendSequence[str] = [
    "PyLegendTime",
]


class PyLegendTime(PyLegendPrimitive):
    """TIME expression type — time-of-day without a date component.

    Supports equality, null checks, and string conversion (inherited from
    ``PyLegendPrimitive``).  Time-arithmetic and extraction functions
    (e.g. ``hour()``, ``minute()``) can be added in follow-up PRs as
    the corresponding Pure built-ins are exposed.
    """

    __value: PyLegendExpressionTimeReturn

    def __init__(self, value: PyLegendExpressionTimeReturn) -> None:
        self.__value = value

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return self.__value.to_sql_expression(frame_name_to_base_query_map, config)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return self.__value.to_pure_expression(config)

    def value(self) -> PyLegendExpressionTimeReturn:
        return self.__value
