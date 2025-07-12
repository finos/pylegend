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

from datetime import date
from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.language.legacy_api.primitives.date import LegacyApiDate
from pylegend.core.language.shared.expression import (
    PyLegendExpression,
    PyLegendExpressionStrictDateReturn,
)
from pylegend.core.language.shared.literal_expressions import (
    PyLegendStrictDateLiteralExpression,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig


__all__: PyLegendSequence[str] = [
    "LegacyApiStrictDate"
]


class LegacyApiStrictDate(LegacyApiDate):
    __value: PyLegendExpressionStrictDateReturn

    def __init__(
            self,
            value: PyLegendExpressionStrictDateReturn
    ) -> None:
        super().__init__(value)
        self.__value = value

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return self.__value.to_sql_expression(frame_name_to_base_query_map, config)

    def value(self) -> PyLegendExpression:
        return self.__value

    @staticmethod
    def __convert_to_strictdate_expr(
            val: PyLegendUnion[date, "LegacyApiStrictDate"]
    ) -> PyLegendExpressionStrictDateReturn:
        if isinstance(val, date):
            return PyLegendStrictDateLiteralExpression(val)
        return val.__value

    @staticmethod
    def validate_param_to_be_strictdate(
            param: PyLegendUnion[date, "LegacyApiStrictDate"],
            desc: str
    ) -> None:
        if not isinstance(param, (date, LegacyApiStrictDate)):
            raise TypeError(desc + " should be a datetime.date or a StrictDate expression (PyLegendStrictDate)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))
