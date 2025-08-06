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

from datetime import datetime
from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.language.shared.primitives.date import PyLegendDate
from pylegend.core.language.shared.expression import (
    PyLegendExpressionDateTimeReturn,
)
from pylegend.core.language.shared.literal_expressions import (
    PyLegendDateTimeLiteralExpression,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig


__all__: PyLegendSequence[str] = [
    "PyLegendDateTime"
]


class PyLegendDateTime(PyLegendDate):
    __value: PyLegendExpressionDateTimeReturn

    def __init__(
            self,
            value: PyLegendExpressionDateTimeReturn
    ) -> None:
        super().__init__(value)
        self.__value = value

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return self.__value.to_sql_expression(frame_name_to_base_query_map, config)

    def value(self) -> PyLegendExpressionDateTimeReturn:
        return self.__value

    @staticmethod
    def __convert_to_datetime_expr(
            val: PyLegendUnion[datetime, "PyLegendDateTime"]
    ) -> PyLegendExpressionDateTimeReturn:
        if isinstance(val, datetime):
            return PyLegendDateTimeLiteralExpression(val)
        return val.__value

    @staticmethod
    def validate_param_to_be_datetime(
            param: PyLegendUnion[datetime, "PyLegendDateTime"],
            desc: str
    ) -> None:
        if not isinstance(param, (datetime, PyLegendDateTime)):
            raise TypeError(desc + " should be a datetime.datetime or a DateTime expression (PyLegendDateTime)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))
