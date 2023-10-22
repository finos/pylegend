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


from abc import ABCMeta, abstractmethod
from datetime import date, datetime
from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
    PyLegendUnion,
    TYPE_CHECKING,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.language.expression import PyLegendExpression
from pylegend.core.language.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.language.operations.primitive_operation_expressions import (
    PyLegendPrimitiveEqualsExpression,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
if TYPE_CHECKING:
    from pylegend.core.language.primitives.boolean import PyLegendBoolean

__all__: PyLegendSequence[str] = [
    "PyLegendPrimitive",
    "PyLegendPrimitiveOrPythonPrimitive",
]


class PyLegendPrimitive(metaclass=ABCMeta):

    @abstractmethod
    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        pass

    def __eq__(  # type: ignore
            self,
            other: "PyLegendUnion[int, float, bool, str, date, datetime, PyLegendPrimitive]"
    ) -> "PyLegendBoolean":
        PyLegendPrimitive.__validate_param_to_be_primitive(other, "Equals (==) parameter")

        if isinstance(other, (int, float, bool, str, date, datetime)):
            other_op = convert_literal_to_literal_expression(other)
        else:
            other_op = other.value()

        from pylegend.core.language.primitives.boolean import PyLegendBoolean
        return PyLegendBoolean(PyLegendPrimitiveEqualsExpression(self.value(), other_op))

    @staticmethod
    def __validate_param_to_be_primitive(
            param: "PyLegendUnion[int, float, bool, str, date, datetime, PyLegendPrimitive]",
            desc: str
    ) -> None:
        if not isinstance(param, (int, float, bool, str, date, datetime, PyLegendPrimitive)):
            raise TypeError(
                desc + " should be a int/float/bool/str/datetime.date/datetime.datetime or a primitive expression."
                       " Got value " + str(param) + " of type: " + str(type(param)))

    @abstractmethod
    def value(self) -> PyLegendExpression:
        pass


PyLegendPrimitiveOrPythonPrimitive = PyLegendUnion[int, float, str, bool, date, datetime, PyLegendPrimitive]
