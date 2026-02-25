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
from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig


__all__: PyLegendSequence[str] = [
    "PyLegendExpression",
    "PyLegendExpressionBooleanReturn",
    "PyLegendExpressionStringReturn",
    "PyLegendExpressionNumberReturn",
    "PyLegendExpressionIntegerReturn",
    "PyLegendExpressionFloatReturn",
    "PyLegendExpressionDateReturn",
    "PyLegendExpressionDateTimeReturn",
    "PyLegendExpressionStrictDateReturn",
    "PyLegendExpressionNullReturn"
]


class PyLegendExpression(metaclass=ABCMeta):
    @abstractmethod
    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        pass

    @abstractmethod
    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        pass

    def is_non_nullable(self) -> bool:
        return False

    @abstractmethod
    def get_sub_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        pass  # pragma: no cover


class PyLegendExpressionBooleanReturn(PyLegendExpression, metaclass=ABCMeta):
    def get_sub_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        return [self]


class PyLegendExpressionStringReturn(PyLegendExpression, metaclass=ABCMeta):
    def get_sub_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        return [self]


class PyLegendExpressionNumberReturn(PyLegendExpression, metaclass=ABCMeta):
    def get_sub_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        return [self]  # pragma: no cover (Covered by its subclasses)


class PyLegendExpressionIntegerReturn(PyLegendExpressionNumberReturn, metaclass=ABCMeta):
    def get_sub_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        return [self]


class PyLegendExpressionFloatReturn(PyLegendExpressionNumberReturn, metaclass=ABCMeta):
    def get_sub_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        return [self]


class PyLegendExpressionDateReturn(PyLegendExpression, metaclass=ABCMeta):
    def get_sub_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        return [self]  # pragma: no cover (Covered by its subclasses)


class PyLegendExpressionDateTimeReturn(PyLegendExpressionDateReturn, metaclass=ABCMeta):
    def get_sub_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        return [self]


class PyLegendExpressionStrictDateReturn(PyLegendExpressionDateReturn, metaclass=ABCMeta):
    def get_sub_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        return [self]


class PyLegendExpressionNullReturn(PyLegendExpression, metaclass=ABCMeta):
    def get_sub_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        return [self]
