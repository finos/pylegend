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


class PyLegendExpressionBooleanReturn(PyLegendExpression, metaclass=ABCMeta):
    pass


class PyLegendExpressionStringReturn(PyLegendExpression, metaclass=ABCMeta):
    pass


class PyLegendExpressionNumberReturn(PyLegendExpression, metaclass=ABCMeta):
    pass


class PyLegendExpressionIntegerReturn(PyLegendExpressionNumberReturn, metaclass=ABCMeta):
    pass


class PyLegendExpressionFloatReturn(PyLegendExpressionNumberReturn, metaclass=ABCMeta):
    pass


class PyLegendExpressionDateReturn(PyLegendExpression, metaclass=ABCMeta):
    pass


class PyLegendExpressionDateTimeReturn(PyLegendExpressionDateReturn, metaclass=ABCMeta):
    pass


class PyLegendExpressionStrictDateReturn(PyLegendExpressionDateReturn, metaclass=ABCMeta):
    pass
