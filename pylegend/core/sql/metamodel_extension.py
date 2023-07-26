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

from enum import Enum
from pylegend._typing import (
    PyLegendSequence,
)
from pylegend.core.sql.metamodel import (
    Expression,
)

__all__: PyLegendSequence[str] = [
    "StringLengthExpression",
    "StringLikeExpression",
    "StringUpperExpression",
    "StringLowerExpression",
    "TrimType",
    "StringTrimExpression",
    "StringPosExpression",
    "StringConcatExpression",
    "AbsoluteExpression",
    "PowerExpression",
    "CeilExpression",
    "FloorExpression",
    "SqrtExpression",
]


class StringLengthExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression"
    ) -> None:
        super().__init__(_type="stringLengthExpression")
        self.value = value


class StringLikeExpression(Expression):
    value: "Expression"
    other: "Expression"

    def __init__(
        self,
        value: "Expression",
        other: "Expression"
    ) -> None:
        super().__init__(_type="stringLikeExpression")
        self.value = value
        self.other = other


class StringUpperExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression"
    ) -> None:
        super().__init__(_type="stringUpperExpression")
        self.value = value


class StringLowerExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression"
    ) -> None:
        super().__init__(_type="stringLowerExpression")
        self.value = value


class TrimType(Enum):
    Left = 1,
    Right = 2,
    Both = 3


class StringTrimExpression(Expression):
    value: "Expression"
    trim_type: TrimType

    def __init__(
        self,
        value: "Expression",
        trim_type: TrimType
    ) -> None:
        super().__init__(_type="stringTrimExpression")
        self.value = value
        self.trim_type = trim_type


class StringPosExpression(Expression):
    value: "Expression"
    other: "Expression"

    def __init__(
        self,
        value: "Expression",
        other: "Expression"
    ) -> None:
        super().__init__(_type="stringPosExpression")
        self.value = value
        self.other = other


class StringConcatExpression(Expression):
    first: "Expression"
    second: "Expression"

    def __init__(
        self,
        first: "Expression",
        second: "Expression"
    ) -> None:
        super().__init__(_type="stringConcatExpression")
        self.first = first
        self.second = second


class AbsoluteExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="absoluteExpression")
        self.value = value


class PowerExpression(Expression):
    first: "Expression"
    second: "Expression"

    def __init__(
        self,
        first: "Expression",
        second: "Expression"
    ) -> None:
        super().__init__(_type="powerExpression")
        self.first = first
        self.second = second


class CeilExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="ceilExpression")
        self.value = value


class FloorExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="floorExpression")
        self.value = value


class SqrtExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="sqrtExpression")
        self.value = value
