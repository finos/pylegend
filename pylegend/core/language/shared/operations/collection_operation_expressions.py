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
    PyLegendDict,
)
from pylegend.core.language.shared.expression import (
    PyLegendExpression,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionNumberReturn,
    PyLegendExpressionStringReturn,
    PyLegendExpressionStrictDateReturn,
    PyLegendExpressionDateReturn,
)
from pylegend.core.language.shared.operations.unary_expression import PyLegendUnaryExpression
from pylegend.core.language.shared.helpers import generate_pure_functional_call
from pylegend.core.language.shared.operations.binary_expression import PyLegendBinaryExpression
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
)
from pylegend.core.sql.metamodel_extension import (
    CountExpression,
    DistinctCountExpression,
    AverageExpression,
    MaxExpression,
    MinExpression,
    SumExpression,
    StdDevSampleExpression,
    StdDevPopulationExpression,
    VarianceSampleExpression,
    VariancePopulationExpression,
    JoinStringsExpression,
)


__all__: PyLegendSequence[str] = [
    "PyLegendCountExpression",
    "PyLegendDistinctCountExpression",
    "PyLegendAverageExpression",
    "PyLegendIntegerMaxExpression",
    "PyLegendIntegerMinExpression",
    "PyLegendIntegerSumExpression",
    "PyLegendFloatMaxExpression",
    "PyLegendFloatMinExpression",
    "PyLegendFloatSumExpression",
    "PyLegendNumberMaxExpression",
    "PyLegendNumberMinExpression",
    "PyLegendNumberSumExpression",
    "PyLegendStdDevSampleExpression",
    "PyLegendStdDevPopulationExpression",
    "PyLegendVarianceSampleExpression",
    "PyLegendVariancePopulationExpression",
    "PyLegendStringMaxExpression",
    "PyLegendStringMinExpression",
    "PyLegendJoinStringsExpression",
    "PyLegendStrictDateMaxExpression",
    "PyLegendStrictDateMinExpression",
    "PyLegendDateMaxExpression",
    "PyLegendDateMinExpression",
]


class PyLegendCountExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return CountExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("count", [op_expr])

    def __init__(self, operand: PyLegendExpression) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendCountExpression.__to_sql_func,
            PyLegendCountExpression.__to_pure_func
        )


class PyLegendDistinctCountExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return DistinctCountExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call(
            "count",
            [generate_pure_functional_call("distinct", [op_expr])]
        )

    def __init__(self, operand: PyLegendExpression) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendDistinctCountExpression.__to_sql_func,
            PyLegendDistinctCountExpression.__to_pure_func
        )


class PyLegendAverageExpression(PyLegendUnaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return AverageExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("average", [op_expr])

    def __init__(self, operand: PyLegendExpressionNumberReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendAverageExpression.__to_sql_func,
            PyLegendAverageExpression.__to_pure_func
        )


class PyLegendIntegerMaxExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MaxExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("max", [op_expr])

    def __init__(self, operand: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendIntegerMaxExpression.__to_sql_func,
            PyLegendIntegerMaxExpression.__to_pure_func
        )


class PyLegendIntegerMinExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MinExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("min", [op_expr])

    def __init__(self, operand: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendIntegerMinExpression.__to_sql_func,
            PyLegendIntegerMinExpression.__to_pure_func
        )


class PyLegendIntegerSumExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return SumExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("sum", [op_expr])

    def __init__(self, operand: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendIntegerSumExpression.__to_sql_func,
            PyLegendIntegerSumExpression.__to_pure_func
        )


class PyLegendFloatMaxExpression(PyLegendUnaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MaxExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("max", [op_expr])

    def __init__(self, operand: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFloatMaxExpression.__to_sql_func,
            PyLegendFloatMaxExpression.__to_pure_func
        )


class PyLegendFloatMinExpression(PyLegendUnaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MinExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("min", [op_expr])

    def __init__(self, operand: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFloatMinExpression.__to_sql_func,
            PyLegendFloatMinExpression.__to_pure_func
        )


class PyLegendFloatSumExpression(PyLegendUnaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return SumExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("sum", [op_expr])

    def __init__(self, operand: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFloatSumExpression.__to_sql_func,
            PyLegendFloatSumExpression.__to_pure_func
        )


class PyLegendNumberMaxExpression(PyLegendUnaryExpression, PyLegendExpressionNumberReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MaxExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("max", [op_expr])

    def __init__(self, operand: PyLegendExpressionNumberReturn) -> None:
        PyLegendExpressionNumberReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendNumberMaxExpression.__to_sql_func,
            PyLegendNumberMaxExpression.__to_pure_func
        )


class PyLegendNumberMinExpression(PyLegendUnaryExpression, PyLegendExpressionNumberReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MinExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("min", [op_expr])

    def __init__(self, operand: PyLegendExpressionNumberReturn) -> None:
        PyLegendExpressionNumberReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendNumberMinExpression.__to_sql_func,
            PyLegendNumberMinExpression.__to_pure_func
        )


class PyLegendNumberSumExpression(PyLegendUnaryExpression, PyLegendExpressionNumberReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return SumExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("sum", [op_expr])

    def __init__(self, operand: PyLegendExpressionNumberReturn) -> None:
        PyLegendExpressionNumberReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendNumberSumExpression.__to_sql_func,
            PyLegendNumberSumExpression.__to_pure_func
        )


class PyLegendStdDevSampleExpression(PyLegendUnaryExpression, PyLegendExpressionNumberReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return StdDevSampleExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("stdDevSample", [op_expr])

    def __init__(self, operand: PyLegendExpressionNumberReturn) -> None:
        PyLegendExpressionNumberReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStdDevSampleExpression.__to_sql_func,
            PyLegendStdDevSampleExpression.__to_pure_func
        )


class PyLegendStdDevPopulationExpression(PyLegendUnaryExpression, PyLegendExpressionNumberReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return StdDevPopulationExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("stdDevPopulation", [op_expr])

    def __init__(self, operand: PyLegendExpressionNumberReturn) -> None:
        PyLegendExpressionNumberReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStdDevPopulationExpression.__to_sql_func,
            PyLegendStdDevPopulationExpression.__to_pure_func
        )


class PyLegendVarianceSampleExpression(PyLegendUnaryExpression, PyLegendExpressionNumberReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return VarianceSampleExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("varianceSample", [op_expr])

    def __init__(self, operand: PyLegendExpressionNumberReturn) -> None:
        PyLegendExpressionNumberReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendVarianceSampleExpression.__to_sql_func,
            PyLegendVarianceSampleExpression.__to_pure_func
        )


class PyLegendVariancePopulationExpression(PyLegendUnaryExpression, PyLegendExpressionNumberReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return VariancePopulationExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("variancePopulation", [op_expr])

    def __init__(self, operand: PyLegendExpressionNumberReturn) -> None:
        PyLegendExpressionNumberReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendVariancePopulationExpression.__to_sql_func,
            PyLegendVariancePopulationExpression.__to_pure_func
        )


class PyLegendStringMaxExpression(PyLegendUnaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MaxExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("max", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringMaxExpression.__to_sql_func,
            PyLegendStringMaxExpression.__to_pure_func
        )


class PyLegendStringMinExpression(PyLegendUnaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MinExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("min", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringMinExpression.__to_sql_func,
            PyLegendStringMinExpression.__to_pure_func
        )


class PyLegendJoinStringsExpression(PyLegendBinaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return JoinStringsExpression(value=expression1, other=expression2)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("joinStrings", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendJoinStringsExpression.__to_sql_func,
            PyLegendJoinStringsExpression.__to_pure_func
        )


class PyLegendStrictDateMaxExpression(PyLegendUnaryExpression, PyLegendExpressionStrictDateReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MaxExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("max", [op_expr])

    def __init__(self, operand: PyLegendExpressionStrictDateReturn) -> None:
        PyLegendExpressionStrictDateReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStrictDateMaxExpression.__to_sql_func,
            PyLegendStrictDateMaxExpression.__to_pure_func
        )


class PyLegendStrictDateMinExpression(PyLegendUnaryExpression, PyLegendExpressionStrictDateReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MinExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("min", [op_expr])

    def __init__(self, operand: PyLegendExpressionStrictDateReturn) -> None:
        PyLegendExpressionStrictDateReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStrictDateMinExpression.__to_sql_func,
            PyLegendStrictDateMinExpression.__to_pure_func
        )


class PyLegendDateMaxExpression(PyLegendUnaryExpression, PyLegendExpressionDateReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MaxExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("max", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendDateMaxExpression.__to_sql_func,
            PyLegendDateMaxExpression.__to_pure_func
        )


class PyLegendDateMinExpression(PyLegendUnaryExpression, PyLegendExpressionDateReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MinExpression(value=expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("min", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendDateMinExpression.__to_sql_func,
            PyLegendDateMinExpression.__to_pure_func
        )
