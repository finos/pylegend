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

"""
Float expression type in the PyLegend expression language.

``PyLegendFloat`` represents a floating-point column or computed
expression within a PyLegend query. It extends ``PyLegendNumber`` with
float-specific arithmetic (``+``, ``-``, ``*``) that preserves the
float type when both operands are floats, plus float-specific unary
operators (``abs()``, ``-``).

Instances are never constructed directly. They are produced by casting
a numeric column to float — for example,
``frame["Order Id"].to_float()`` — or by float arithmetic on existing
float expressions.

``PyLegendFloat`` inherits all mathematical functions from
``PyLegendNumber`` (trigonometric, logarithmic, rounding, etc.) and
general-purpose methods from ``PyLegendPrimitive`` (equality, null
checks, string conversion, ``in_list``).
"""

from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
    PyLegendUnion,
    TYPE_CHECKING,
)
from pylegend.core.language.shared.primitives.number import PyLegendNumber
from pylegend.core.language.shared.expression import PyLegendExpressionFloatReturn
from pylegend.core.language.shared.literal_expressions import PyLegendFloatLiteralExpression
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import grammar_method
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.language.shared.operations.float_operation_expressions import (
    PyLegendFloatAbsoluteExpression,
    PyLegendFloatAddExpression,
    PyLegendFloatNegativeExpression,
    PyLegendFloatSubtractExpression,
    PyLegendFloatMultiplyExpression,
)
if TYPE_CHECKING:
    from pylegend.core.language.shared.primitives import PyLegendInteger

__all__: PyLegendSequence[str] = [
    "PyLegendFloat"
]


class PyLegendFloat(PyLegendNumber):
    __value_copy: PyLegendExpressionFloatReturn

    def __init__(
            self,
            value: PyLegendExpressionFloatReturn
    ) -> None:
        self.__value_copy = value
        super().__init__(value)

    @grammar_method
    def __add__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendFloat]":
        """
        Addition (``+``).

        When both operands are floats, returns a ``PyLegendFloat``;
        otherwise falls back to ``PyLegendNumber``.

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The right-hand operand.

        Returns
        -------
        PyLegendFloat or PyLegendNumber
            The sum expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_plus"] = frame["Order Id"].to_float() + 0.5
            frame.head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Float plus (+) parameter")
        if isinstance(other, (float, PyLegendFloat)):
            other_op = PyLegendFloat.__convert_to_float_expr(other)
            return PyLegendFloat(PyLegendFloatAddExpression(self.__value_copy, other_op))
        else:
            return super().__add__(other)

    @grammar_method
    def __radd__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendFloat]":
        """
        Reflected addition (``float + expr``).

        Called when a Python literal is on the left side of ``+``.

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The left-hand operand.

        Returns
        -------
        PyLegendFloat or PyLegendNumber
            The sum expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.
        """
        PyLegendNumber.validate_param_to_be_number(other, "Float plus (+) parameter")
        if isinstance(other, (float, PyLegendFloat)):
            other_op = PyLegendFloat.__convert_to_float_expr(other)
            return PyLegendFloat(PyLegendFloatAddExpression(other_op, self.__value_copy))
        else:
            return super().__radd__(other)

    @grammar_method
    def __sub__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendFloat]":
        """
        Subtraction (``-``).

        When both operands are floats, returns a ``PyLegendFloat``;
        otherwise falls back to ``PyLegendNumber``.

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The right-hand operand.

        Returns
        -------
        PyLegendFloat or PyLegendNumber
            The difference expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_minus"] = frame["Order Id"].to_float() - 0.5
            frame.head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Float minus (-) parameter")
        if isinstance(other, (float, PyLegendFloat)):
            other_op = PyLegendFloat.__convert_to_float_expr(other)
            return PyLegendFloat(PyLegendFloatSubtractExpression(self.__value_copy, other_op))
        else:
            return super().__sub__(other)

    @grammar_method
    def __rsub__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendFloat]":
        """
        Reflected subtraction (``float - expr``).

        Called when a Python literal is on the left side of ``-``.

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The left-hand operand.

        Returns
        -------
        PyLegendFloat or PyLegendNumber
            The difference expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.
        """
        PyLegendNumber.validate_param_to_be_number(other, "Float minus (-) parameter")
        if isinstance(other, (float, PyLegendFloat)):
            other_op = PyLegendFloat.__convert_to_float_expr(other)
            return PyLegendFloat(PyLegendFloatSubtractExpression(other_op, self.__value_copy))
        else:
            return super().__rsub__(other)

    @grammar_method
    def __mul__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendFloat]":
        """
        Multiplication (``*``).

        When both operands are floats, returns a ``PyLegendFloat``;
        otherwise falls back to ``PyLegendNumber``.

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The right-hand operand.

        Returns
        -------
        PyLegendFloat or PyLegendNumber
            The product expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_scaled"] = frame["Order Id"].to_float() * 1.5
            frame.head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Float multiply (*) parameter")
        if isinstance(other, (float, PyLegendFloat)):
            other_op = PyLegendFloat.__convert_to_float_expr(other)
            return PyLegendFloat(PyLegendFloatMultiplyExpression(self.__value_copy, other_op))
        else:
            return super().__mul__(other)

    @grammar_method
    def __rmul__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendFloat]":
        """
        Reflected multiplication (``float * expr``).

        Called when a Python literal is on the left side of ``*``.

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The left-hand operand.

        Returns
        -------
        PyLegendFloat or PyLegendNumber
            The product expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.
        """
        PyLegendNumber.validate_param_to_be_number(other, "Float multiply (*) parameter")
        if isinstance(other, (float, PyLegendFloat)):
            other_op = PyLegendFloat.__convert_to_float_expr(other)
            return PyLegendFloat(PyLegendFloatMultiplyExpression(other_op, self.__value_copy))
        else:
            return super().__rmul__(other)

    @grammar_method
    def __abs__(self) -> "PyLegendFloat":
        """
        Absolute value (``abs(expr)``).

        Returns
        -------
        PyLegendFloat
            The absolute value expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["abs_val"] = abs(frame["Order Id"].to_float() - 10255.0)
            frame.head(3).to_pandas()

        """
        return PyLegendFloat(PyLegendFloatAbsoluteExpression(self.__value_copy))

    @grammar_method
    def __neg__(self) -> "PyLegendFloat":
        """
        Unary negation (``-expr``).

        Returns
        -------
        PyLegendFloat
            The negated expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["neg_val"] = -frame["Order Id"].to_float()
            frame.head(3).to_pandas()

        """
        return PyLegendFloat(PyLegendFloatNegativeExpression(self.__value_copy))

    @grammar_method
    def __pos__(self) -> "PyLegendFloat":
        """
        Unary positive (``+expr``).

        Returns the expression unchanged.

        Returns
        -------
        PyLegendFloat
            The same expression.
        """
        return self

    @staticmethod
    def __convert_to_float_expr(
            val: PyLegendUnion[float, "PyLegendFloat"]
    ) -> PyLegendExpressionFloatReturn:
        if isinstance(val, float):
            return PyLegendFloatLiteralExpression(val)
        return val.__value_copy

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)

    def value(self) -> PyLegendExpressionFloatReturn:
        return self.__value_copy

    @staticmethod
    def __validate__param_to_be_float(params, desc):  # type: ignore
        PyLegendNumber.validate_param_to_be_number(params, desc)
