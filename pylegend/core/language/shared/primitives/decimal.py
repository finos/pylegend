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
Decimal expression type in the PyLegend expression language.

``PyLegendDecimal`` represents a fixed-precision decimal column or
computed expression within a PyLegend query. It extends
``PyLegendNumber`` with decimal-specific arithmetic (``+``, ``-``,
``*``) that preserves the decimal type when both operands are decimals,
decimal-specific rounding, and a scaled ``divide`` method.

Instances are never constructed directly. They are produced by casting
a numeric column to decimal — for example,
``frame["Order Id"].to_decimal()`` — or by decimal arithmetic on
existing decimal expressions.

``PyLegendDecimal`` inherits all mathematical functions from
``PyLegendNumber`` (trigonometric, logarithmic, rounding, etc.) and
general-purpose methods from ``PyLegendPrimitive`` (equality, null
checks, string conversion, ``in_list``).
"""

from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
    PyLegendUnion,
    PyLegendOptional,
    TYPE_CHECKING,
)
from decimal import Decimal as PythonDecimal
from pylegend.core.language.shared.primitives.number import PyLegendNumber
from pylegend.core.language.shared.expression import (
    PyLegendExpressionDecimalReturn,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import grammar_method
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.language.shared.operations.decimal_operation_expressions import (
    PyLegendDecimalAbsoluteExpression,
    PyLegendDecimalAddExpression,
    PyLegendDecimalNegativeExpression,
    PyLegendDecimalSubtractExpression,
    PyLegendDecimalMultiplyExpression,
    PyLegendDecimalDivideScaledExpression,
    PyLegendDecimalRoundExpression,
)
from pylegend.core.language.shared.literal_expressions import (
    PyLegendDecimalLiteralExpression,
    PyLegendIntegerLiteralExpression,
)
if TYPE_CHECKING:
    from pylegend.core.language.shared.primitives import PyLegendInteger, PyLegendFloat

__all__: PyLegendSequence[str] = [
    "PyLegendDecimal"
]


class PyLegendDecimal(PyLegendNumber):
    __value_copy: PyLegendExpressionDecimalReturn

    def __init__(
            self,
            value: PyLegendExpressionDecimalReturn
    ) -> None:
        self.__value_copy = value
        super().__init__(value)

    @grammar_method
    def __add__(
            self,
            other: PyLegendUnion[
                int, float, PythonDecimal, "PyLegendInteger", "PyLegendFloat", "PyLegendDecimal", "PyLegendNumber"
            ]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendDecimal]":
        """
        Addition (``+``).

        When both operands are decimals, returns a ``PyLegendDecimal``;
        otherwise falls back to ``PyLegendNumber``.

        Parameters
        ----------
        other : int, float, Decimal, PyLegendInteger, PyLegendFloat, PyLegendDecimal, or PyLegendNumber
            The right-hand operand.

        Returns
        -------
        PyLegendDecimal or PyLegendNumber
            The sum expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from decimal import Decimal
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_plus"] = frame["Order Id"].to_decimal() + Decimal("0.5")
            frame.head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Decimal plus (+) parameter")
        if isinstance(other, (PythonDecimal, PyLegendDecimal)):
            other_op = PyLegendDecimal.__convert_to_decimal_expr(other)
            return PyLegendDecimal(PyLegendDecimalAddExpression(self.__value_copy, other_op))
        else:
            return super().__add__(other)

    @grammar_method
    def __radd__(
            self,
            other: PyLegendUnion[
                int, float, PythonDecimal, "PyLegendInteger", "PyLegendFloat", "PyLegendDecimal", "PyLegendNumber"
            ]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendDecimal]":
        """
        Reflected addition (``Decimal + expr``).

        Called when a Python literal is on the left side of ``+``.

        Parameters
        ----------
        other : int, float, Decimal, PyLegendInteger, PyLegendFloat, PyLegendDecimal, or PyLegendNumber
            The left-hand operand.

        Returns
        -------
        PyLegendDecimal or PyLegendNumber
            The sum expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.
        """
        PyLegendNumber.validate_param_to_be_number(other, "Decimal plus (+) parameter")
        if isinstance(other, (PythonDecimal, PyLegendDecimal)):
            other_op = PyLegendDecimal.__convert_to_decimal_expr(other)
            return PyLegendDecimal(PyLegendDecimalAddExpression(other_op, self.__value_copy))
        else:
            return super().__radd__(other)

    @grammar_method
    def __sub__(
            self,
            other: PyLegendUnion[
                int, float, PythonDecimal, "PyLegendInteger", "PyLegendFloat", "PyLegendDecimal", "PyLegendNumber"
            ]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendDecimal]":
        """
        Subtraction (``-``).

        When both operands are decimals, returns a ``PyLegendDecimal``;
        otherwise falls back to ``PyLegendNumber``.

        Parameters
        ----------
        other : int, float, Decimal, PyLegendInteger, PyLegendFloat, PyLegendDecimal, or PyLegendNumber
            The right-hand operand.

        Returns
        -------
        PyLegendDecimal or PyLegendNumber
            The difference expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from decimal import Decimal
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_minus"] = frame["Order Id"].to_decimal() - Decimal("100.25")
            frame.head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Decimal minus (-) parameter")
        if isinstance(other, (PythonDecimal, PyLegendDecimal)):
            other_op = PyLegendDecimal.__convert_to_decimal_expr(other)
            return PyLegendDecimal(PyLegendDecimalSubtractExpression(self.__value_copy, other_op))
        else:
            return super().__sub__(other)

    @grammar_method
    def __rsub__(
            self,
            other: PyLegendUnion[
                int, float, PythonDecimal, "PyLegendInteger", "PyLegendFloat", "PyLegendDecimal", "PyLegendNumber"
            ]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendDecimal]":
        """
        Reflected subtraction (``Decimal - expr``).

        Called when a Python literal is on the left side of ``-``.

        Parameters
        ----------
        other : int, float, Decimal, PyLegendInteger, PyLegendFloat, PyLegendDecimal, or PyLegendNumber
            The left-hand operand.

        Returns
        -------
        PyLegendDecimal or PyLegendNumber
            The difference expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.
        """
        PyLegendNumber.validate_param_to_be_number(other, "Decimal minus (-) parameter")
        if isinstance(other, (PythonDecimal, PyLegendDecimal)):
            other_op = PyLegendDecimal.__convert_to_decimal_expr(other)
            return PyLegendDecimal(PyLegendDecimalSubtractExpression(other_op, self.__value_copy))
        else:
            return super().__rsub__(other)

    @grammar_method
    def __mul__(
            self,
            other: PyLegendUnion[
                int, float, PythonDecimal, "PyLegendInteger", "PyLegendFloat", "PyLegendDecimal", "PyLegendNumber"
            ]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendDecimal]":
        """
        Multiplication (``*``).

        When both operands are decimals, returns a ``PyLegendDecimal``;
        otherwise falls back to ``PyLegendNumber``.

        Parameters
        ----------
        other : int, float, Decimal, PyLegendInteger, PyLegendFloat, PyLegendDecimal, or PyLegendNumber
            The right-hand operand.

        Returns
        -------
        PyLegendDecimal or PyLegendNumber
            The product expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from decimal import Decimal
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_scaled"] = frame["Order Id"].to_decimal() * Decimal("1.5")
            frame.head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Decimal multiply (*) parameter")
        if isinstance(other, (PythonDecimal, PyLegendDecimal)):
            other_op = PyLegendDecimal.__convert_to_decimal_expr(other)
            return PyLegendDecimal(PyLegendDecimalMultiplyExpression(self.__value_copy, other_op))
        else:
            return super().__mul__(other)

    @grammar_method
    def __rmul__(
            self,
            other: PyLegendUnion[
                int, float, PythonDecimal, "PyLegendInteger", "PyLegendFloat", "PyLegendDecimal", "PyLegendNumber"
            ]
    ) -> "PyLegendUnion[PyLegendNumber, PyLegendDecimal]":
        """
        Reflected multiplication (``Decimal * expr``).

        Called when a Python literal is on the left side of ``*``.

        Parameters
        ----------
        other : int, float, Decimal, PyLegendInteger, PyLegendFloat, PyLegendDecimal, or PyLegendNumber
            The left-hand operand.

        Returns
        -------
        PyLegendDecimal or PyLegendNumber
            The product expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.
        """
        PyLegendNumber.validate_param_to_be_number(other, "Decimal multiply (*) parameter")
        if isinstance(other, (PythonDecimal, PyLegendDecimal)):
            other_op = PyLegendDecimal.__convert_to_decimal_expr(other)
            return PyLegendDecimal(PyLegendDecimalMultiplyExpression(other_op, self.__value_copy))
        else:
            return super().__rmul__(other)

    @grammar_method
    def __abs__(self) -> "PyLegendDecimal":
        """
        Absolute value (``abs(expr)``).

        Returns
        -------
        PyLegendDecimal
            The absolute value expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from decimal import Decimal
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["abs_val"] = abs(frame["Order Id"].to_decimal() - Decimal("10255"))
            frame.head(3).to_pandas()

        """
        return PyLegendDecimal(PyLegendDecimalAbsoluteExpression(self.__value_copy))

    @grammar_method
    def __neg__(self) -> "PyLegendDecimal":
        """
        Unary negation (``-expr``).

        Returns
        -------
        PyLegendDecimal
            The negated expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["neg_val"] = -frame["Order Id"].to_decimal()
            frame.head(3).to_pandas()

        """
        return PyLegendDecimal(PyLegendDecimalNegativeExpression(self.__value_copy))

    @grammar_method
    def __pos__(self) -> "PyLegendDecimal":
        """
        Unary positive (``+expr``).

        Returns the expression unchanged.

        Returns
        -------
        PyLegendDecimal
            The same expression.
        """
        return self

    @grammar_method
    def round(
            self,
            n: PyLegendOptional[int] = None
    ) -> "PyLegendDecimal":
        """
        Round to *n* decimal places.

        Parameters
        ----------
        n : int, optional
            Number of decimal places. Defaults to ``0``.

        Returns
        -------
        PyLegendDecimal
            The rounded expression.

        Raises
        ------
        TypeError
            If *n* is not an ``int``.

        See Also
        --------
        __round__ : Alias called by ``round()``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_round"] = (frame["Order Id"].to_decimal() / 3).round(2)
            frame.head(3).to_pandas()

        """
        if n is None:
            return PyLegendDecimal(
                PyLegendDecimalRoundExpression(self.__value_copy, PyLegendIntegerLiteralExpression(0))
            )
        else:
            if not isinstance(n, int):
                raise TypeError("Round parameter should be an int. Passed - " + str(type(n)))
            return PyLegendDecimal(
                PyLegendDecimalRoundExpression(self.__value_copy, PyLegendIntegerLiteralExpression(n))
            )

    @grammar_method
    def __round__(self, n: PyLegendOptional[int] = None) -> "PyLegendDecimal":
        """
        Round via built-in ``round()``.

        Alias for :meth:`round`.

        Parameters
        ----------
        n : int, optional
            Number of decimal places.

        Returns
        -------
        PyLegendDecimal
            The rounded expression.

        See Also
        --------
        round : Canonical rounding method.
        """
        return self.round(n)

    @grammar_method
    def divide(
            self,
            other: PyLegendUnion[PythonDecimal, "PyLegendDecimal"],
            scale: int
    ) -> "PyLegendDecimal":
        """
        Scaled division.

        Divide by *other* and round the result to *scale* decimal
        places. This is the decimal-specific alternative to ``/``
        that gives explicit control over result precision.

        Parameters
        ----------
        other : Decimal or PyLegendDecimal
            The divisor.
        scale : int
            Number of decimal places in the result.

        Returns
        -------
        PyLegendDecimal
            The quotient expression rounded to *scale* places.

        Raises
        ------
        TypeError
            If *scale* is not an ``int``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            from decimal import Decimal
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_div"] = frame["Order Id"].to_decimal().divide(Decimal("3"), 4)
            frame.head(3).to_pandas()

        """
        if not isinstance(scale, int):
            raise TypeError("Divide scale parameter should be an int. Passed - " + str(type(scale)))
        other_op = PyLegendDecimal.__convert_to_decimal_expr(other)
        return PyLegendDecimal(
            PyLegendDecimalDivideScaledExpression(
                self.__value_copy, other_op, PyLegendIntegerLiteralExpression(scale)
            )
        )

    @staticmethod
    def __convert_to_decimal_expr(
            val: PyLegendUnion[PythonDecimal, "PyLegendDecimal"]
    ) -> PyLegendExpressionDecimalReturn:
        if isinstance(val, (PythonDecimal)):
            return PyLegendDecimalLiteralExpression(val)
        return val.__value_copy

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)

    def value(self) -> PyLegendExpressionDecimalReturn:
        return self.__value_copy

    @staticmethod
    def __validate__param_to_be_decimal(params, desc):  # type: ignore
        PyLegendNumber.validate_param_to_be_number(params, desc)
