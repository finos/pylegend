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
Numeric expression type in the PyLegend expression language.

``PyLegendNumber`` is the base class for all numeric column expressions
(``PyLegendInteger``, ``PyLegendFloat``, ``PyLegendDecimal``). It
supports the standard arithmetic operators (``+``, ``-``, ``*``, ``/``,
``**``), comparison operators (``<``, ``<=``, ``>``, ``>=``), unary
operators (``+``, ``-``, ``abs()``), rounding/ceiling/floor, and a wide
range of mathematical functions (trigonometric, logarithmic, hyperbolic,
etc.).

Instances are never constructed directly. They are produced by accessing
a numeric column on a TDS frame — for example,
``frame["Order Id"]`` — or by arithmetic on existing numeric
expressions.

``PyLegendNumber`` also inherits general-purpose methods from
``PyLegendPrimitive``, including equality / inequality tests, null checks,
string conversion, and ``in_list``.
"""

from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
    PyLegendUnion,
    PyLegendOptional,
    TYPE_CHECKING,
)
from decimal import Decimal as PythonDecimal
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
from pylegend.core.language.shared.expression import (
    PyLegendExpressionNumberReturn,
)
from pylegend.core.language.shared.literal_expressions import (
    PyLegendIntegerLiteralExpression,
    PyLegendFloatLiteralExpression,
)
from pylegend.core.language.shared.operations.number_operation_expressions import (
    PyLegendNumberAddExpression,
    PyLegendNumberMultiplyExpression,
    PyLegendNumberDivideExpression,
    PyLegendNumberSubtractExpression,
    PyLegendNumberLessThanExpression,
    PyLegendNumberLessThanEqualExpression,
    PyLegendNumberGreaterThanExpression,
    PyLegendNumberGreaterThanEqualExpression,
    PyLegendNumberNegativeExpression,
    PyLegendNumberAbsoluteExpression,
    PyLegendNumberPowerExpression,
    PyLegendNumberCeilExpression,
    PyLegendNumberFloorExpression,
    PyLegendNumberSqrtExpression,
    PyLegendNumberCbrtExpression,
    PyLegendNumberExpExpression,
    PyLegendNumberLogExpression,
    PyLegendNumberRemainderExpression,
    PyLegendNumberRoundExpression,
    PyLegendNumberSineExpression,
    PyLegendNumberArcSineExpression,
    PyLegendNumberCosineExpression,
    PyLegendNumberArcCosineExpression,
    PyLegendNumberTanExpression,
    PyLegendNumberArcTanExpression,
    PyLegendNumberArcTan2Expression,
    PyLegendNumberCotExpression,
    PyLegendNumberLog10Expression,
    PyLegendNumberDegreesExpression,
    PyLegendNumberRadiansExpression,
    PyLegendNumberSignExpression,
    PyLegendNumberHyperbolicSinExpression,
    PyLegendNumberHyperbolicCosExpression,
    PyLegendNumberHyperbolicTanExpression
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import grammar_method
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
if TYPE_CHECKING:
    from pylegend.core.language.shared.primitives.integer import PyLegendInteger
    from pylegend.core.language.shared.primitives.float import PyLegendFloat
    from pylegend.core.language.shared.primitives.decimal import PyLegendDecimal
    from pylegend.core.language.shared.primitive_collection import PyLegendNumberPairCollection


__all__: PyLegendSequence[str] = [
    "PyLegendNumber"
]


class PyLegendNumber(PyLegendPrimitive):
    __value: PyLegendExpressionNumberReturn

    def __init__(
            self,
            value: PyLegendExpressionNumberReturn
    ) -> None:
        self.__value = value

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return self.__value.to_sql_expression(frame_name_to_base_query_map, config)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return self.__value.to_pure_expression(config)

    def value(self) -> PyLegendExpressionNumberReturn:
        return self.__value

    @grammar_method
    def __add__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        """
        Addition (``+``).

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The right-hand operand.

        Returns
        -------
        PyLegendNumber
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

            frame["id_plus_10"] = frame["Order Id"] + 10
            frame.head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Number plus (+) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberAddExpression(self.__value, other_op))

    @grammar_method
    def __radd__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        """
        Reflected addition (``int + expr``).

        Called when a Python literal is on the left side of ``+``.
        Behaves identically to :meth:`__add__` with swapped operand
        order.

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The left-hand operand.

        Returns
        -------
        PyLegendNumber
            The sum expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.
        """
        PyLegendNumber.validate_param_to_be_number(other, "Number plus (+) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberAddExpression(other_op, self.__value))

    @grammar_method
    def __mul__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        """
        Multiplication (``*``).

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The right-hand operand.

        Returns
        -------
        PyLegendNumber
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

            frame["id_times_2"] = frame["Order Id"] * 2
            frame.head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Number multiply (*) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberMultiplyExpression(self.__value, other_op))

    @grammar_method
    def __rmul__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        """
        Reflected multiplication (``int * expr``).

        Called when a Python literal is on the left side of ``*``.
        Behaves identically to :meth:`__mul__` with swapped operand
        order.

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The left-hand operand.

        Returns
        -------
        PyLegendNumber
            The product expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.
        """
        PyLegendNumber.validate_param_to_be_number(other, "Number multiply (*) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberMultiplyExpression(other_op, self.__value))

    @grammar_method
    def __truediv__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        """
        True division (``/``).

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The divisor.

        Returns
        -------
        PyLegendNumber
            The quotient expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_half"] = frame["Order Id"] / 2
            frame.head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Number divide (/) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberDivideExpression(self.__value, other_op))

    @grammar_method
    def __rtruediv__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        """
        Reflected true division (``int / expr``).

        Called when a Python literal is on the left side of ``/``.
        Behaves identically to :meth:`__truediv__` with swapped operand
        order.

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The dividend (left-hand operand).

        Returns
        -------
        PyLegendNumber
            The quotient expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.
        """
        PyLegendNumber.validate_param_to_be_number(other, "Number divide (/) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberDivideExpression(other_op, self.__value))

    @grammar_method
    def __sub__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        """
        Subtraction (``-``).

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The right-hand operand.

        Returns
        -------
        PyLegendNumber
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

            frame["id_minus_10000"] = frame["Order Id"] - 10000
            frame.head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Number subtract (-) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberSubtractExpression(self.__value, other_op))

    @grammar_method
    def __rsub__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        """
        Reflected subtraction (``int - expr``).

        Called when a Python literal is on the left side of ``-``.
        Behaves identically to :meth:`__sub__` with swapped operand
        order.

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The left-hand operand (minuend).

        Returns
        -------
        PyLegendNumber
            The difference expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.
        """
        PyLegendNumber.validate_param_to_be_number(other, "Number subtract (-) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberSubtractExpression(other_op, self.__value))

    @grammar_method
    def __lt__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendBoolean":
        """
        Less than comparison (``<``).

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The right-hand operand.

        Returns
        -------
        PyLegendBoolean
            ``True`` where ``self < other``.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Order Id"] < 10250].head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Number less than (<) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendBoolean(PyLegendNumberLessThanExpression(self.__value, other_op))

    @grammar_method
    def __le__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendBoolean":
        """
        Less than or equal comparison (``<=``).

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The right-hand operand.

        Returns
        -------
        PyLegendBoolean
            ``True`` where ``self <= other``.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Order Id"] <= 10250].head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Number less than equal (<=) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendBoolean(PyLegendNumberLessThanEqualExpression(self.__value, other_op))

    @grammar_method
    def __gt__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendBoolean":
        """
        Greater than comparison (``>``).

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The right-hand operand.

        Returns
        -------
        PyLegendBoolean
            ``True`` where ``self > other``.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Order Id"] > 10250].head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Number greater than (>) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendBoolean(PyLegendNumberGreaterThanExpression(self.__value, other_op))

    @grammar_method
    def __ge__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendBoolean":
        """
        Greater than or equal comparison (``>=``).

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The right-hand operand.

        Returns
        -------
        PyLegendBoolean
            ``True`` where ``self >= other``.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Order Id"] >= 10250].head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Number greater than equal (>=) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendBoolean(PyLegendNumberGreaterThanEqualExpression(self.__value, other_op))

    @grammar_method
    def __pos__(self) -> "PyLegendNumber":
        """
        Unary positive (``+expr``).

        Returns the expression unchanged. Included for operator
        completeness.

        Returns
        -------
        PyLegendNumber
            The same expression.
        """
        return self

    @grammar_method
    def __neg__(self) -> "PyLegendNumber":
        """
        Unary negation (``-expr``).

        Returns
        -------
        PyLegendNumber
            The negated expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["neg_id"] = -frame["Order Id"]
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberNegativeExpression(self.__value))

    @grammar_method
    def __abs__(self) -> "PyLegendNumber":
        """
        Absolute value (``abs(expr)``).

        Returns
        -------
        PyLegendNumber
            The absolute value expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["abs_id"] = abs(frame["Order Id"])
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberAbsoluteExpression(self.__value))

    @grammar_method
    def __pow__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        """
        Power / exponentiation (``**``).

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The exponent.

        Returns
        -------
        PyLegendNumber
            The power expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_squared"] = frame["Order Id"] ** 2
            frame.head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Number power (**) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberPowerExpression(self.__value, other_op))

    @grammar_method
    def __rpow__(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        """
        Reflected power (``int ** expr``).

        Called when a Python literal is on the left side of ``**``.
        Behaves identically to :meth:`__pow__` with swapped operand
        order.

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The base (left-hand operand).

        Returns
        -------
        PyLegendNumber
            The power expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.
        """
        PyLegendNumber.validate_param_to_be_number(other, "Number power (**) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberPowerExpression(other_op, self.__value))

    @grammar_method
    def ceil(self) -> "PyLegendInteger":
        """
        Ceiling — smallest integer greater than or equal to the value.

        Returns
        -------
        PyLegendInteger
            The ceiling expression.

        See Also
        --------
        floor : The opposite rounding direction.
        __ceil__ : Alias called by ``math.ceil()``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_ceil"] = (frame["Order Id"] / 3).ceil()
            frame.head(3).to_pandas()

        """
        from pylegend.core.language.shared.primitives.integer import PyLegendInteger
        return PyLegendInteger(PyLegendNumberCeilExpression(self.__value))

    @grammar_method
    def __ceil__(self) -> "PyLegendInteger":
        """
        Ceiling via ``math.ceil()``.

        Alias for :meth:`ceil`.

        Returns
        -------
        PyLegendInteger
            The ceiling expression.

        See Also
        --------
        ceil : Canonical ceiling method.
        """
        return self.ceil()

    @grammar_method
    def floor(self) -> "PyLegendInteger":
        """
        Floor — largest integer less than or equal to the value.

        Returns
        -------
        PyLegendInteger
            The floor expression.

        See Also
        --------
        ceil : The opposite rounding direction.
        __floor__ : Alias called by ``math.floor()``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_floor"] = (frame["Order Id"] / 3).floor()
            frame.head(3).to_pandas()

        """
        from pylegend.core.language.shared.primitives.integer import PyLegendInteger
        return PyLegendInteger(PyLegendNumberFloorExpression(self.__value))

    @grammar_method
    def __floor__(self) -> "PyLegendInteger":
        """
        Floor via ``math.floor()``.

        Alias for :meth:`floor`.

        Returns
        -------
        PyLegendInteger
            The floor expression.

        See Also
        --------
        floor : Canonical floor method.
        """
        return self.floor()

    @grammar_method
    def sqrt(self) -> "PyLegendNumber":
        """
        Square root.

        Returns
        -------
        PyLegendNumber
            The square-root expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_sqrt"] = frame["Order Id"].sqrt()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberSqrtExpression(self.__value))

    @grammar_method
    def cbrt(self) -> "PyLegendNumber":
        """
        Cube root.

        Returns
        -------
        PyLegendNumber
            The cube-root expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_cbrt"] = frame["Order Id"].cbrt()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberCbrtExpression(self.__value))

    @grammar_method
    def exp(self) -> "PyLegendNumber":
        """
        Natural exponential (``e ** x``).

        Returns
        -------
        PyLegendNumber
            The exponential expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_exp"] = (frame["Order Id"] - 10248).exp()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberExpExpression(self.__value))

    @grammar_method
    def log(self) -> "PyLegendNumber":
        """
        Natural logarithm (``ln(x)``).

        Returns
        -------
        PyLegendNumber
            The natural-log expression.

        See Also
        --------
        log10 : Base-10 logarithm.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_log"] = frame["Order Id"].log()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberLogExpression(self.__value))

    @grammar_method
    def rem(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        """
        Remainder (modulo for numeric expressions).

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The divisor.

        Returns
        -------
        PyLegendNumber
            The remainder expression.

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_rem_3"] = frame["Order Id"].rem(3)
            frame.head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Number remainder (rem) parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberRemainderExpression(self.__value, other_op))

    @grammar_method
    def sin(self) -> "PyLegendNumber":
        """
        Sine (input in radians).

        Returns
        -------
        PyLegendNumber
            The sine expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_sin"] = frame["Order Id"].sin()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberSineExpression(self.__value))

    @grammar_method
    def asin(self) -> "PyLegendNumber":
        """
        Arc-sine (inverse sine). Result in radians.

        Returns
        -------
        PyLegendNumber
            The arc-sine expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_asin"] = (frame["Order Id"] - 10248).asin()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberArcSineExpression(self.__value))

    @grammar_method
    def cos(self) -> "PyLegendNumber":
        """
        Cosine (input in radians).

        Returns
        -------
        PyLegendNumber
            The cosine expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_cos"] = frame["Order Id"].cos()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberCosineExpression(self.__value))

    @grammar_method
    def acos(self) -> "PyLegendNumber":
        """
        Arc-cosine (inverse cosine). Result in radians.

        Returns
        -------
        PyLegendNumber
            The arc-cosine expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_acos"] = (frame["Order Id"] - 10248).acos()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberArcCosineExpression(self.__value))

    @grammar_method
    def tan(self) -> "PyLegendNumber":
        """
        Tangent (input in radians).

        Returns
        -------
        PyLegendNumber
            The tangent expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_tan"] = frame["Order Id"].tan()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberTanExpression(self.__value))

    @grammar_method
    def atan(self) -> "PyLegendNumber":
        """
        Arc-tangent (inverse tangent). Result in radians.

        Returns
        -------
        PyLegendNumber
            The arc-tangent expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_atan"] = frame["Order Id"].atan()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberArcTanExpression(self.__value))

    @grammar_method
    def atan2(
            self,
            other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> "PyLegendNumber":
        """
        Two-argument arc-tangent (``atan2(self, other)``).

        Parameters
        ----------
        other : int, float, PyLegendInteger, PyLegendFloat, or PyLegendNumber
            The second argument (x-coordinate).

        Returns
        -------
        PyLegendNumber
            The arc-tangent expression (result in radians).

        Raises
        ------
        TypeError
            If *other* is not a supported numeric type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_atan2"] = frame["Order Id"].atan2(10000)
            frame.head(3).to_pandas()

        """
        PyLegendNumber.validate_param_to_be_number(other, "Number atan2 parameter")
        other_op = PyLegendNumber.__convert_to_number_expr(other)
        return PyLegendNumber(PyLegendNumberArcTan2Expression(self.__value, other_op))

    @grammar_method
    def cot(self) -> "PyLegendNumber":
        """
        Cotangent (input in radians).

        Returns
        -------
        PyLegendNumber
            The cotangent expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_cot"] = frame["Order Id"].cot()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberCotExpression(self.__value))

    @grammar_method
    def round(
            self,
            n: PyLegendOptional[int] = None
    ) -> "PyLegendNumber":
        """
        Round to *n* decimal places.

        Parameters
        ----------
        n : int, optional
            Number of decimal places. Defaults to ``0`` (round to
            nearest integer).

        Returns
        -------
        PyLegendNumber
            The rounded expression.

        Raises
        ------
        TypeError
            If *n* is not an ``int``.

        See Also
        --------
        __round__ : Alias called by ``round()``.
        ceil : Round up.
        floor : Round down.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_round"] = (frame["Order Id"] / 3).round(2)
            frame.head(3).to_pandas()

        """
        if n is None:
            return PyLegendNumber(PyLegendNumberRoundExpression(self.__value, PyLegendIntegerLiteralExpression(0)))
        else:
            if not isinstance(n, int):
                raise TypeError("Round parameter should be an int. Passed - " + str(type(n)))
            return PyLegendNumber(PyLegendNumberRoundExpression(self.__value, PyLegendIntegerLiteralExpression(n)))

    @grammar_method
    def log10(self) -> "PyLegendNumber":
        """
        Base-10 logarithm.

        Returns
        -------
        PyLegendNumber
            The base-10 logarithm expression.

        See Also
        --------
        log : Natural logarithm.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_log10"] = frame["Order Id"].log10()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberLog10Expression(self.__value))

    @grammar_method
    def degrees(self) -> "PyLegendNumber":
        """
        Convert radians to degrees.

        Returns
        -------
        PyLegendNumber
            The value in degrees.

        See Also
        --------
        radians : The inverse conversion.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_deg"] = frame["Order Id"].degrees()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberDegreesExpression(self.__value))

    @grammar_method
    def radians(self) -> "PyLegendNumber":
        """
        Convert degrees to radians.

        Returns
        -------
        PyLegendNumber
            The value in radians.

        See Also
        --------
        degrees : The inverse conversion.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_rad"] = frame["Order Id"].radians()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberRadiansExpression(self.__value))

    @grammar_method
    def sign(self) -> "PyLegendNumber":
        """
        Sign of the value (``-1``, ``0``, or ``1``).

        Returns
        -------
        PyLegendNumber
            ``-1`` for negative, ``0`` for zero, ``1`` for positive.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_sign"] = (frame["Order Id"] - 10255).sign()
            frame.head(5).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberSignExpression(self.__value))

    @grammar_method
    def sinh(self) -> "PyLegendNumber":
        """
        Hyperbolic sine.

        Returns
        -------
        PyLegendNumber
            The hyperbolic sine expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_sinh"] = (frame["Order Id"] - 10248).sinh()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberHyperbolicSinExpression(self.__value))

    @grammar_method
    def cosh(self) -> "PyLegendNumber":
        """
        Hyperbolic cosine.

        Returns
        -------
        PyLegendNumber
            The hyperbolic cosine expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_cosh"] = (frame["Order Id"] - 10248).cosh()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberHyperbolicCosExpression(self.__value))

    @grammar_method
    def tanh(self) -> "PyLegendNumber":
        """
        Hyperbolic tangent.

        Returns
        -------
        PyLegendNumber
            The hyperbolic tangent expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_tanh"] = (frame["Order Id"] - 10248).tanh()
            frame.head(3).to_pandas()

        """
        return PyLegendNumber(PyLegendNumberHyperbolicTanExpression(self.__value))

    @grammar_method
    def __round__(self, n: PyLegendOptional[int] = None) -> "PyLegendNumber":
        """
        Round via built-in ``round()``.

        Alias for :meth:`round`.

        Parameters
        ----------
        n : int, optional
            Number of decimal places.

        Returns
        -------
        PyLegendNumber
            The rounded expression.

        See Also
        --------
        round : Canonical rounding method.
        """
        return self.round(n)

    @grammar_method
    def to_decimal(self) -> "PyLegendDecimal":
        """
        Cast the value to a decimal expression.

        Returns
        -------
        PyLegendDecimal
            The value as a decimal expression.

        See Also
        --------
        to_float : Cast to float instead.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_decimal"] = frame["Order Id"].to_decimal()
            frame.head(3).to_pandas()

        """
        from pylegend.core.language.shared.primitives.decimal import PyLegendDecimal
        from pylegend.core.language.shared.operations.decimal_operation_expressions import (
            PyLegendNumberToDecimalExpression,
        )
        return PyLegendDecimal(PyLegendNumberToDecimalExpression(self.__value))

    @grammar_method
    def to_float(self) -> "PyLegendFloat":
        """
        Cast the value to a float expression.

        Returns
        -------
        PyLegendFloat
            The value as a float expression.

        See Also
        --------
        to_decimal : Cast to decimal instead.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_float"] = frame["Order Id"].to_float()
            frame.head(3).to_pandas()

        """
        from pylegend.core.language.shared.primitives.float import PyLegendFloat
        from pylegend.core.language.shared.operations.decimal_operation_expressions import (
            PyLegendNumberToFloatExpression,
        )
        return PyLegendFloat(PyLegendNumberToFloatExpression(self.__value))

    def row_mapper(
        self,
        other: PyLegendUnion[int, float, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"],
    ) -> "PyLegendNumberPairCollection":
        from pylegend.core.language.shared.primitive_collection import PyLegendNumberPairCollection
        return PyLegendNumberPairCollection(self, other)

    @staticmethod
    def __convert_to_number_expr(
            val: PyLegendUnion[int, float, PythonDecimal, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"]
    ) -> PyLegendExpressionNumberReturn:
        if isinstance(val, int):
            return PyLegendIntegerLiteralExpression(val)
        if isinstance(val, PythonDecimal):
            from pylegend.core.language.shared.literal_expressions import PyLegendDecimalLiteralExpression
            return PyLegendDecimalLiteralExpression(val)
        if isinstance(val, float):
            return PyLegendFloatLiteralExpression(val)
        return val.__value

    @staticmethod
    def validate_param_to_be_number(
            param: PyLegendUnion[int, float, PythonDecimal, "PyLegendInteger", "PyLegendFloat", "PyLegendNumber"],
            desc: str
    ) -> None:
        from pylegend.core.language.shared.primitives.integer import PyLegendInteger
        from pylegend.core.language.shared.primitives.float import PyLegendFloat
        from pylegend.core.language.shared.primitives.decimal import PyLegendDecimal
        if not isinstance(param, (int, float, PythonDecimal, PyLegendInteger, PyLegendFloat, PyLegendDecimal,
                                  PyLegendNumber)):
            raise TypeError(  # pragma: no cover
                desc + " should be a int/float/decimal.Decimal or a int/float/decimal/number expression"
                       " (PyLegendInteger/PyLegendFloat/PyLegendDecimal/PyLegendNumber)."
                       " Got value " + str(param) + " of type: " + str(type(param))
            )
