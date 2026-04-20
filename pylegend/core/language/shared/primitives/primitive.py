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
from decimal import Decimal as PythonDecimal
from datetime import date, datetime
from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
    PyLegendUnion,
    PyLegendList,
    TYPE_CHECKING,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.language.shared.expression import PyLegendExpression
from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.language.shared.operations.primitive_operation_expressions import (
    PyLegendPrimitiveEqualsExpression,
    PyLegendPrimitiveNotEqualsExpression,
    PyLegendIsEmptyExpression,
    PyLegendIsNotEmptyExpression,
    PyLegendPrimitiveToStringExpression,
    PyLegendInListExpression,
)
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import grammar_method
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
if TYPE_CHECKING:
    from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
    from pylegend.core.language.shared.primitives.string import PyLegendString

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

    @abstractmethod
    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        pass

    @grammar_method
    def __eq__(  # type: ignore
            self,
            other: "PyLegendUnion[int, float, bool, str, date, datetime, PythonDecimal, PyLegendPrimitive]"
    ) -> "PyLegendBoolean":
        """
        Test element-wise equality (``==``).

        Compare this expression against another primitive value or
        expression. Returns a boolean expression that is ``True`` where
        the values are equal.

        Parameters
        ----------
        other : int, float, bool, str, date, datetime, Decimal, or PyLegendPrimitive
            The value or expression to compare against.

        Returns
        -------
        PyLegendBoolean
            A boolean expression representing the equality test.

        Raises
        ------
        TypeError
            If *other* is not a supported primitive type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Filter rows where Ship Name equals a literal
            frame[frame["Ship Name"] == "Around the Horn"].head(3).to_pandas()

        """
        PyLegendPrimitive.__validate_param_to_be_primitive(other, "Equals (==) parameter")

        if isinstance(other, (int, float, bool, str, date, datetime, PythonDecimal)):
            other_op = convert_literal_to_literal_expression(other)
        else:
            other_op = other.value()

        from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
        return PyLegendBoolean(PyLegendPrimitiveEqualsExpression(self.value(), other_op))

    @grammar_method
    def __ne__(  # type: ignore
            self,
            other: "PyLegendUnion[int, float, bool, str, date, datetime, PythonDecimal, PyLegendPrimitive]"
    ) -> "PyLegendBoolean":
        """
        Test element-wise inequality (``!=``).

        Compare this expression against another primitive value or
        expression. Returns a boolean expression that is ``True`` where
        the values differ.

        Parameters
        ----------
        other : int, float, bool, str, date, datetime, Decimal, or PyLegendPrimitive
            The value or expression to compare against.

        Returns
        -------
        PyLegendBoolean
            A boolean expression representing the inequality test.

        Raises
        ------
        TypeError
            If *other* is not a supported primitive type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Filter rows where Ship Name is not a specific value
            frame[frame["Ship Name"] != "Ship1"].head(3).to_pandas()

        """
        PyLegendPrimitive.__validate_param_to_be_primitive(other, "Not Equals (!=) parameter")

        if isinstance(other, (int, float, bool, str, date, datetime, PythonDecimal)):
            other_op = convert_literal_to_literal_expression(other)
        else:
            other_op = other.value()

        from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
        return PyLegendBoolean(PyLegendPrimitiveNotEqualsExpression(self.value(), other_op))

    @grammar_method
    def is_empty(self) -> "PyLegendBoolean":
        """
        Test whether the value is null / empty.

        Returns a boolean expression that is ``True`` where the
        underlying column value is ``NULL``.

        Returns
        -------
        PyLegendBoolean
            ``True`` where the value is null.

        See Also
        --------
        is_not_empty : The inverse test.
        is_null : Alias for ``is_empty``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Keep only rows where Ship Name is null
            frame[frame["Shipped Date"].is_empty()].head(3).to_pandas()

        """
        from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
        return PyLegendBoolean(PyLegendIsEmptyExpression(self.value()))

    @grammar_method
    def is_null(self) -> "PyLegendBoolean":
        """
        Test whether the value is null.

        Alias for :meth:`is_empty`.

        Returns
        -------
        PyLegendBoolean
            ``True`` where the value is null.

        See Also
        --------
        is_empty : Canonical null-check method.
        is_not_null : The inverse test.
        """
        return self.is_empty()

    @grammar_method
    def is_not_empty(self) -> "PyLegendBoolean":
        """
        Test whether the value is not null / not empty.

        Returns a boolean expression that is ``True`` where the
        underlying column value is not ``NULL``.

        Returns
        -------
        PyLegendBoolean
            ``True`` where the value is not null.

        See Also
        --------
        is_empty : The inverse test.
        is_not_null : Alias for ``is_not_empty``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Keep only rows where Ship Name is present
            frame[frame["Ship Name"].is_not_empty()].head(3).to_pandas()

        """
        from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
        return PyLegendBoolean(PyLegendIsNotEmptyExpression(self.value()))

    @grammar_method
    def is_not_null(self) -> "PyLegendBoolean":
        """
        Test whether the value is not null.

        Alias for :meth:`is_not_empty`.

        Returns
        -------
        PyLegendBoolean
            ``True`` where the value is not null.

        See Also
        --------
        is_not_empty : Canonical not-null-check method.
        is_null : The inverse test.
        """
        return self.is_not_empty()

    @grammar_method
    def to_string(self) -> "PyLegendString":
        """
        Convert the value to its string representation.

        Returns a string expression produced by applying the database's
        ``CAST(... AS TEXT)`` (SQL) or ``->toString()`` (Pure).

        Returns
        -------
        PyLegendString
            The stringified expression.

        See Also
        --------
        toString : Alias for ``to_string``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Convert an integer column to a string
            frame["order_str"] = frame["Order Id"].to_string()
            frame.head(3).to_pandas()

        """
        from pylegend.core.language.shared.primitives.string import PyLegendString
        return PyLegendString(PyLegendPrimitiveToStringExpression(self.value()))

    @grammar_method
    def toString(self) -> "PyLegendString":
        """
        Convert the value to its string representation.

        Alias for :meth:`to_string`.

        Returns
        -------
        PyLegendString
            The stringified expression.

        See Also
        --------
        to_string : Canonical string-conversion method.
        """
        return self.to_string()

    @grammar_method
    def in_list(
            self,
            lst: "PyLegendList[PyLegendUnion[int, float, bool, str, date, datetime, PythonDecimal, PyLegendPrimitive]]"
    ) -> "PyLegendBoolean":
        """
        Test whether the value is contained in a list of values.

        Returns a boolean expression equivalent to SQL ``IN (...)``.

        Parameters
        ----------
        lst : list of int, float, bool, str, date, datetime, Decimal, or PyLegendPrimitive
            A **non-empty** list of literal values or expressions to
            check membership against.

        Returns
        -------
        PyLegendBoolean
            ``True`` where the value matches any element in *lst*.

        Raises
        ------
        ValueError
            If *lst* is not a list or is empty.
        TypeError
            If any element in *lst* is not a supported primitive type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            # Keep rows where Ship Name is one of several values
            frame[
                frame["Ship Name"].in_list(["Hanari Carnes", "Victuailles en stock", "Suprêmes délices"])
            ].head(3).to_pandas()

        """
        if not isinstance(lst, list) or len(lst) == 0:
            raise ValueError("in_list parameter should be a non-empty list of primitive values.")
        operands: PyLegendList[PyLegendExpression] = [self.value()]
        for item in lst:
            PyLegendPrimitive.__validate_param_to_be_primitive(item, "in_list list element")
            if isinstance(item, (int, float, bool, str, date, datetime, PythonDecimal)):
                operands.append(convert_literal_to_literal_expression(item))
            else:
                operands.append(item.value())
        from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
        return PyLegendBoolean(PyLegendInListExpression(operands))

    @staticmethod
    def __validate_param_to_be_primitive(
            param: "PyLegendUnion[int, float, bool, str, date, datetime, PythonDecimal, PyLegendPrimitive]",
            desc: str
    ) -> None:
        if not isinstance(param, (int, float, bool, str, date, datetime, PythonDecimal, PyLegendPrimitive)):
            raise TypeError(
                desc + " should be a int/float/bool/str/datetime.date/datetime.datetime/decimal.Decimal"
                       " or a primitive expression."
                       " Got value " + str(param) + " of type: " + str(type(param)))

    @abstractmethod
    def value(self) -> PyLegendExpression:
        pass


PyLegendPrimitiveOrPythonPrimitive = PyLegendUnion[int, float, str, bool, date, datetime, PythonDecimal, PyLegendPrimitive]
