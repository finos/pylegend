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
String expression type in the PyLegend expression language.

``PyLegendString`` represents a string-valued column or computed
expression within a PyLegend query. It provides a rich set of string
operations: case conversion, trimming, searching, slicing, padding,
concatenation (``+``), lexicographic comparisons (``<``, ``<=``, ``>``,
``>=``), regex matching, parsing to other types, and more.

Instances are never constructed directly. They are produced by accessing
a string column on a TDS frame — for example,
``frame["Ship Name"]`` — or by calling ``to_string()`` on another
expression.

``PyLegendString`` also inherits general-purpose methods from
``PyLegendPrimitive``, including equality / inequality tests, null checks,
and ``in_list``.
"""

from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
    PyLegendUnion,
    PyLegendOptional
)
from pylegend.core.language.shared.literal_expressions import (
    PyLegendIntegerLiteralExpression,
    convert_literal_to_literal_expression
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.language.shared.primitives.integer import PyLegendInteger
from pylegend.core.language.shared.primitives.float import PyLegendFloat
from pylegend.core.language.shared.primitives.decimal import PyLegendDecimal
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
from pylegend.core.language.shared.expression import PyLegendExpressionStringReturn
from pylegend.core.language.shared.literal_expressions import PyLegendStringLiteralExpression
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.pandas_api.frames.helpers.series_helper import grammar_method
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.language.shared.operations.string_operation_expressions import (
    PyLegendStringLengthExpression,
    PyLegendStringStartsWithExpression,
    PyLegendStringEndsWithExpression,
    PyLegendStringContainsExpression,
    PyLegendStringUpperExpression,
    PyLegendStringLowerExpression,
    PyLegendStringLTrimExpression,
    PyLegendStringRTrimExpression,
    PyLegendStringBTrimExpression,
    PyLegendStringPosExpression,
    PyLegendStringParseIntExpression,
    PyLegendStringParseFloatExpression,
    PyLegendStringParseDecimalExpression,
    PyLegendStringConcatExpression,
    PyLegendStringLessThanExpression,
    PyLegendStringLessThanEqualExpression,
    PyLegendStringGreaterThanExpression,
    PyLegendStringGreaterThanEqualExpression,
    PyLegendStringParseBooleanExpression,
    PyLegendStringParseDateTimeExpression,
    PyLegendStringAsciiExpression,
    PyLegendStringDecodeBase64Expression,
    PyLegendStringEncodeBase64Expression,
    PyLegendStringReverseExpression,
    PyLegendStringToLowerFirstCharacterExpression,
    PyLegendStringToUpperFirstCharacterExpression,
    PyLegendStringLeftExpression,
    PyLegendStringRightExpression,
    PyLegendStringSubStringExpression,
    PyLegendStringReplaceExpression,
    PyLegendStringLpadExpression,
    PyLegendStringRpadExpression,
    PyLegendStringSplitPartExpression,
    PyLegendStringFullMatchExpression,
    PyLegendStringRepeatStringExpression,
    PyLegendStringMatchExpression,
    PyLegendStringCoalesceExpression
)

__all__: PyLegendSequence[str] = [
    "PyLegendString"
]


class PyLegendString(PyLegendPrimitive):
    __value: PyLegendExpressionStringReturn

    def __init__(
            self,
            value: PyLegendExpressionStringReturn
    ) -> None:
        self.__value = value

    @grammar_method
    def len(self) -> PyLegendInteger:
        """
        Length of the string.

        Returns
        -------
        PyLegendInteger
            The number of characters.

        See Also
        --------
        length : Alias for ``len``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["name_len"] = frame["Ship Name"].len()
            frame.head(3).to_pandas()

        """
        return PyLegendInteger(PyLegendStringLengthExpression(self.__value))

    @grammar_method
    def length(self) -> PyLegendInteger:
        """
        Length of the string.

        Alias for :meth:`len`.

        Returns
        -------
        PyLegendInteger
            The number of characters.

        See Also
        --------
        len : Canonical length method.
        """
        return self.len()

    @grammar_method
    def startswith(self, prefix: str) -> PyLegendBoolean:
        """
        Test whether the string starts with *prefix*.

        Parameters
        ----------
        prefix : str
            The prefix to check.

        Returns
        -------
        PyLegendBoolean
            ``True`` where the string starts with *prefix*.

        Raises
        ------
        TypeError
            If *prefix* is not a ``str``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Ship Name"].startswith("Around")].head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_str(prefix, "startswith prefix parameter")
        return PyLegendBoolean(
            PyLegendStringStartsWithExpression(self.__value, PyLegendStringLiteralExpression(prefix))
        )

    @grammar_method
    def endswith(self, suffix: str) -> PyLegendBoolean:
        """
        Test whether the string ends with *suffix*.

        Parameters
        ----------
        suffix : str
            The suffix to check.

        Returns
        -------
        PyLegendBoolean
            ``True`` where the string ends with *suffix*.

        Raises
        ------
        TypeError
            If *suffix* is not a ``str``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Ship Name"].endswith("Horn")].head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_str(suffix, "endswith suffix parameter")
        return PyLegendBoolean(
            PyLegendStringEndsWithExpression(self.__value, PyLegendStringLiteralExpression(suffix))
        )

    @grammar_method
    def contains(self, other: str) -> PyLegendBoolean:
        """
        Test whether the string contains *other*.

        Parameters
        ----------
        other : str
            The substring to search for.

        Returns
        -------
        PyLegendBoolean
            ``True`` where the string contains *other*.

        Raises
        ------
        TypeError
            If *other* is not a ``str``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Ship Name"].contains("the")].head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_str(other, "contains/in other parameter")
        return PyLegendBoolean(
            PyLegendStringContainsExpression(self.__value, PyLegendStringLiteralExpression(other))
        )

    @grammar_method
    def string_contains(self, other: str) -> PyLegendBoolean:
        """
        Test whether the string contains *other*.

        Alias for :meth:`contains`.

        Parameters
        ----------
        other : str
            The substring to search for.

        Returns
        -------
        PyLegendBoolean
            ``True`` where the string contains *other*.

        See Also
        --------
        contains : Canonical contains method.
        """
        return self.contains(other)

    @grammar_method
    def equals(self, other: PyLegendUnion[str, "PyLegendString"]) -> PyLegendBoolean:
        """
        Test string equality.

        Alias for ``==``. Equivalent to :meth:`__eq__`.

        Parameters
        ----------
        other : str or PyLegendString
            The value to compare against.

        Returns
        -------
        PyLegendBoolean
            ``True`` where the strings are equal.

        See Also
        --------
        __eq__ : Operator form (``==``).
        """
        return self.__eq__(other)

    @grammar_method
    def upper(self) -> "PyLegendString":
        """
        Convert to upper case.

        Returns
        -------
        PyLegendString
            The upper-cased expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["name_upper"] = frame["Ship Name"].upper()
            frame.head(3).to_pandas()

        """
        return PyLegendString(PyLegendStringUpperExpression(self.__value))

    @grammar_method
    def lower(self) -> "PyLegendString":
        """
        Convert to lower case.

        Returns
        -------
        PyLegendString
            The lower-cased expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["name_lower"] = frame["Ship Name"].lower()
            frame.head(3).to_pandas()

        """
        return PyLegendString(PyLegendStringLowerExpression(self.__value))

    @grammar_method
    def lstrip(self) -> "PyLegendString":
        """
        Strip leading whitespace.

        Returns
        -------
        PyLegendString
            The left-trimmed expression.

        See Also
        --------
        rstrip : Strip trailing whitespace.
        strip : Strip both sides.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["name_ltrim"] = frame["Ship Name"].lstrip()
            frame.head(3).to_pandas()

        """
        return PyLegendString(PyLegendStringLTrimExpression(self.__value))

    @grammar_method
    def rstrip(self) -> "PyLegendString":
        """
        Strip trailing whitespace.

        Returns
        -------
        PyLegendString
            The right-trimmed expression.

        See Also
        --------
        lstrip : Strip leading whitespace.
        strip : Strip both sides.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["name_rtrim"] = frame["Ship Name"].rstrip()
            frame.head(3).to_pandas()

        """
        return PyLegendString(PyLegendStringRTrimExpression(self.__value))

    @grammar_method
    def strip(self) -> "PyLegendString":
        """
        Strip leading and trailing whitespace.

        Returns
        -------
        PyLegendString
            The trimmed expression.

        See Also
        --------
        lstrip : Strip leading whitespace only.
        rstrip : Strip trailing whitespace only.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["name_trim"] = frame["Ship Name"].strip()
            frame.head(3).to_pandas()

        """
        return PyLegendString(PyLegendStringBTrimExpression(self.__value))

    @grammar_method
    def index_of(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendInteger":
        """
        Find the position of a substring.

        Returns the zero-based index of the first occurrence of
        *other*, or ``-1`` if not found.

        Parameters
        ----------
        other : str or PyLegendString
            The substring to locate.

        Returns
        -------
        PyLegendInteger
            The position expression.

        Raises
        ------
        TypeError
            If *other* is not a ``str`` or ``PyLegendString``.

        See Also
        --------
        index : Alias for ``index_of``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["pos"] = frame["Ship Name"].index_of("the")
            frame.head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "Index_of parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendInteger(PyLegendStringPosExpression(self.__value, other_op))

    @grammar_method
    def index(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendInteger":
        """
        Find the position of a substring.

        Alias for :meth:`index_of`.

        Parameters
        ----------
        other : str or PyLegendString
            The substring to locate.

        Returns
        -------
        PyLegendInteger
            The position expression.

        See Also
        --------
        index_of : Canonical method.
        """
        return self.index_of(other)

    @grammar_method
    def parse_int(self) -> "PyLegendInteger":
        """
        Parse the string as an integer.

        Returns
        -------
        PyLegendInteger
            The parsed integer expression.

        See Also
        --------
        parse_integer : Alias for ``parse_int``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_parsed"] = frame["Order Id"].to_string().parse_int()
            frame.head(3).to_pandas()

        """
        return PyLegendInteger(PyLegendStringParseIntExpression(self.__value))

    @grammar_method
    def parse_integer(self) -> "PyLegendInteger":
        """
        Parse the string as an integer.

        Alias for :meth:`parse_int`.

        Returns
        -------
        PyLegendInteger
            The parsed integer expression.

        See Also
        --------
        parse_int : Canonical method.
        """
        return self.parse_int()

    @grammar_method
    def parse_float(self) -> "PyLegendFloat":
        """
        Parse the string as a float.

        Returns
        -------
        PyLegendFloat
            The parsed float expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_float"] = frame["Order Id"].to_string().parse_float()
            frame.head(3).to_pandas()

        """
        return PyLegendFloat(PyLegendStringParseFloatExpression(self.__value))

    @grammar_method
    def parse_decimal(
            self,
            precision: PyLegendOptional[int] = None,
            scale: PyLegendOptional[int] = None
    ) -> "PyLegendDecimal":
        """
        Parse the string as a decimal.

        Optionally specify *precision* and *scale* for a
        ``PyLegendNumeric`` result; both must be provided together or
        neither.

        Parameters
        ----------
        precision : int, optional
            Total number of digits.
        scale : int, optional
            Number of digits after the decimal point.

        Returns
        -------
        PyLegendDecimal
            The parsed decimal expression (or ``PyLegendNumeric``
            when *precision* and *scale* are given).

        Raises
        ------
        TypeError
            If only one of *precision* / *scale* is provided.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["id_dec"] = frame["Order Id"].to_string().parse_decimal()
            frame.head(3).to_pandas()

        """
        from pylegend.core.language.shared.primitives.precise_primitives import PyLegendNumeric
        if precision is not None and scale is not None:
            return PyLegendNumeric(
                PyLegendStringParseDecimalExpression(self.__value, precision, scale),
                precision,
                scale
            )
        if precision is not None or scale is not None:
            raise TypeError(
                "parse_decimal requires both precision and scale, or neither. "
                f"Got precision={precision}, scale={scale}"
            )
        return PyLegendDecimal(PyLegendStringParseDecimalExpression(self.__value))

    @grammar_method
    def parse_boolean(self) -> "PyLegendBoolean":
        """
        Parse the string as a boolean.

        Returns
        -------
        PyLegendBoolean
            The parsed boolean expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["is_long"] = (frame["Ship Name"].len() > 20).to_string()
            frame["parsed"] = frame["is_long"].parse_boolean()
            frame[["Ship Name", "is_long", "parsed"]].head(3).to_pandas()

        """
        return PyLegendBoolean(PyLegendStringParseBooleanExpression(self.__value))

    @grammar_method
    def parse_datetime(self) -> "PyLegendDateTime":
        """
        Parse the string as a datetime.

        Returns
        -------
        PyLegendDateTime
            The parsed datetime expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["date_str"] = frame["Shipped Date"].to_string() + " 00:00:00"
            frame["parsed_dt"] = frame["date_str"].parse_datetime()
            frame[["date_str", "parsed_dt"]].head(3).to_pandas()

        """
        return PyLegendDateTime(PyLegendStringParseDateTimeExpression(self.__value))

    @grammar_method
    def ascii(self) -> "PyLegendInteger":
        """
        ASCII code of the first character.

        Returns
        -------
        PyLegendInteger
            The ASCII code expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["first_ascii"] = frame["Ship Name"].ascii()
            frame.head(3).to_pandas()

        """
        return PyLegendInteger(PyLegendStringAsciiExpression(self.__value))

    @grammar_method
    def b64decode(self) -> "PyLegendString":
        """
        Decode a Base64-encoded string.

        Returns
        -------
        PyLegendString
            The decoded expression.

        See Also
        --------
        b64encode : The inverse operation.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["encoded"] = frame["Ship Name"].b64encode()
            frame["decoded"] = frame["encoded"].b64decode()
            frame[["Ship Name", "encoded", "decoded"]].head(3).to_pandas()

        """
        return PyLegendString(PyLegendStringDecodeBase64Expression(self.__value))

    @grammar_method
    def b64encode(self) -> "PyLegendString":
        """
        Encode the string to Base64.

        Returns
        -------
        PyLegendString
            The Base64-encoded expression.

        See Also
        --------
        b64decode : The inverse operation.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["encoded"] = frame["Ship Name"].b64encode()
            frame[["Ship Name", "encoded"]].head(3).to_pandas()

        """
        return PyLegendString(PyLegendStringEncodeBase64Expression(self.__value))

    @grammar_method
    def reverse(self) -> "PyLegendString":
        """
        Reverse the string.

        Returns
        -------
        PyLegendString
            The reversed expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["name_rev"] = frame["Ship Name"].reverse()
            frame.head(3).to_pandas()

        """
        return PyLegendString(PyLegendStringReverseExpression(self.__value))

    @grammar_method
    def to_lower_first_character(self) -> "PyLegendString":
        """
        Convert only the first character to lower case.

        Returns
        -------
        PyLegendString
            The expression with its first character lowered.

        See Also
        --------
        to_upper_first_character : The inverse operation.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["name_lf"] = frame["Ship Name"].to_lower_first_character()
            frame.head(3).to_pandas()

        """
        return PyLegendString(PyLegendStringToLowerFirstCharacterExpression(self.__value))

    @grammar_method
    def to_upper_first_character(self) -> "PyLegendString":
        """
        Convert only the first character to upper case.

        Returns
        -------
        PyLegendString
            The expression with its first character uppercased.

        See Also
        --------
        to_lower_first_character : The inverse operation.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["name_uf"] = frame["Ship Name"].lower().to_upper_first_character()
            frame.head(3).to_pandas()

        """
        return PyLegendString(PyLegendStringToUpperFirstCharacterExpression(self.__value))

    @grammar_method
    def left(self, count: PyLegendUnion[int, "PyLegendInteger"]) -> "PyLegendString":
        """
        Return the leftmost *count* characters.

        Parameters
        ----------
        count : int or PyLegendInteger
            Number of characters to take from the left.

        Returns
        -------
        PyLegendString
            The left-substring expression.

        Raises
        ------
        TypeError
            If *count* is not an ``int`` or ``PyLegendInteger``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["first_5"] = frame["Ship Name"].left(5)
            frame.head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_int_or_int_expr(count, "left parameter")
        count_op = PyLegendIntegerLiteralExpression(count) if isinstance(count, int) else count.value()
        return PyLegendString(PyLegendStringLeftExpression(self.__value, count_op))

    @grammar_method
    def right(self, count: PyLegendUnion[int, "PyLegendInteger"]) -> "PyLegendString":
        """
        Return the rightmost *count* characters.

        Parameters
        ----------
        count : int or PyLegendInteger
            Number of characters to take from the right.

        Returns
        -------
        PyLegendString
            The right-substring expression.

        Raises
        ------
        TypeError
            If *count* is not an ``int`` or ``PyLegendInteger``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["last_5"] = frame["Ship Name"].right(5)
            frame.head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_int_or_int_expr(count, "right parameter")
        count_op = PyLegendIntegerLiteralExpression(count) if isinstance(count, int) else count.value()
        return PyLegendString(PyLegendStringRightExpression(self.__value, count_op))

    @grammar_method
    def substring(
            self,
            start: PyLegendUnion[int, "PyLegendInteger"],
            end: PyLegendOptional[PyLegendUnion[int, "PyLegendInteger"]] = None) -> "PyLegendString":
        """
        Extract a substring.

        Parameters
        ----------
        start : int or PyLegendInteger
            The starting position (zero-based).
        end : int or PyLegendInteger, optional
            The ending position (exclusive). If omitted, takes
            everything from *start* to the end.

        Returns
        -------
        PyLegendString
            The substring expression.

        Raises
        ------
        TypeError
            If *start* or *end* is not an ``int`` or
            ``PyLegendInteger``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["sub"] = frame["Ship Name"].substring(0, 5)
            frame.head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_int_or_int_expr(start, "substring start parameter")
        start_op = PyLegendIntegerLiteralExpression(start) if isinstance(start, int) else start.value()
        if end is None:
            return PyLegendString(PyLegendStringSubStringExpression([self.__value, start_op]))
        PyLegendString.__validate_param_to_be_int_or_int_expr(end, "substring end parameter")
        end_op = PyLegendIntegerLiteralExpression(end) if isinstance(end, int) else end.value()
        return PyLegendString(PyLegendStringSubStringExpression([self.__value, start_op, end_op]))

    @grammar_method
    def replace(
            self,
            to_replace: PyLegendUnion[str, "PyLegendString"],
            replacement: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendString":
        """
        Replace occurrences of a substring.

        Parameters
        ----------
        to_replace : str or PyLegendString
            The substring to find.
        replacement : str or PyLegendString
            The replacement string.

        Returns
        -------
        PyLegendString
            The expression with replacements applied.

        Raises
        ------
        TypeError
            If *to_replace* or *replacement* is not a ``str`` or
            ``PyLegendString``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["name_replaced"] = frame["Ship Name"].replace("the", "THE")
            frame.head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_str_or_str_expr(to_replace, "replace to_replace parameter")
        to_replace_op = PyLegendStringLiteralExpression(to_replace) if isinstance(to_replace,
                                                                                  str) else to_replace.__value
        PyLegendString.__validate_param_to_be_str_or_str_expr(replacement, "replace replacement parameter")
        replacement_op = PyLegendStringLiteralExpression(replacement) if isinstance(replacement,
                                                                                    str) else replacement.__value
        return PyLegendString(PyLegendStringReplaceExpression([self.__value, to_replace_op, replacement_op]))

    @grammar_method
    def rjust(
            self,
            length: PyLegendUnion[int, "PyLegendInteger"],
            fill_char: PyLegendUnion[str, "PyLegendString"] = ' ') -> "PyLegendString":
        """
        Right-justify (left-pad) the string to *length*.

        Parameters
        ----------
        length : int or PyLegendInteger
            The target total length.
        fill_char : str or PyLegendString, default ``' '``
            The padding character.

        Returns
        -------
        PyLegendString
            The padded expression.

        Raises
        ------
        TypeError
            If *length* is not an ``int`` or ``PyLegendInteger``.

        See Also
        --------
        ljust : Left-justify (right-pad).

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["name_rj"] = frame["Ship Name"].rjust(30, "*")
            frame.head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_int_or_int_expr(length, "rjust length parameter")
        length_op = PyLegendIntegerLiteralExpression(length) if isinstance(length, int) else length.value()
        fill_char_op = PyLegendStringLiteralExpression(fill_char) if isinstance(fill_char, str) else fill_char.__value
        return PyLegendString(PyLegendStringLpadExpression([self.__value, length_op, fill_char_op]))

    @grammar_method
    def ljust(
            self,
            length: PyLegendUnion[int, "PyLegendInteger"],
            fill_char: PyLegendUnion[str, "PyLegendString"] = ' ') -> "PyLegendString":
        """
        Left-justify (right-pad) the string to *length*.

        Parameters
        ----------
        length : int or PyLegendInteger
            The target total length.
        fill_char : str or PyLegendString, default ``' '``
            The padding character.

        Returns
        -------
        PyLegendString
            The padded expression.

        Raises
        ------
        TypeError
            If *length* is not an ``int`` or ``PyLegendInteger``.

        See Also
        --------
        rjust : Right-justify (left-pad).

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["name_lj"] = frame["Ship Name"].ljust(30, "*")
            frame.head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_int_or_int_expr(length, "ljust length parameter")
        length_op = PyLegendIntegerLiteralExpression(length) if isinstance(length, int) else length.value()
        fill_char_op = PyLegendStringLiteralExpression(fill_char) if isinstance(fill_char, str) else fill_char.__value
        return PyLegendString(PyLegendStringRpadExpression([self.__value, length_op, fill_char_op]))

    @grammar_method
    def split_part(
            self,
            sep: PyLegendUnion[str, "PyLegendString"],
            part: PyLegendUnion[int, "PyLegendInteger"]) -> "PyLegendString | None":
        """
        Split the string by *sep* and return the *part*-th element.

        Parameters
        ----------
        sep : str or PyLegendString
            The delimiter.
        part : int or PyLegendInteger
            The one-based index of the part to return.

        Returns
        -------
        PyLegendString
            The extracted part.

        Raises
        ------
        TypeError
            If *sep* or *part* is not a supported type.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["first_word"] = frame["Ship Name"].split_part(" ", 1)
            frame.head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_str_or_str_expr(sep, "split_part sep parameter")
        sep_op = PyLegendStringLiteralExpression(sep) if isinstance(sep, str) else sep.value()
        PyLegendString.__validate_param_to_be_int_or_int_expr(part, "split_part part parameter")
        part_op = PyLegendIntegerLiteralExpression(part) if isinstance(part, int) else part.value()
        return PyLegendString(PyLegendStringSplitPartExpression([self.__value, sep_op, part_op]))

    @grammar_method
    def full_match(self, pattern: PyLegendUnion[str, "PyLegendString"]) -> PyLegendBoolean:
        """
        Test whether the entire string matches a regex *pattern*.

        Parameters
        ----------
        pattern : str or PyLegendString
            The regular expression pattern.

        Returns
        -------
        PyLegendBoolean
            ``True`` where the full string matches.

        Raises
        ------
        TypeError
            If *pattern* is not a ``str`` or ``PyLegendString``.

        See Also
        --------
        match : Partial (substring) match.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Ship Name"].full_match("Around%")].head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_str_or_str_expr(pattern, "full_match parameter")
        pattern_op = PyLegendStringLiteralExpression(pattern) if isinstance(pattern, str) else pattern.__value
        return PyLegendBoolean(PyLegendStringFullMatchExpression(self.__value, pattern_op))

    @grammar_method
    def match(self, pattern: PyLegendUnion[str, "PyLegendString"]) -> PyLegendBoolean:
        """
        Test whether any part of the string matches a regex *pattern*.

        Parameters
        ----------
        pattern : str or PyLegendString
            The regular expression pattern.

        Returns
        -------
        PyLegendBoolean
            ``True`` where a substring matches.

        Raises
        ------
        TypeError
            If *pattern* is not a ``str`` or ``PyLegendString``.

        See Also
        --------
        full_match : Full-string match.
        """
        PyLegendString.__validate_param_to_be_str_or_str_expr(pattern, "match parameter")  # pragma: no cover
        pattern_op = PyLegendStringLiteralExpression(pattern) if isinstance(
            pattern, str) else pattern.__value  # pragma: no cover
        return PyLegendBoolean(PyLegendStringMatchExpression(self.__value, pattern_op))  # pragma: no cover

    @grammar_method
    def repeat_string(self, times: PyLegendUnion[int, "PyLegendInteger"]) -> "PyLegendString":
        """
        Repeat the string *times* times.

        Parameters
        ----------
        times : int or PyLegendInteger
            The number of repetitions.

        Returns
        -------
        PyLegendString
            The repeated expression.

        Raises
        ------
        TypeError
            If *times* is not an ``int`` or ``PyLegendInteger``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["repeated"] = frame["Ship Name"].left(3).repeat_string(2)
            frame.head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_int_or_int_expr(times, "repeatString parameter")
        times_op = PyLegendIntegerLiteralExpression(times) if isinstance(times, int) else times.value()
        return PyLegendString(PyLegendStringRepeatStringExpression(self.__value, times_op))

    @grammar_method
    def coalesce(self, *other: PyLegendOptional[PyLegendUnion[str, "PyLegendString"]]) -> "PyLegendString":
        """
        Return the first non-null value among ``self`` and *other*.

        Translates to SQL ``COALESCE(self, ...)``.

        Parameters
        ----------
        *other : str, PyLegendString, or None
            Fallback values, checked in order.

        Returns
        -------
        PyLegendString
            The first non-null expression.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["name_or_default"] = frame["Ship Name"].coalesce("N/A")
            frame.head(3).to_pandas()

        """
        other_op = []
        for op in other:
            if op is not None:
                PyLegendString.__validate_param_to_be_str_or_str_expr(op, "coalesce parameter")
            other_op.append(
                convert_literal_to_literal_expression(op) if not isinstance(op, PyLegendString) else op.__value)

        return PyLegendString(PyLegendStringCoalesceExpression([self.__value, *other_op]))

    @grammar_method
    def __add__(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendString":
        """
        String concatenation (``+``).

        Parameters
        ----------
        other : str or PyLegendString
            The string to append.

        Returns
        -------
        PyLegendString
            The concatenated expression.

        Raises
        ------
        TypeError
            If *other* is not a ``str`` or ``PyLegendString``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame["greeting"] = frame["Ship Name"] + " - OK"
            frame.head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "String plus (+) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendString(PyLegendStringConcatExpression(self.__value, other_op))

    @grammar_method
    def __radd__(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendString":
        """
        Reflected string concatenation (``str + expr``).

        Called when a Python ``str`` is on the left side of ``+``.

        Parameters
        ----------
        other : str or PyLegendString
            The string to prepend.

        Returns
        -------
        PyLegendString
            The concatenated expression.

        Raises
        ------
        TypeError
            If *other* is not a ``str`` or ``PyLegendString``.
        """
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "String plus (+) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendString(PyLegendStringConcatExpression(other_op, self.__value))

    @grammar_method
    def __lt__(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendBoolean":
        """
        Lexicographic less than (``<``).

        Parameters
        ----------
        other : str or PyLegendString
            The right-hand operand.

        Returns
        -------
        PyLegendBoolean
            ``True`` where ``self < other`` lexicographically.

        Raises
        ------
        TypeError
            If *other* is not a ``str`` or ``PyLegendString``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Ship Name"] < "C"].head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "String less than (<) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendBoolean(PyLegendStringLessThanExpression(self.__value, other_op))

    @grammar_method
    def __le__(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendBoolean":
        """
        Lexicographic less than or equal (``<=``).

        Parameters
        ----------
        other : str or PyLegendString
            The right-hand operand.

        Returns
        -------
        PyLegendBoolean
            ``True`` where ``self <= other`` lexicographically.

        Raises
        ------
        TypeError
            If *other* is not a ``str`` or ``PyLegendString``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Ship Name"] <= "C"].head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "String less than equal (<=) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendBoolean(PyLegendStringLessThanEqualExpression(self.__value, other_op))

    @grammar_method
    def __gt__(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendBoolean":
        """
        Lexicographic greater than (``>``).

        Parameters
        ----------
        other : str or PyLegendString
            The right-hand operand.

        Returns
        -------
        PyLegendBoolean
            ``True`` where ``self > other`` lexicographically.

        Raises
        ------
        TypeError
            If *other* is not a ``str`` or ``PyLegendString``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Ship Name"] > "V"].head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "String greater than (>) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendBoolean(PyLegendStringGreaterThanExpression(self.__value, other_op))

    @grammar_method
    def __ge__(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendBoolean":
        """
        Lexicographic greater than or equal (``>=``).

        Parameters
        ----------
        other : str or PyLegendString
            The right-hand operand.

        Returns
        -------
        PyLegendBoolean
            ``True`` where ``self >= other`` lexicographically.

        Raises
        ------
        TypeError
            If *other* is not a ``str`` or ``PyLegendString``.

        Examples
        --------
        .. ipython:: python

            import pylegend
            frame = pylegend.samples.pandas_api.northwind_orders_frame()

            frame[frame["Ship Name"] >= "V"].head(3).to_pandas()

        """
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "String greater than equal (>=) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendBoolean(PyLegendStringGreaterThanEqualExpression(self.__value, other_op))

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return self.__value.to_sql_expression(frame_name_to_base_query_map, config)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return self.__value.to_pure_expression(config)

    def value(self) -> PyLegendExpressionStringReturn:
        return self.__value

    @staticmethod
    def __validate_param_to_be_str_or_str_expr(param: PyLegendUnion[str, "PyLegendString"], desc: str) -> None:
        if not isinstance(param, (str, PyLegendString)):
            raise TypeError(desc + " should be a str or a string expression (PyLegendString)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))

    @staticmethod
    def __validate_param_to_be_str(param: str, desc: str) -> None:
        if not isinstance(param, str):
            raise TypeError(desc + " should be a str."
                                   " Got value " + str(param) + " of type: " + str(type(param)))

    @staticmethod
    def __validate_param_to_be_int_or_int_expr(param: PyLegendUnion[int, "PyLegendInteger"], desc: str) -> None:
        if not isinstance(param, (int, PyLegendInteger)):
            raise TypeError(desc + " should be a int or a int expression (PyLegendInteger)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))
