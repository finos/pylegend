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


from decimal import Decimal
from datetime import date, datetime
from pylegend._typing import (
    PyLegendSequence,
)
from pylegend.core.language.shared.expression import (
    PyLegendExpressionStringReturn,
    PyLegendExpressionBooleanReturn,
    PyLegendExpressionNumberReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionDecimalReturn,
    PyLegendExpressionDateReturn,
    PyLegendExpressionDateTimeReturn,
    PyLegendExpressionStrictDateReturn,
)
from pylegend.core.language.shared.literal_expressions import (
    PyLegendStringLiteralExpression,
    convert_literal_to_literal_expression,
)
from pylegend.core.language.shared.primitives.strictdate import PyLegendStrictDate
from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
from pylegend.core.language.shared.primitives.string import PyLegendString
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive, PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.language.shared.operations.date_operation_expressions import (
    PyLegendTodayExpression,
    PyLegendNowExpression,
    PyLegendMostRecentDayOfWeekExpression,
    PyLegendPreviousDayOfWeekExpression,
)
from pylegend.core.language.shared.operations.string_operation_expressions import (
    PyLegendCurrentUserExpression,
)
from pylegend.core.language.shared.primitives.float import PyLegendFloat
from pylegend.core.language.shared.operations.float_operation_expressions import PyLegendFloatPiExpression
from pylegend.core.language.shared.operations.boolean_operation_expressions import (
    PyLegendBooleanMultiCaseExpression,
    PyLegendStringMultiCaseExpression,
    PyLegendNumberMultiCaseExpression,
    PyLegendIntegerMultiCaseExpression,
    PyLegendFloatMultiCaseExpression,
    PyLegendDecimalMultiCaseExpression,
    PyLegendDateMultiCaseExpression,
    PyLegendDateTimeMultiCaseExpression,
    PyLegendStrictDateMultiCaseExpression,
)


__all__: PyLegendSequence[str] = [
    "today",
    "now",
    "current_user",
    "pi",
    "most_recent_day_of_week",
    "previous_day_of_week",
    "cases",
]


def today() -> PyLegendStrictDate:
    return PyLegendStrictDate(PyLegendTodayExpression())


def now() -> PyLegendDateTime:
    return PyLegendDateTime(PyLegendNowExpression())


def current_user() -> PyLegendString:
    return PyLegendString(PyLegendCurrentUserExpression())


def pi() -> PyLegendFloat:
    return PyLegendFloat(PyLegendFloatPiExpression())


def _validate_day_of_week(day_of_week: str) -> PyLegendExpressionStringReturn:
    if not isinstance(day_of_week, str):
        raise TypeError(f"day_of_week must be a string, got {type(day_of_week).__name__}")
    normalized = day_of_week.strip().capitalize()
    if normalized not in ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"):
        raise ValueError(
            f"Invalid day of week: '{day_of_week}'. "
            f"Must be one of: Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday"
        )
    return PyLegendStringLiteralExpression(normalized)


def most_recent_day_of_week(day_of_week: str) -> PyLegendStrictDate:
    validated_day = _validate_day_of_week(day_of_week)
    return PyLegendStrictDate(PyLegendMostRecentDayOfWeekExpression(validated_day))


def previous_day_of_week(day_of_week: str) -> PyLegendStrictDate:
    validated_day = _validate_day_of_week(day_of_week)
    return PyLegendStrictDate(PyLegendPreviousDayOfWeekExpression(validated_day))


def cases(*when_pairs: tuple, default: PyLegendPrimitiveOrPythonPrimitive) -> PyLegendPrimitive:
    """
    Multi-branch conditional — SQL ``CASE WHEN … THEN … ELSE … END``.

    Build an N-branch CASE expression from a sequence of
    ``(condition, value)`` pairs and a required default value.  Each
    *condition* must be a boolean expression; all *value* arguments
    (including *default*) must belong to the same type family.

    Parameters
    ----------
    *when_pairs : tuple of (condition, value)
        One or more ``(PyLegendBoolean, value)`` pairs evaluated in
        order.  The first pair whose condition is ``True`` determines
        the result.  At least one pair is required.
    default : int, float, bool, str, date, datetime, Decimal, or PyLegendPrimitive
        Value returned when no condition matches.

    Returns
    -------
    PyLegendPrimitive
        An expression whose concrete type is inferred from the value
        branches (e.g. ``PyLegendString`` when all values are strings).
        Mixed numeric branches widen to ``PyLegendNumber``.

    Raises
    ------
    TypeError
        If no pairs are supplied, a pair is not a 2-tuple, a condition
        is not boolean, a value is not a supported primitive type, or
        the values have incompatible types.

    Examples
    --------
    .. ipython:: python

        import pylegend
        frame = pylegend.samples.legendql_api.northwind_frame()

        frame = frame.extend(("Category", lambda r: pylegend.cases(
            (r.get_integer("Order Id") < 10250, "small"),
            (r.get_integer("Order Id") < 10260, "medium"),
            default="large",
        )))

    """
    from pylegend.core.language.shared.primitives import (
        PyLegendBoolean,
        PyLegendString,
        PyLegendNumber,
        PyLegendInteger,
        PyLegendFloat,
        PyLegendDecimal,
        PyLegendDate,
        PyLegendDateTime,
        PyLegendStrictDate,
    )

    if not when_pairs:
        raise TypeError("cases() requires at least one (condition, value) pair.")

    def resolve_value(param: PyLegendPrimitiveOrPythonPrimitive, label: str):  # type: ignore[no-untyped-def]
        if isinstance(param, (bool, int, float, str, date, datetime, Decimal)):
            return convert_literal_to_literal_expression(param)
        elif isinstance(param, PyLegendPrimitive):
            return param.value()
        else:
            raise TypeError(
                f"cases() {label} should be a primitive value or PyLegendPrimitive expression."
                f" Got value {param} of type: {type(param)}"
            )

    resolved_pairs = []
    for i, pair in enumerate(when_pairs):
        if not (isinstance(pair, tuple) and len(pair) == 2):
            raise TypeError(
                f"cases() when_pairs[{i}] must be a 2-tuple (condition, value)."
                f" Got: {pair!r}"
            )
        cond, val = pair
        if isinstance(cond, PyLegendBoolean):
            cond_expr = cond.value()
        elif isinstance(cond, PyLegendPrimitive):
            expr = cond.value()
            if not isinstance(expr, PyLegendExpressionBooleanReturn):
                raise TypeError(
                    f"cases() condition at index {i} must be a boolean expression."
                    f" Got: {type(cond)}"
                )
            cond_expr = expr
        else:
            raise TypeError(
                f"cases() condition at index {i} must be a PyLegendBoolean expression."
                f" Got: {type(cond)}"
            )
        val_expr = resolve_value(val, f"value at index {i}")
        resolved_pairs.append((cond_expr, val_expr))

    default_expr = resolve_value(default, "default")

    all_result_exprs = [val for _, val in resolved_pairs] + [default_expr]

    def all_match(cls: type) -> bool:
        return all(isinstance(e, cls) for e in all_result_exprs)

    if all_match(PyLegendExpressionBooleanReturn):
        return PyLegendBoolean(PyLegendBooleanMultiCaseExpression(resolved_pairs, default_expr))
    elif all_match(PyLegendExpressionStringReturn):
        return PyLegendString(PyLegendStringMultiCaseExpression(resolved_pairs, default_expr))
    elif all_match(PyLegendExpressionDateTimeReturn):
        return PyLegendDateTime(PyLegendDateTimeMultiCaseExpression(resolved_pairs, default_expr))
    elif all_match(PyLegendExpressionStrictDateReturn):
        return PyLegendStrictDate(PyLegendStrictDateMultiCaseExpression(resolved_pairs, default_expr))
    elif all_match(PyLegendExpressionDateReturn):
        return PyLegendDate(PyLegendDateMultiCaseExpression(resolved_pairs, default_expr))
    elif all_match(PyLegendExpressionDecimalReturn):
        return PyLegendDecimal(PyLegendDecimalMultiCaseExpression(resolved_pairs, default_expr))
    elif all_match(PyLegendExpressionFloatReturn):
        return PyLegendFloat(PyLegendFloatMultiCaseExpression(resolved_pairs, default_expr))
    elif all_match(PyLegendExpressionIntegerReturn):
        return PyLegendInteger(PyLegendIntegerMultiCaseExpression(resolved_pairs, default_expr))
    elif all_match(PyLegendExpressionNumberReturn):
        return PyLegendNumber(PyLegendNumberMultiCaseExpression(resolved_pairs, default_expr))
    else:
        raise TypeError(
            "cases() all value branches (including default) must belong to the same type family."
            " Supported families: bool, int, float, Decimal, str, date, datetime, PyLegendPrimitive"
        )
