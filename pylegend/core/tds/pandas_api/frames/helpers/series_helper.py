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


import copy
import importlib
from textwrap import dedent
from typing import (
    Type,
    TypeVar,
)
from pylegend._typing import (
    PyLegendAny,
    PyLegendCallable,
    PyLegendDict,
    PyLegendList,
    PyLegendOptional,
    PyLegendSequence,
    PyLegendTuple,
    PyLegendUnion,
    TYPE_CHECKING,
)
from pylegend.core.language.shared.expression import PyLegendExpression
from pylegend.core.language.shared.helpers import escape_column_name, generate_pure_lambda
from pylegend.core.tds.tds_frame import FrameToPureConfig
if TYPE_CHECKING:
    from pylegend.core.sql.metamodel import Expression
    from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries


__all__: PyLegendSequence[str] = [
    "add_primitive_methods",
    "assert_and_find_core_series",
    "has_window_function",
    "has_window_aggregate_function",
    "has_aggregate_function",
    "needs_zero_column_for_window",
    "get_pure_query_from_expr",
    "get_applied_func",
    "find_window_expression",
    "split_window_from_arithmetic",
    "convert_aggregate_series_to_window_aggregate_series",
]

T = TypeVar("T")
F = TypeVar("F", bound=PyLegendCallable[..., PyLegendAny])  # type: ignore[explicit-any]


def get_series_from_col_type(col_type: str) -> Type["Series"]:
    from pylegend.core.language.pandas_api.pandas_api_series import (
        IntegerSeries, FloatSeries, NumberSeries,
        StringSeries, BooleanSeries, DateSeries, DateTimeSeries,
        StrictDateSeries, DecimalSeries
    )

    _map: PyLegendDict[str, Type["Series"]] = {
        # Boolean
        "Boolean": BooleanSeries,

        # String
        "String": StringSeries,
        "Varchar": StringSeries,

        # Number
        "Number": NumberSeries,

        # Integer
        "Integer": IntegerSeries,
        "TinyInt": IntegerSeries,
        "UTinyInt": IntegerSeries,
        "SmallInt": IntegerSeries,
        "USmallInt": IntegerSeries,
        "Int": IntegerSeries,
        "UInt": IntegerSeries,
        "BigInt": IntegerSeries,
        "UBigInt": IntegerSeries,

        # Float
        "Float": FloatSeries,
        "Float4": FloatSeries,
        "Double": FloatSeries,

        # Decimal
        "Decimal": DecimalSeries,
        "Numeric": DecimalSeries,

        # Dates and Times
        "Date": DateSeries,
        "DateTime": DateTimeSeries,
        "Timestamp": DateTimeSeries,
        "StrictDate": StrictDateSeries,
    }

    if col_type not in _map:
        raise ValueError(f"Unsupported column type '{col_type}'")  # pragma: no cover

    return _map[col_type]


def get_groupby_series_from_col_type(col_type: str) -> Type["GroupbySeries"]:
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import (
        BooleanGroupbySeries, StringGroupbySeries, NumberGroupbySeries,
        IntegerGroupbySeries, FloatGroupbySeries, DecimalGroupbySeries,
        DateGroupbySeries, DateTimeGroupbySeries, StrictDateGroupbySeries
    )

    _map: PyLegendDict[str, Type["GroupbySeries"]] = {
        # Boolean
        "Boolean": BooleanGroupbySeries,

        # String
        "String": StringGroupbySeries,
        "Varchar": StringGroupbySeries,

        # Number
        "Number": NumberGroupbySeries,

        # Integer
        "Integer": IntegerGroupbySeries,
        "TinyInt": IntegerGroupbySeries,
        "UTinyInt": IntegerGroupbySeries,
        "SmallInt": IntegerGroupbySeries,
        "USmallInt": IntegerGroupbySeries,
        "Int": IntegerGroupbySeries,
        "UInt": IntegerGroupbySeries,
        "BigInt": IntegerGroupbySeries,
        "UBigInt": IntegerGroupbySeries,

        # Float
        "Float": FloatGroupbySeries,
        "Float4": FloatGroupbySeries,
        "Double": FloatGroupbySeries,

        # Decimal
        "Decimal": DecimalGroupbySeries,
        "Numeric": DecimalGroupbySeries,

        # Dates and Times
        "Date": DateGroupbySeries,
        "DateTime": DateTimeGroupbySeries,
        "Timestamp": DateTimeGroupbySeries,
        "StrictDate": StrictDateGroupbySeries,
    }

    if col_type not in _map:
        raise ValueError(f"Unsupported column type '{col_type}'")  # pragma: no cover

    return _map[col_type]


def grammar_method(func: F) -> F:  # type: ignore[explicit-any]
    setattr(func, "_is_grammar_method", True)
    return func


def add_primitive_methods(cls: Type[T]) -> Type[T]:
    primitive_to_series_map = {
        "PyLegendBoolean": "BooleanSeries",
        "PyLegendString": "StringSeries",
        "PyLegendNumber": "NumberSeries",
        "PyLegendInteger": "IntegerSeries",
        "PyLegendFloat": "FloatSeries",
        "PyLegendDecimal": "DecimalSeries",
        "PyLegendDate": "DateSeries",
        "PyLegendDateTime": "DateTimeSeries",
        "PyLegendStrictDate": "StrictDateSeries"
    }

    primitive_to_groupby_series_map = {
        "PyLegendBoolean": "BooleanGroupbySeries",
        "PyLegendString": "StringGroupbySeries",
        "PyLegendNumber": "NumberGroupbySeries",
        "PyLegendInteger": "IntegerGroupbySeries",
        "PyLegendFloat": "FloatGroupbySeries",
        "PyLegendDecimal": "DecimalGroupbySeries",
        "PyLegendDate": "DateGroupbySeries",
        "PyLegendDateTime": "DateTimeGroupbySeries",
        "PyLegendStrictDate": "StrictDateGroupbySeries"
    }

    mro_names = [base.__name__ for base in cls.__mro__]

    series_type: str
    if "GroupbySeries" in mro_names:
        series_type = "GroupbySeries"
    elif "Series" in mro_names:
        series_type = "Series"
    else:
        raise NotImplementedError(f"Can't add primitive methods to class of type: {cls.__name__}")  # pragma: no cover

    if series_type == "GroupbySeries":
        target_map = primitive_to_groupby_series_map
        target_module_path = "pylegend.core.language.pandas_api.pandas_api_groupby_series"
    else:
        target_map = primitive_to_series_map
        target_module_path = "pylegend.core.language.pandas_api.pandas_api_series"

    methods_to_wrap = {}
    for base in cls.__mro__:
        for name, attr in base.__dict__.items():
            if name not in methods_to_wrap:
                if callable(attr) and getattr(attr, "_is_grammar_method", False):
                    methods_to_wrap[name] = attr

    for name, original_func in methods_to_wrap.items():
        if name in cls.__dict__ and not getattr(cls.__dict__[name], "_is_grammar_method", False):  # pragma: no cover
            continue

        def make_wrapper(  # type: ignore[explicit-any]
                func: PyLegendCallable[..., PyLegendAny]
        ) -> PyLegendCallable[..., PyLegendAny]:
            def wrapper(  # type: ignore[explicit-any]
                    self: PyLegendAny,
                    *args: PyLegendAny,
                    **kwargs: PyLegendAny
            ) -> PyLegendAny:
                result_primitive = func(self, *args, **kwargs)
                primitive_type_name = type(result_primitive).__name__

                if not hasattr(result_primitive, 'value') or primitive_type_name not in target_map:
                    return result_primitive  # pragma: no cover

                base_frame = self.get_base_frame()
                col_name = self.columns()[0].get_name()

                target_class_str = target_map[primitive_type_name]
                module = importlib.import_module(target_module_path)
                TargetSeriesClass = getattr(module, target_class_str)

                expr = result_primitive.value()
                if series_type == "GroupbySeries":
                    return TargetSeriesClass(base_frame, None, expr)
                else:
                    return TargetSeriesClass(base_frame, col_name, expr)

            return wrapper

        setattr(cls, name, make_wrapper(original_func))

    return cls


def assert_and_find_core_series(expr: PyLegendExpression) -> PyLegendOptional[PyLegendUnion["Series", "GroupbySeries"]]:
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries

    core_series_list: PyLegendList[PyLegendUnion[Series, GroupbySeries]] = []

    sub_expressions = expr.get_leaf_expressions()
    for expr in sub_expressions:
        if isinstance(expr, (Series, GroupbySeries)):
            core_series_list.append(expr)

    if len(core_series_list) == 0:
        return None  # pragma: no cover
    elif len(core_series_list) == 1:
        return core_series_list[0]
    else:
        core_series_with_applied_function = [series for series in core_series_list if series.has_applied_function()]
        if len(core_series_with_applied_function) == 0:
            return core_series_list[0]
        elif len(core_series_with_applied_function) == 1:
            return core_series_with_applied_function[0]

        error_msg = '''
            Only expressions with maximum one Series/GroupbySeries function call (such as .rank()) is supported.
            If multiple Series/GroupbySeries need function calls, please compute them in separate steps.
            For example,
                unsupported:
                    frame['new_col'] = frame['col1'].rank() + 2 + frame['col2'].rank()
                supported:
                    frame['new_col'] = frame['col1'].rank() + 2
                    frame['new_col'] += frame['col2'].rank()
        '''
        error_msg = dedent(error_msg).strip()
        raise ValueError(error_msg)


def has_window_function(series: PyLegendUnion["Series", "GroupbySeries"]) -> bool:
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
    from pylegend.core.tds.pandas_api.frames.functions.rank_function import RankFunction
    from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction
    from pylegend.core.tds.pandas_api.frames.functions.window_aggregate_function import WindowAggregateFunction
    from pylegend.core.tds.pandas_api.frames.functions.zscore_window_function import ZScoreWindowFunction

    if series.expr is not None:
        core_series = assert_and_find_core_series(series.expr)
        assert core_series is not None
        return has_window_function(core_series)

    considered_window_functions = [RankFunction, WindowAggregateFunction, TwoColumnWindowFunction, ZScoreWindowFunction]

    if isinstance(series, Series):
        applied_func = series.get_filtered_frame().get_applied_function()
        return any(isinstance(applied_func, window_function) for window_function in considered_window_functions)

    elif isinstance(series, GroupbySeries):
        applied_func = series.raise_exception_if_no_function_applied().get_applied_function()
        return any(isinstance(applied_func, window_function) for window_function in considered_window_functions)

    else:
        raise TypeError("Window function's existence can only be checked in a Series or a GroupbySeries")  # pragma: no cover


def has_window_aggregate_function(series: PyLegendUnion["Series", "GroupbySeries"]) -> bool:
    """Check if the series (or its core) uses a WindowAggregateFunction (not RankFunction)."""
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
    from pylegend.core.tds.pandas_api.frames.functions.window_aggregate_function import WindowAggregateFunction

    core: PyLegendUnion[Series, GroupbySeries] = series
    if series.expr is not None:
        found = assert_and_find_core_series(series.expr)
        if found is None:
            return False  # pragma: no cover
        core = found

    return isinstance(get_applied_func(core), WindowAggregateFunction)


def has_aggregate_function(series: PyLegendUnion["Series", "GroupbySeries"]) -> bool:
    """Check if the series (or its core) uses an AggregateFunction (not window)."""
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
    from pylegend.core.tds.pandas_api.frames.functions.aggregate_function import AggregateFunction

    core: PyLegendUnion[Series, GroupbySeries] = series
    if series.expr is not None:
        found = assert_and_find_core_series(series.expr)
        if found is None:
            return False  # pragma: no cover
        core = found

    return isinstance(get_applied_func(core), AggregateFunction)


def needs_zero_column_for_window(series: PyLegendUnion["Series", "GroupbySeries"]) -> bool:
    """Check if the series uses a WindowAggregateFunction that requires the zero column.

    Partition-only windows (from transform()) do NOT need the zero column.
    Only expanding/rolling windows need it.
    """
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
    from pylegend.core.tds.pandas_api.frames.functions.window_aggregate_function import WindowAggregateFunction

    core: PyLegendUnion[Series, GroupbySeries] = series
    if series.expr is not None:
        found = assert_and_find_core_series(series.expr)
        if found is None:
            return False  # pragma: no cover
        core = found

    applied_func = get_applied_func(core)
    if isinstance(applied_func, WindowAggregateFunction):
        return not applied_func._is_partition_only()
    return False


def find_window_expression(expr: "Expression") -> "PyLegendOptional[Expression]":
    """Recursively find the first WindowExpression in an expression tree. Returns None if not found."""
    from pylegend.core.sql.metamodel_extension import WindowExpression
    from pylegend.core.sql.metamodel import Expression as SqlExpression

    if isinstance(expr, WindowExpression):
        return expr
    for attr_name in vars(expr):
        child = getattr(expr, attr_name)
        if isinstance(child, SqlExpression):
            result = find_window_expression(child)
            if result is not None:
                return result
    return None


def split_window_from_arithmetic(
    full_expr: "Expression",
) -> "PyLegendTuple[Expression, PyLegendOptional[PyLegendCallable[[Expression], Expression]]]":
    """
    Given a SQL expression that may contain a WindowExpression wrapped in arithmetic,
    return a tuple of:
      - The bare WindowExpression (to go in the inner query)
      - A factory that, given a column reference, returns the outer arithmetic expression
        (or None if the full expression IS the WindowExpression with no arithmetic)

    Example: (SUM(col) OVER (...) - 100)
      returns: (SUM(col) OVER (...), lambda ref: (ref - 100))
    """
    import copy
    from pylegend.core.sql.metamodel_extension import WindowExpression

    if isinstance(full_expr, WindowExpression):
        return full_expr, None

    window = find_window_expression(full_expr)
    if window is None:
        return full_expr, None  # pragma: no cover - no window at all

    def make_outer(col_ref: "Expression") -> "Expression":
        clone = copy.deepcopy(full_expr)
        _replace_window(clone, col_ref)
        return clone

    return window, make_outer


def _replace_window(expr: "Expression", replacement: "Expression") -> bool:
    """Recursively replace the first WindowExpression child with replacement. Returns True if replaced."""
    from pylegend.core.sql.metamodel_extension import WindowExpression
    from pylegend.core.sql.metamodel import Expression as SqlExpression

    for attr_name in vars(expr):
        child = getattr(expr, attr_name)
        if isinstance(child, WindowExpression):
            setattr(expr, attr_name, replacement)
            return True
        if isinstance(child, SqlExpression):
            if _replace_window(child, replacement):
                return True
    return False


def get_pure_query_from_expr(series: PyLegendUnion["Series", "GroupbySeries"], config: FrameToPureConfig) -> str:
    temp_column_name_suffix = "__pylegend_olap_column__"
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
    from pylegend.core.tds.pandas_api.frames.functions.rank_function import RankFunction
    from pylegend.core.tds.pandas_api.frames.functions.two_column_window_function import TwoColumnWindowFunction
    from pylegend.core.tds.pandas_api.frames.functions.window_aggregate_function import WindowAggregateFunction
    from pylegend.core.tds.pandas_api.frames.functions.zscore_window_function import ZScoreWindowFunction

    col_name = series.columns()[0].get_name()
    full_expr = series.expr
    assert full_expr is not None

    has_window_func = False
    extend = ""
    sub_expressions = series.get_leaf_expressions()
    for expr in sub_expressions:
        if isinstance(expr, (Series, GroupbySeries)):
            applied_func = get_applied_func(expr)
            if isinstance(applied_func, RankFunction):
                assert has_window_func is False
                has_window_func = True
                c, window = applied_func.construct_column_expression_and_window_tuples("r")[0]
                window_expr = window.to_pure_expression(config)
                function_expr = c[1].to_pure_expression(config)
                temp_name = escape_column_name(col_name + temp_column_name_suffix)
                extend = f"->extend({window_expr}, ~{temp_name}:{generate_pure_lambda('p,w,r', function_expr)})"
            elif isinstance(applied_func, (TwoColumnWindowFunction, WindowAggregateFunction, ZScoreWindowFunction)):
                assert has_window_func is False
                has_window_func = True
                extend_strs = applied_func.build_pure_extend_strs(temp_column_name_suffix, config)
                extend = config.separator(1).join(extend_strs)

    if has_window_func:
        pure_expr = full_expr.to_pure_expression(config)
        project = f"->project(~[{escape_column_name(col_name)}:c|{pure_expr}])"
    else:
        project = f"->project(~[{escape_column_name(col_name)}:c|{series.to_pure_expression(config)}])"

    if len(extend) > 0:
        extend = config.separator(1) + extend
    project = config.separator(1) + project

    base_frame = series.get_base_frame().base_frame() if isinstance(series, GroupbySeries) else series.get_base_frame()
    return base_frame.to_pure_query(config) + extend + project


def get_applied_func(series: PyLegendUnion["Series", "GroupbySeries"]) -> "PandasApiAppliedFunction":
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries

    if isinstance(series, GroupbySeries):
        return series.raise_exception_if_no_function_applied().get_applied_function()
    else:
        return series.get_filtered_frame().get_applied_function()


def convert_aggregate_series_to_window_aggregate_series(
        series: PyLegendUnion["Series", "GroupbySeries"]
) -> PyLegendUnion["Series", "GroupbySeries"]:
    """
    Convert a Series backed by an AggregateFunction into one backed by a
    WindowAggregateFunction with UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING.

    If the series has arithmetic on top (``series.expr is not None``), the
    arithmetic wrapper is preserved — only the leaf AggregateFunction core
    is swapped out for a WindowAggregateFunction equivalent.
    """
    from pylegend.core.tds.pandas_api.frames.functions.aggregate_function import AggregateFunction

    core_series = series if series.expr is None else assert_and_find_core_series(series.expr)
    assert core_series is not None
    applied_func_frame = get_applied_func(core_series)
    if not isinstance(applied_func_frame, AggregateFunction):
        return series  # pragma: no cover

    core_series_with_window = _convert_core_aggregate_series_to_window_aggregate_seroes(core_series)
    if series.expr is None:
        return core_series_with_window

    series_with_new_expr = copy.copy(series)
    series_with_new_expr._expr = _replace_core_series_in_expr(series.expr, core_series_with_window)
    return series_with_new_expr


def _convert_core_aggregate_series_to_window_aggregate_seroes(
        core_series: PyLegendUnion["Series", "GroupbySeries"]
) -> PyLegendUnion["Series", "GroupbySeries"]:
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
    from pylegend.core.tds.pandas_api.frames.functions.aggregate_function import AggregateFunction
    from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame
    from pylegend.core.language.pandas_api.pandas_api_frame_spec import RowsBetween
    from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

    applied_func_frame = get_applied_func(core_series)
    assert (
        core_series.expr is None
        and isinstance(applied_func_frame, AggregateFunction)
    )

    window_frame = PandasApiWindowTdsFrame(
        base_frame=core_series.get_base_frame(),
        order_by=None,
        frame_spec=RowsBetween(None, None),
    )

    column_name: str
    if isinstance(core_series, GroupbySeries):
        num_grouping_cols = len(core_series.get_base_frame().get_grouping_columns())
        column_name = core_series.columns()[num_grouping_cols].get_name()
    else:
        column_name = core_series.columns()[0].get_name()
    window_series = WindowSeries(window_frame=window_frame, column_name=column_name)
    core_series_with_window = window_series.aggregate(func=applied_func_frame.func)
    assert isinstance(core_series_with_window, (Series, GroupbySeries))
    return core_series_with_window


def _replace_core_series_in_expr(
        expr: PyLegendExpression,
        core_series_with_window: PyLegendUnion["Series", "GroupbySeries"]
) -> PyLegendExpression:
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
    from pylegend.core.tds.pandas_api.frames.functions.aggregate_function import AggregateFunction

    cloned = copy.deepcopy(expr)
    visited: set[int] = set()

    def condition_for_replacement(leaf_series: PyLegendUnion["Series", "GroupbySeries"]) -> bool:
        assert isinstance(leaf_series, (Series, GroupbySeries)) and leaf_series.expr is None
        applied_func = get_applied_func(leaf_series)
        if isinstance(applied_func, AggregateFunction):
            return True
        return False  # pragma: no cover

    _recursively_replace_leaf_when_meets_condition(cloned, core_series_with_window, visited, condition_for_replacement)
    return cloned


def _recursively_replace_leaf_when_meets_condition(
        expr: PyLegendExpression,
        new_leaf: PyLegendUnion["Series", "GroupbySeries"],
        visited: set[int],
        condition_for_replacement: PyLegendCallable[[PyLegendUnion["Series", "GroupbySeries"]], bool],
) -> None:
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries

    obj_id = id(expr)
    if obj_id in visited:
        return  # pragma: no cover
    visited.add(obj_id)

    for attr_name in list(vars(expr)):
        child = getattr(expr, attr_name)
        if isinstance(child, (Series, GroupbySeries)) and child.expr is None:
            if condition_for_replacement(child):
                setattr(expr, attr_name, new_leaf)
        elif isinstance(child, PyLegendExpression):
            _recursively_replace_leaf_when_meets_condition(child, new_leaf, visited, condition_for_replacement)
