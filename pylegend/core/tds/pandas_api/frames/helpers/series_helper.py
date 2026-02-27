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


import importlib
from textwrap import dedent
from typing import (
    Type,
    TypeVar,
)
from pylegend._typing import (
    PyLegendAny,
    PyLegendCallable,
    PyLegendList,
    PyLegendOptional,
    PyLegendSequence,
    PyLegendUnion,
    TYPE_CHECKING,
)
from pylegend.core.language.shared.expression import PyLegendExpression
if TYPE_CHECKING:
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries


__all__: PyLegendSequence[str] = [
    "add_primitive_methods",
    "assert_and_find_core_series",
    "has_window_function",
]

T = TypeVar("T")
F = TypeVar("F", bound=PyLegendCallable[..., PyLegendAny])  # type: ignore[explicit-any]


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
    from pylegend.core.tds.pandas_api.frames.functions.shift_function import ShiftFunction

    if series.expr is not None:
        core_series = assert_and_find_core_series(series.expr)
        assert core_series is not None
        return has_window_function(core_series)

    considered_window_functions = [RankFunction, ShiftFunction]

    if isinstance(series, Series):
        applied_func = series.get_filtered_frame().get_applied_function()
        return any(isinstance(applied_func, window_function) for window_function in considered_window_functions)

    elif isinstance(series, GroupbySeries):
        applied_func = series.raise_exception_if_no_function_applied().get_applied_function()
        return any(isinstance(applied_func, window_function) for window_function in considered_window_functions)

    else:
        raise TypeError("Window function's existence can only be checked in a Series or a GroupbySeries")  # pragma: no cover
