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
    PyLegendSequence,
)
from pylegend.core.language.shared.expression import PyLegendExpression


__all__: PyLegendSequence[str] = [
    "add_primitive_methods",
    "assert_max_one_window_in_expr",
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
        raise NotImplementedError(f"Can't add primitive methods to class of type: {cls.__name__}")

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


def assert_max_one_window_in_expr(expr: PyLegendExpression) -> None:
    from pylegend.core.language.pandas_api.pandas_api_series import Series
    from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
    from pylegend.core.tds.pandas_api.frames.functions.rank_function import RankFunction

    window_functions = []

    sub_expressions = expr.get_sub_expressions()
    for expr in sub_expressions:
        if isinstance(expr, Series):
            applied_func = expr.get_filtered_frame().get_applied_function()
        elif isinstance(expr, GroupbySeries):
            applied_func = expr.raise_exception_if_no_function_applied().get_applied_function()
        else:
            continue

        if isinstance(applied_func, RankFunction):
            window_functions.append(applied_func)

    error_msg = '''
        Cannot process multiple window expressions in a single PyLegend expression.
        For example,
            instead of: frame['new_col'] = frame['col1'].rank() + 2 + frame['col2'].rank()
            do: frame['new_col'] = frame['col1'].rank() + 2; frame['new_col'] += frame['col2'].rank()
    '''
    error_msg = dedent(error_msg).strip()
    assert len(window_functions) <= 1, error_msg
