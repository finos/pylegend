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
from typing import (
    Type,
    TypeVar,
)
from pylegend._typing import (
    PyLegendAny,
    PyLegendCallable,
    PyLegendSequence,
)


__all__: PyLegendSequence[str] = [
    "wrap_primitive_methods",
]

T = TypeVar("T")


def wrap_primitive_methods(cls: Type[T]) -> Type[T]:
    primitive_to_series_map = {
        "PyLegendInteger": "IntegerSeries",
        "PyLegendFloat": "FloatSeries",
        "PyLegendNumber": "NumberSeries",
        "PyLegendBoolean": "BooleanSeries",
        "PyLegendString": "StringSeries",
        "PyLegendDate": "DateSeries",
        "PyLegendDateTime": "DateTimeSeries",
        "PyLegendStrictDate": "StrictDateSeries"
    }

    series_methods_map = {
        "IntegerSeries": [
            'char', '__add__', '__radd__', '__sub__', '__rsub__', '__mul__', '__rmul__', '__mod__', '__rmod__',
            '__abs__', '__neg__', '__pos__', '__invert__', '__and__', '__rand__', '__or__', '__ror__', '__xor__', '__rxor__',
            '__lshift__', '__rlshift__', '__rshift__', '__rrshift__',
        ],
        "FloatSeries": [
            '__add__', '__radd__', '__sub__', '__rsub__', '__mul__', '__rmul__', '__abs__', '__neg__', '__pos__',
        ],
        "NumberSeries": [
            '__add__', '__radd__', '__mul__', '__rmul__', '__truediv__', '__rtruediv__', '__sub__', '__rsub__', '__lt__',
            '__le__', '__gt__', '__ge__', '__pos__', '__neg__', '__abs__', '__pow__', '__rpow__', 'ceil', '__ceil__', 'floor',
            '__floor__', 'sqrt', 'cbrt', 'exp', 'log', 'rem', 'sin', 'asin', 'cos', 'acos', 'tan', 'atan', 'atan2', 'cot',
            'round', 'log10', 'degrees', 'radians', 'sign', 'sinh', 'cosh', 'tanh', '__round__',
        ],
        "BooleanSeries": [
            '__or__', '__ror__', '__and__', '__rand__', '__invert__',
            '__lt__', '__le__', '__gt__', '__ge__', '__xor__', '__rxor__'
        ],
        "StringSeries": [
            'len', 'length', 'startswith', 'endswith', 'contains', 'upper', 'lower', 'lstrip', 'rstrip', 'strip', 'index_of',
            'index', 'parse_int', 'parse_integer', 'parse_float', 'parse_boolean', 'parse_datetime', 'ascii', 'b64decode',
            'b64encode', 'reverse', 'to_lower_first_character', 'to_upper_first_character', 'left', 'right', 'substring',
            'replace', 'rjust', 'ljust', 'split_part', 'full_match', 'match', 'repeat_string', 'coalesce', '__add__',
            '__radd__', '__lt__', '__le__', '__gt__', '__ge__',
        ],
        "DateSeries": [
             'first_day_of_year', 'first_day_of_quarter', 'first_day_of_month', 'first_day_of_week', 'first_hour_of_day',
             'first_minute_of_hour', 'first_second_of_minute', 'first_millisecond_of_second', 'year', 'month', 'day', 'hour',
             'minute', 'second', 'epoch_value', 'quarter', 'week_of_year', 'day_of_year', 'day_of_week', 'date_part',
             'timedelta', 'diff', '__lt__', '__le__', '__gt__', '__ge__'
        ],
        "DateTimeSeries": [
            'time_bucket',
        ],
        "StrictDateSeries": [
            'time_bucket',
        ]
    }

    methods_to_wrap = series_methods_map.get(cls.__name__, [])

    for name in methods_to_wrap:
        if name in cls.__dict__:
            continue

        original_func = getattr(cls, name, None)

        if not (original_func and callable(original_func)):
            continue

        def make_wrapper(
                func: PyLegendCallable[..., PyLegendAny]
        ) -> PyLegendCallable[..., PyLegendAny]:  # type: ignore[explicit-any]
            def wrapper(
                    self: PyLegendAny,
                    *args: PyLegendAny,
                    **kwargs: PyLegendAny
            ) -> PyLegendAny:  # type: ignore[explicit-any]
                result_primitive = func(self, *args, **kwargs)
                primitive_type_name = type(result_primitive).__name__

                if not hasattr(result_primitive, 'value') or primitive_type_name not in primitive_to_series_map:
                    return result_primitive

                base_frame = self._base_frame
                col_name = self.columns()[0].get_name()

                target_class_str = primitive_to_series_map[primitive_type_name]
                module = importlib.import_module("pylegend.core.language.pandas_api.pandas_api_series")
                TargetSeriesClass = getattr(module, target_class_str)

                expr = result_primitive.value()
                return TargetSeriesClass(base_frame, col_name, expr)

            return wrapper

        setattr(cls, name, make_wrapper(original_func))

    return cls
