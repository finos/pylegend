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
    "add_primitive_methods",
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
