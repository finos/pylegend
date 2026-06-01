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

from typing import TypeVar, Callable
from pylegend._typing import PyLegendSequence

__all__: PyLegendSequence[str] = ["grammar_method"]

F = TypeVar('F', bound=Callable)  # type: ignore[type-arg]


def grammar_method(func: F) -> F:
    """Mark a method as a grammar method by setting the _is_grammar_method attribute."""
    setattr(func, "_is_grammar_method", True)
    return func
