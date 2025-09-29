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

import pytest
from pylegend.utils.class_utils import find_sub_classes


class A:
    pass


class B:
    pass


class C(A, B):
    pass


class D(C):
    pass


class TestClassUtils:

    def test_find_subclasses_non_recursive(self) -> None:
        assert [] == find_sub_classes(D, False)
        assert {D} == set(find_sub_classes(C, False))
        assert {C} == set(find_sub_classes(B, False))
        assert {C} == set(find_sub_classes(A, False))

    def test_find_subclasses_recursive(self) -> None:
        assert [] == find_sub_classes(D)
        assert {D} == set(find_sub_classes(C))
        assert {C, D} == set(find_sub_classes(B))
        assert {C, D} == set(find_sub_classes(A))

def test_find_subclasses_raises_type_error_on_non_class() -> None:
    with pytest.raises(TypeError):
        find_sub_classes(123)  # type: ignore[arg-type]


def test_find_subclasses_returns_empty_list_for_class_without_subclasses() -> None:
    class Solo:
        pass

    assert find_sub_classes(Solo, recursive=True) == []
