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

import pytest  # type: ignore

from pylegend.core.tds_column import PyLegendTdsColumn


class TestPyLegendTdsColumn:

    def test_tds_column_creation(self) -> None:
        c1 = PyLegendTdsColumn('C1', 'Integer')
        assert "PyLegendTdsColumn(Name: C1, Type: Integer)" == str(c1)

        c2 = PyLegendTdsColumn('C2', 'Float')
        assert "PyLegendTdsColumn(Name: C2, Type: Float)" == str(c2)

        c3 = PyLegendTdsColumn('C3', 'String')
        assert "PyLegendTdsColumn(Name: C3, Type: String)" == str(c3)

        c4 = PyLegendTdsColumn('C4', 'Boolean')
        assert "PyLegendTdsColumn(Name: C4, Type: Boolean)" == str(c4)

    def test_error_on_unknown_type(self) -> None:
        with pytest.raises(ValueError, match="Unknown type provided for TDS column: 'SomeType'. "
                                             "Known types are: Integer, Float, String, Boolean"):
            PyLegendTdsColumn('C1', 'SomeType')
