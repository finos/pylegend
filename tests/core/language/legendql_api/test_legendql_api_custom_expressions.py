# Copyright 2025 Goldman Sachs
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
from pylegend.core.language.legendql_api.legendql_api_custom_expressions import (
    LegendQLApiDurationUnit,
    LegendQLApiWindowFrameBoundType,
    LegendQLApiWindowFrameBound)


class TestLegendQLApiCustomExpressions:

    def test_duration_unit_exceptions(self) -> None:
        with pytest.raises(ValueError) as r:
            LegendQLApiDurationUnit.from_string("test")
        assert r.value.args[0] == (
            "Invalid duration unit 'test'. Supported values: ['years', 'months', 'weeks', 'days', "
            "'hours', 'minutes', 'seconds', 'milliseconds', 'microseconds', 'nanoseconds']"
        )

    def test_frame_bound_exceptions(self) -> None:
        with pytest.raises(ValueError) as r:
            LegendQLApiWindowFrameBound(LegendQLApiWindowFrameBoundType.PRECEDING)
        assert r.value.args[0] == (
            "row_offset must be provided for bound_type PRECEDING"
        )

        with pytest.raises(ValueError) as r:
            LegendQLApiWindowFrameBound(LegendQLApiWindowFrameBoundType.UNBOUNDED_FOLLOWING, 1)
        assert r.value.args[0] == (
            "row_offset is not allowed for bound_type UNBOUNDED_FOLLOWING"
        )
