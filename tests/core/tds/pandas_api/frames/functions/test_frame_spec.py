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

# type: ignore
# flake8: noqa

"""Tests for frame spec classes and range_between functionality."""

import pytest

from pylegend.core.language.pandas_api.pandas_api_frame_spec import (
    FrameSpec,
    RowsBetween,
    RangeBetween,
)
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiDurationUnit,
    PandasApiFrameBoundType,
    PandasApiFrameBound,
    PandasApiWindowFrameMode,
)
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn


class TestRowsBetweenValidation:
    """Tests for RowsBetween validation."""

    def test_rows_between_start_greater_than_end_raises(self) -> None:
        """RowsBetween raises ValueError if start > end."""
        with pytest.raises(ValueError) as exc:
            RowsBetween(start=5, end=2)
        assert "lower bound of window frame cannot be greater than the upper bound" in str(exc.value)

    def test_rows_between_valid_unbounded_start(self) -> None:
        """RowsBetween with None start (unbounded preceding) works."""
        rb = RowsBetween(start=None, end=0)
        start_bound = rb.build_start_bound()
        assert start_bound.type_ == PandasApiFrameBoundType.UNBOUNDED_PRECEDING

    def test_rows_between_valid_unbounded_end(self) -> None:
        """RowsBetween with None end (unbounded following) works."""
        rb = RowsBetween(start=0, end=None)
        end_bound = rb.build_end_bound()
        assert end_bound.type_ == PandasApiFrameBoundType.UNBOUNDED_FOLLOWING


class TestRangeBetweenSimple:
    """Tests for RangeBetween with simple numeric bounds."""

    def test_range_between_simple_bounds(self) -> None:
        """RangeBetween with simple start/end works."""
        rb = RangeBetween(start=-100, end=0)
        start_bound = rb.build_start_bound()
        end_bound = rb.build_end_bound()
        assert start_bound.type_ == PandasApiFrameBoundType.PRECEDING
        assert start_bound.value == 100
        assert end_bound.type_ == PandasApiFrameBoundType.CURRENT_ROW

    def test_range_between_unbounded_both(self) -> None:
        """RangeBetween with None for both (unbounded) works."""
        rb = RangeBetween(start=None, end=None)
        start_bound = rb.build_start_bound()
        end_bound = rb.build_end_bound()
        assert start_bound.type_ == PandasApiFrameBoundType.UNBOUNDED_PRECEDING
        assert end_bound.type_ == PandasApiFrameBoundType.UNBOUNDED_FOLLOWING

    def test_range_between_following(self) -> None:
        """RangeBetween with positive end value works."""
        rb = RangeBetween(start=0, end=10)
        end_bound = rb.build_end_bound()
        assert end_bound.type_ == PandasApiFrameBoundType.FOLLOWING
        assert end_bound.value == 10

    def test_range_between_start_greater_than_end_raises(self) -> None:
        """RangeBetween raises ValueError if start > end."""
        with pytest.raises(ValueError) as exc:
            RangeBetween(start=10, end=-5)
        assert "lower bound of window frame cannot be greater than the upper bound" in str(exc.value)


class TestRangeBetweenDuration:
    """Tests for RangeBetween with duration-based bounds."""

    def test_range_between_duration_bounds(self) -> None:
        """RangeBetween with duration_start/duration_end works."""
        rb = RangeBetween(
            duration_start=-1,
            duration_start_unit="DAYS",
            duration_end=1,
            duration_end_unit="MONTHS",
        )
        start_bound = rb.build_start_bound()
        end_bound = rb.build_end_bound()
        assert start_bound.type_ == PandasApiFrameBoundType.PRECEDING
        assert start_bound.value == 1
        assert start_bound.duration_unit == PandasApiDurationUnit.DAYS
        assert end_bound.type_ == PandasApiFrameBoundType.FOLLOWING
        assert end_bound.value == 1
        assert end_bound.duration_unit == PandasApiDurationUnit.MONTHS

    def test_range_between_duration_unbounded_string(self) -> None:
        """RangeBetween with 'unbounded' string works."""
        rb = RangeBetween(
            duration_start="unbounded",
            duration_end=0,
            duration_end_unit="DAYS",
        )
        start_bound = rb.build_start_bound()
        end_bound = rb.build_end_bound()
        assert start_bound.type_ == PandasApiFrameBoundType.UNBOUNDED_PRECEDING
        assert end_bound.type_ == PandasApiFrameBoundType.CURRENT_ROW

    def test_range_between_duration_none_value(self) -> None:
        """RangeBetween with None duration value works."""
        rb = RangeBetween(
            duration_start=None,
            duration_end=5,
            duration_end_unit="HOURS",
        )
        start_bound = rb.build_start_bound()
        assert start_bound.type_ == PandasApiFrameBoundType.UNBOUNDED_PRECEDING

    def test_range_between_invalid_duration_string(self) -> None:
        """RangeBetween raises ValueError for invalid duration string."""
        with pytest.raises(ValueError) as exc:
            RangeBetween(
                duration_start="invalid",
                duration_start_unit="DAYS",
            )
        assert "string value must be 'unbounded'" in str(exc.value)

    def test_range_between_mixed_simple_and_duration_raises(self) -> None:
        """RangeBetween raises ValueError if mixing simple and duration bounds."""
        with pytest.raises(ValueError) as exc:
            RangeBetween(
                start=-100,
                duration_end=1,
                duration_end_unit="DAYS",
            )
        assert "Cannot mix positional start/end with duration_start/duration_end" in str(exc.value)


class TestRangeBetweenFunction:
    """Tests for the range_between helper function."""
    columns = [PrimitiveTdsColumn.string_column("col1")]
    frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

    def test_range_between_function_simple(self) -> None:
        """range_between() function with simple bounds works."""
        rb = self.frame.range_between(start=-10, end=10)
        assert isinstance(rb, RangeBetween)
        start_bound = rb.build_start_bound()
        end_bound = rb.build_end_bound()
        assert start_bound.type_ == PandasApiFrameBoundType.PRECEDING
        assert end_bound.type_ == PandasApiFrameBoundType.FOLLOWING

    def test_range_between_function_duration(self) -> None:
        """range_between() function with duration bounds works."""
        rb = self.frame.range_between(
            duration_start=-1,
            duration_start_unit="WEEKS",
            duration_end=2,
            duration_end_unit="DAYS",
        )
        assert isinstance(rb, RangeBetween)
        start_bound = rb.build_start_bound()
        end_bound = rb.build_end_bound()
        assert start_bound.duration_unit == PandasApiDurationUnit.WEEKS
        assert end_bound.duration_unit == PandasApiDurationUnit.DAYS

    def test_range_between_function_defaults(self) -> None:
        """range_between() function with defaults (unbounded both)."""
        rb = self.frame.range_between()
        assert isinstance(rb, RangeBetween)
        start_bound = rb.build_start_bound()
        end_bound = rb.build_end_bound()
        assert start_bound.type_ == PandasApiFrameBoundType.UNBOUNDED_PRECEDING
        assert end_bound.type_ == PandasApiFrameBoundType.UNBOUNDED_FOLLOWING


class TestDurationUnitFromString:
    """Tests for PandasApiDurationUnit.from_string."""

    def test_valid_duration_units(self) -> None:
        """Valid duration unit strings are parsed correctly."""
        assert PandasApiDurationUnit.from_string("YEARS") == PandasApiDurationUnit.YEARS
        assert PandasApiDurationUnit.from_string("months") == PandasApiDurationUnit.MONTHS
        assert PandasApiDurationUnit.from_string("Weeks") == PandasApiDurationUnit.WEEKS
        assert PandasApiDurationUnit.from_string("days") == PandasApiDurationUnit.DAYS
        assert PandasApiDurationUnit.from_string("HOURS") == PandasApiDurationUnit.HOURS
        assert PandasApiDurationUnit.from_string("minutes") == PandasApiDurationUnit.MINUTES
        assert PandasApiDurationUnit.from_string("SECONDS") == PandasApiDurationUnit.SECONDS
        assert PandasApiDurationUnit.from_string("MILLISECONDS") == PandasApiDurationUnit.MILLISECONDS
        assert PandasApiDurationUnit.from_string("MICROSECONDS") == PandasApiDurationUnit.MICROSECONDS
        assert PandasApiDurationUnit.from_string("NANOSECONDS") == PandasApiDurationUnit.NANOSECONDS

    def test_invalid_duration_unit_raises(self) -> None:
        """Invalid duration unit string raises ValueError."""
        with pytest.raises(ValueError) as exc:
            PandasApiDurationUnit.from_string("invalid_unit")
        assert "Invalid duration unit" in str(exc.value)


class TestRowsBetweenFunction:
    """Tests for the rows_between helper function."""
    columns = [PrimitiveTdsColumn.string_column("col1")]
    frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

    def test_rows_between_function_creates_instance(self) -> None:
        """rows_between() function creates RowsBetween instance."""
        rb = self.frame.rows_between(start=-5, end=5)
        assert isinstance(rb, RowsBetween)
        start_bound = rb.build_start_bound()
        end_bound = rb.build_end_bound()
        assert start_bound.type_ == PandasApiFrameBoundType.PRECEDING
        assert start_bound.value == 5
        assert end_bound.type_ == PandasApiFrameBoundType.FOLLOWING
        assert end_bound.value == 5


class TestPandasApiDurationUnitMethods:
    """Tests for PandasApiDurationUnit to_pure_expression and to_sql_node methods."""

    def test_duration_unit_to_pure_expression(self) -> None:
        """to_pure_expression() returns the enum name."""
        config = FrameToPureConfig()
        assert PandasApiDurationUnit.YEARS.to_pure_expression(config) == "YEARS"
        assert PandasApiDurationUnit.MONTHS.to_pure_expression(config) == "MONTHS"
        assert PandasApiDurationUnit.WEEKS.to_pure_expression(config) == "WEEKS"
        assert PandasApiDurationUnit.DAYS.to_pure_expression(config) == "DAYS"
        assert PandasApiDurationUnit.HOURS.to_pure_expression(config) == "HOURS"
        assert PandasApiDurationUnit.MINUTES.to_pure_expression(config) == "MINUTES"
        assert PandasApiDurationUnit.SECONDS.to_pure_expression(config) == "SECONDS"
        assert PandasApiDurationUnit.MILLISECONDS.to_pure_expression(config) == "MILLISECONDS"
        assert PandasApiDurationUnit.MICROSECONDS.to_pure_expression(config) == "MICROSECONDS"
        assert PandasApiDurationUnit.NANOSECONDS.to_pure_expression(config) == "NANOSECONDS"

    def test_duration_unit_to_sql_node(self) -> None:
        """to_sql_node() returns a StringLiteral with the SQL unit name."""
        from pylegend.core.sql.metamodel import StringLiteral, QuerySpecification, Select, AllColumns

        # Create a minimal query for the test
        query = QuerySpecification(
            select=Select(selectItems=[AllColumns(prefix=None)], distinct=False),
            from_=[],
            where=None,
            groupBy=[],
            having=None,
            orderBy=[],
            limit=None,
            offset=None,
        )
        config = FrameToSqlConfig()

        result = PandasApiDurationUnit.DAYS.to_sql_node(query, config)
        assert isinstance(result, StringLiteral)
        assert result.value == "DAY"

        result = PandasApiDurationUnit.YEARS.to_sql_node(query, config)
        assert result.value == "YEAR"

        result = PandasApiDurationUnit.MONTHS.to_sql_node(query, config)
        assert result.value == "MONTH"

        result = PandasApiDurationUnit.WEEKS.to_sql_node(query, config)
        assert result.value == "WEEK"

        result = PandasApiDurationUnit.HOURS.to_sql_node(query, config)
        assert result.value == "HOUR"

        result = PandasApiDurationUnit.MINUTES.to_sql_node(query, config)
        assert result.value == "MINUTE"

        result = PandasApiDurationUnit.SECONDS.to_sql_node(query, config)
        assert result.value == "SECOND"

        result = PandasApiDurationUnit.MILLISECONDS.to_sql_node(query, config)
        assert result.value == "MILLISECOND"

        result = PandasApiDurationUnit.MICROSECONDS.to_sql_node(query, config)
        assert result.value == "MICROSECOND"

        result = PandasApiDurationUnit.NANOSECONDS.to_sql_node(query, config)
        assert result.value == "NANOSECOND"


class TestPandasApiFrameBoundTypeMethods:
    """Tests for PandasApiFrameBoundType.to_pure_expression method."""

    def test_frame_bound_type_to_pure_expression(self) -> None:
        """to_pure_expression() returns the correct Pure expression."""
        assert PandasApiFrameBoundType.UNBOUNDED_PRECEDING.to_pure_expression() == "unbounded()"
        assert PandasApiFrameBoundType.PRECEDING.to_pure_expression() == "0"
        assert PandasApiFrameBoundType.CURRENT_ROW.to_pure_expression() == "0"
        assert PandasApiFrameBoundType.FOLLOWING.to_pure_expression() == "0"
        assert PandasApiFrameBoundType.UNBOUNDED_FOLLOWING.to_pure_expression() == "unbounded()"


class TestPandasApiFrameBoundMethods:
    """Tests for PandasApiFrameBound.to_pure_expression method branches."""

    def test_frame_bound_to_pure_with_duration_unit(self) -> None:
        """to_pure_expression() includes duration unit when present."""
        config = FrameToPureConfig()

        # PRECEDING with duration unit
        bound = PandasApiFrameBound(
            type_=PandasApiFrameBoundType.PRECEDING,
            value=5,
            duration_unit=PandasApiDurationUnit.DAYS,
        )
        result = bound.to_pure_expression(config)
        assert "minus(5)" in result
        assert "DurationUnit.DAYS" in result

        # FOLLOWING with duration unit
        bound = PandasApiFrameBound(
            type_=PandasApiFrameBoundType.FOLLOWING,
            value=3,
            duration_unit=PandasApiDurationUnit.HOURS,
        )
        result = bound.to_pure_expression(config)
        assert "3" in result
        assert "DurationUnit.HOURS" in result

    def test_frame_bound_to_pure_without_value_uses_type_expression(self) -> None:
        """to_pure_expression() uses type's expression when value is None and not unbounded."""
        config = FrameToPureConfig()

        # PRECEDING without value - should use type's to_pure_expression
        bound = PandasApiFrameBound(
            type_=PandasApiFrameBoundType.PRECEDING,
            value=None,
            duration_unit=None,
        )
        result = bound.to_pure_expression(config)
        assert result == "0"  # Falls through to type_.to_pure_expression()

        # FOLLOWING without value
        bound = PandasApiFrameBound(
            type_=PandasApiFrameBoundType.FOLLOWING,
            value=None,
            duration_unit=None,
        )
        result = bound.to_pure_expression(config)
        assert result == "0"

    def test_frame_bound_should_use_value_returns_false_for_unbounded(self) -> None:
        """_should_use_value() returns False for UNBOUNDED types even with value set."""
        # UNBOUNDED_PRECEDING with value should still return False
        bound = PandasApiFrameBound(
            type_=PandasApiFrameBoundType.UNBOUNDED_PRECEDING,
            value=5,  # This value should be ignored
            duration_unit=None,
        )
        assert bound._should_use_value() is False

        # UNBOUNDED_FOLLOWING with value should still return False
        bound = PandasApiFrameBound(
            type_=PandasApiFrameBoundType.UNBOUNDED_FOLLOWING,
            value=10,  # This value should be ignored
            duration_unit=None,
        )
        assert bound._should_use_value() is False

    def test_frame_bound_to_pure_unbounded_with_value_ignores_value(self) -> None:
        """to_pure_expression() for UNBOUNDED types ignores any value set."""
        config = FrameToPureConfig()

        # UNBOUNDED_PRECEDING with value should return "unbounded()"
        bound = PandasApiFrameBound(
            type_=PandasApiFrameBoundType.UNBOUNDED_PRECEDING,
            value=100,
            duration_unit=None,
        )
        result = bound.to_pure_expression(config)
        assert result == "unbounded()"

        # UNBOUNDED_FOLLOWING with value should return "unbounded()"
        bound = PandasApiFrameBound(
            type_=PandasApiFrameBoundType.UNBOUNDED_FOLLOWING,
            value=100,
            duration_unit=None,
        )
        result = bound.to_pure_expression(config)
        assert result == "unbounded()"


class TestPandasApiWindowFrameModeMethods:
    """Tests for PandasApiWindowFrameMode.to_pure_expression method."""

    def test_window_frame_mode_to_pure_expression(self) -> None:
        """to_pure_expression() returns the correct Pure expression."""
        assert PandasApiWindowFrameMode.RANGE.to_pure_expression() == "_range"
        assert PandasApiWindowFrameMode.ROWS.to_pure_expression() == "rows"
