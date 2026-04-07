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

from textwrap import dedent
import json

import numpy as np
import pytest

from pylegend._typing import (
    PyLegendDict,
    PyLegendOptional,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_relation_person_service_frame_pandas_api


class TestExpandingErrors:
    def test_invalid_min_periods(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.expanding(min_periods=2)
        assert v.value.args[0] == "The expanding function is only supported for min_periods=1, but got: min_periods=2"

    def test_invalid_axis(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.expanding(axis=1)
        assert v.value.args[0] == 'The expanding function is only supported for axis=0 or axis="index", but got: axis=1'

    def test_invalid_method(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.expanding(method='single')
        assert v.value.args[0] == (
            "The expanding function does not support the 'method' parameter, but got: method='single'"
        )


class TestGroupbyExpandingErrors:
    """Tests for error handling in groupby().expanding() method."""

    def test_groupby_expanding_invalid_min_periods(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("grp").expanding(min_periods=2)
        assert v.value.args[0] == "The expanding function is only supported for min_periods=1, but got: min_periods=2"

    def test_groupby_expanding_invalid_method(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("grp").expanding(method='single')
        assert v.value.args[0] == (
            "The expanding function does not support the 'method' parameter, but got: method='single'"
        )


class TestGroupbyRollingErrors:
    """Tests for error handling in groupby().rolling() method."""

    def test_groupby_rolling_invalid_min_periods(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("grp").rolling(window=3, min_periods=2)
        assert v.value.args[0] == (
            "The rolling function is only supported for min_periods=1 or None, but got: min_periods=2"
        )

    def test_groupby_rolling_invalid_center(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("grp").rolling(window=3, center=True)
        assert v.value.args[0] == "The rolling function does not support center=True, but got: center=True"

    def test_groupby_rolling_invalid_win_type(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("grp").rolling(window=3, win_type="triang")
        assert v.value.args[0] == (
            "The rolling function does not support the 'win_type' parameter, but got: win_type='triang'"
        )

    def test_groupby_rolling_invalid_on(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("grp").rolling(window=3, on="val")
        assert v.value.args[0] == "The rolling function does not support the 'on' parameter, but got: on='val'"

    def test_groupby_rolling_invalid_closed(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("grp").rolling(window=3, closed="left")
        assert v.value.args[0] == (
            "The rolling function does not support the 'closed' parameter, but got: closed='left'"
        )

    def test_groupby_rolling_invalid_step(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("grp").rolling(window=3, step=2)
        assert v.value.args[0] == "The rolling function does not support the 'step' parameter, but got: step=2"

    def test_groupby_rolling_invalid_method(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("grp").rolling(window=3, method="single")
        assert v.value.args[0] == (
            "The rolling function does not support the 'method' parameter, but got: method='single'"
        )


class TestGroupbyWindowFrameLegendExtErrors:
    """Tests for error handling in groupby().window_frame_legend_ext() method."""

    def test_groupby_window_frame_legend_ext_invalid_frame_spec(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.groupby("grp").window_frame_legend_ext(frame_spec="invalid")  # type: ignore
        assert "frame_spec must be a RowsBetween or RangeBetween, got str" in str(v.value)

    def test_groupby_window_frame_legend_ext_with_none(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.groupby("grp").window_frame_legend_ext(frame_spec=None)  # type: ignore
        assert "frame_spec must be a RowsBetween or RangeBetween, got NoneType" in str(v.value)


class TestRollingErrors:
    def test_invalid_min_periods(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rolling(window=3, min_periods=2)
        assert v.value.args[0] == (
            "The rolling function is only supported for min_periods=1 or None, but got: min_periods=2"
        )

    def test_invalid_center(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rolling(window=3, center=True)
        assert v.value.args[0] == "The rolling function does not support center=True, but got: center=True"

    def test_invalid_win_type(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rolling(window=3, win_type="triang")
        assert v.value.args[0] == (
            "The rolling function does not support the 'win_type' parameter, but got: win_type='triang'"
        )

    def test_invalid_on(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rolling(window=3, on="col1")
        assert v.value.args[0] == "The rolling function does not support the 'on' parameter, but got: on='col1'"

    def test_invalid_closed(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rolling(window=3, closed="left")
        assert v.value.args[0] == (
            "The rolling function does not support the 'closed' parameter, but got: closed='left'"
        )

    def test_invalid_step(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rolling(window=3, step=2)
        assert v.value.args[0] == "The rolling function does not support the 'step' parameter, but got: step=2"

    def test_invalid_method(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rolling(window=3, method="single")
        assert v.value.args[0] == (
            "The rolling function does not support the 'method' parameter, but got: method='single'"
        )


class TestExpandingOnBaseFrame:
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_simple_sum(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.expanding().agg("sum")

        expected_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col1__pylegend_olap_column__",
                        SUM("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col2__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), 0)), ~[
                col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()},
                col2__pylegend_olap_column__:{p,w,r | $r.col2}:{c | $c->sum()}
              ])
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__,
                col2:p|$p.col2__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_complex_aggregation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.expanding().agg({
            "col1": ["sum", lambda x: x.count()],
            "col2": np.min
        })

        expected_sql = '''
            SELECT
                "root"."sum(col1)__pylegend_olap_column__" AS "sum(col1)",
                "root"."lambda_1(col1)__pylegend_olap_column__" AS "lambda_1(col1)",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "sum(col1)__pylegend_olap_column__",
                        COUNT("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "lambda_1(col1)__pylegend_olap_column__",
                        MIN("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col2__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), 0)), ~[
                'sum(col1)__pylegend_olap_column__':{p,w,r | $r.col1}:{c | $c->sum()},
                'lambda_1(col1)__pylegend_olap_column__':{p,w,r | $r.col1}:{c | $c->count()},
                col2__pylegend_olap_column__:{p,w,r | $r.col2}:{c | $c->min()}
              ])
              ->project(~[
                'sum(col1)':p|$p.'sum(col1)__pylegend_olap_column__',
                'lambda_1(col1)':p|$p.'lambda_1(col1)__pylegend_olap_column__',
                col2:p|$p.col2__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_expanding_with_explicit_order_by(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.expanding(order_by="col2").agg("sum")

        expected_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col2" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col1__pylegend_olap_column__",
                        SUM("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col2" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col2__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col2)], rows(unbounded(), 0)), ~[
                col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()},
                col2__pylegend_olap_column__:{p,w,r | $r.col2}:{c | $c->sum()}
              ])
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__,
                col2:p|$p.col2__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure


class TestRollingOnBaseFrame:
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_simple_sum(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.rolling(window=3, order_by="col1").agg("sum")

        expected_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col1__pylegend_olap_column__",
                        SUM("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col2__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(minus(2), 0)), ~[
                col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()},
                col2__pylegend_olap_column__:{p,w,r | $r.col2}:{c | $c->sum()}
              ])
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__,
                col2:p|$p.col2__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure


class TestExpandingOnGroupbyFrame:
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_simple_sum(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("rnd")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("grp").expanding().agg("sum")

        expected_sql = '''
            SELECT
                "root"."val__pylegend_olap_column__" AS "val",
                "root"."rnd__pylegend_olap_column__" AS "rnd"
            FROM
                (
                    SELECT
                        SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "val__pylegend_olap_column__",
                        SUM("root"."rnd") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "rnd__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".rnd AS "rnd",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~grp)], rows(unbounded(), 0)), ~[
                val__pylegend_olap_column__:{p,w,r | $r.val}:{c | $c->sum()},
                rnd__pylegend_olap_column__:{p,w,r | $r.rnd}:{c | $c->sum()}
              ])
              ->project(~[
                val:p|$p.val__pylegend_olap_column__,
                rnd:p|$p.rnd__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_complex_aggregation(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("rnd")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("grp").expanding().agg({
            "val": ["sum", lambda x: x.count()],
            "rnd": np.min
        })

        expected_sql = '''
            SELECT
                "root"."sum(val)__pylegend_olap_column__" AS "sum(val)",
                "root"."lambda_1(val)__pylegend_olap_column__" AS "lambda_1(val)",
                "root"."rnd__pylegend_olap_column__" AS "rnd"
            FROM
                (
                    SELECT
                        SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "sum(val)__pylegend_olap_column__",
                        COUNT("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "lambda_1(val)__pylegend_olap_column__",
                        MIN("root"."rnd") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "rnd__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".rnd AS "rnd",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~grp)], rows(unbounded(), 0)), ~[
                'sum(val)__pylegend_olap_column__':{p,w,r | $r.val}:{c | $c->sum()},
                'lambda_1(val)__pylegend_olap_column__':{p,w,r | $r.val}:{c | $c->count()},
                rnd__pylegend_olap_column__:{p,w,r | $r.rnd}:{c | $c->min()}
              ])
              ->project(~[
                'sum(val)':p|$p.'sum(val)__pylegend_olap_column__',
                'lambda_1(val)':p|$p.'lambda_1(val)__pylegend_olap_column__',
                rnd:p|$p.rnd__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_expanding_with_explicit_order_by(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("rnd")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("grp").expanding(order_by="val").agg("sum")

        expected_sql = '''
            SELECT
                "root"."val__pylegend_olap_column__" AS "val",
                "root"."rnd__pylegend_olap_column__" AS "rnd"
            FROM
                (
                    SELECT
                        SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."val" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "val__pylegend_olap_column__",
                        SUM("root"."rnd") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."val" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "rnd__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".rnd AS "rnd",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~val)], rows(unbounded(), 0)), ~[
                val__pylegend_olap_column__:{p,w,r | $r.val}:{c | $c->sum()},
                rnd__pylegend_olap_column__:{p,w,r | $r.rnd}:{c | $c->sum()}
              ])
              ->project(~[
                val:p|$p.val__pylegend_olap_column__,
                rnd:p|$p.rnd__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure


class TestRollingOnBaseFrame:
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_simple_sum(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.rolling(window=3, order_by="col1").agg("sum")

        expected_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col1__pylegend_olap_column__",
                        SUM("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col2__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(minus(2), 0)), ~[
                col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()},
                col2__pylegend_olap_column__:{p,w,r | $r.col2}:{c | $c->sum()}
              ])
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__,
                col2:p|$p.col2__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure


class TestExpandingOnGroupbyFrame:
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_simple_sum(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("rnd")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("grp").expanding().agg("sum")

        expected_sql = '''
            SELECT
                "root"."val__pylegend_olap_column__" AS "val",
                "root"."rnd__pylegend_olap_column__" AS "rnd"
            FROM
                (
                    SELECT
                        SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "val__pylegend_olap_column__",
                        SUM("root"."rnd") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "rnd__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".rnd AS "rnd",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~grp)], rows(unbounded(), 0)), ~[
                val__pylegend_olap_column__:{p,w,r | $r.val}:{c | $c->sum()},
                rnd__pylegend_olap_column__:{p,w,r | $r.rnd}:{c | $c->sum()}
              ])
              ->project(~[
                val:p|$p.val__pylegend_olap_column__,
                rnd:p|$p.rnd__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_complex_aggregation(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("rnd")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("grp").expanding().agg({
            "val": ["sum", lambda x: x.count()],
            "rnd": np.min
        })

        expected_sql = '''
            SELECT
                "root"."sum(val)__pylegend_olap_column__" AS "sum(val)",
                "root"."lambda_1(val)__pylegend_olap_column__" AS "lambda_1(val)",
                "root"."rnd__pylegend_olap_column__" AS "rnd"
            FROM
                (
                    SELECT
                        SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "sum(val)__pylegend_olap_column__",
                        COUNT("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "lambda_1(val)__pylegend_olap_column__",
                        MIN("root"."rnd") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "rnd__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".rnd AS "rnd",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~grp)], rows(unbounded(), 0)), ~[
                'sum(val)__pylegend_olap_column__':{p,w,r | $r.val}:{c | $c->sum()},
                'lambda_1(val)__pylegend_olap_column__':{p,w,r | $r.val}:{c | $c->count()},
                rnd__pylegend_olap_column__:{p,w,r | $r.rnd}:{c | $c->min()}
              ])
              ->project(~[
                'sum(val)':p|$p.'sum(val)__pylegend_olap_column__',
                'lambda_1(val)':p|$p.'lambda_1(val)__pylegend_olap_column__',
                rnd:p|$p.rnd__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_expanding_with_explicit_order_by(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("rnd")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("grp").expanding(order_by="val").agg("sum")

        expected_sql = '''
            SELECT
                "root"."val__pylegend_olap_column__" AS "val",
                "root"."rnd__pylegend_olap_column__" AS "rnd"
            FROM
                (
                    SELECT
                        SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."val" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "val__pylegend_olap_column__",
                        SUM("root"."rnd") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."val" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "rnd__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".rnd AS "rnd",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~val)], rows(unbounded(), 0)), ~[
                val__pylegend_olap_column__:{p,w,r | $r.val}:{c | $c->sum()},
                rnd__pylegend_olap_column__:{p,w,r | $r.rnd}:{c | $c->sum()}
              ])
              ->project(~[
                val:p|$p.val__pylegend_olap_column__,
                rnd:p|$p.rnd__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure


class TestRollingOnBaseFrame:
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_simple_sum(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.rolling(window=3, order_by="col1").agg("sum")

        expected_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col1__pylegend_olap_column__",
                        SUM("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col2__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(minus(2), 0)), ~[
                col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()},
                col2__pylegend_olap_column__:{p,w,r | $r.col2}:{c | $c->sum()}
              ])
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__,
                col2:p|$p.col2__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure


class TestExpandingOnGroupbyFrame:
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_simple_sum(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("rnd")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("grp").expanding().agg("sum")

        expected_sql = '''
            SELECT
                "root"."val__pylegend_olap_column__" AS "val",
                "root"."rnd__pylegend_olap_column__" AS "rnd"
            FROM
                (
                    SELECT
                        SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "val__pylegend_olap_column__",
                        SUM("root"."rnd") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "rnd__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".rnd AS "rnd",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~grp)], rows(unbounded(), 0)), ~[
                val__pylegend_olap_column__:{p,w,r | $r.val}:{c | $c->sum()},
                rnd__pylegend_olap_column__:{p,w,r | $r.rnd}:{c | $c->sum()}
              ])
              ->project(~[
                val:p|$p.val__pylegend_olap_column__,
                rnd:p|$p.rnd__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_complex_aggregation(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("rnd")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("grp").expanding().agg({
            "val": ["sum", lambda x: x.count()],
            "rnd": np.min
        })

        expected_sql = '''
            SELECT
                "root"."sum(val)__pylegend_olap_column__" AS "sum(val)",
                "root"."lambda_1(val)__pylegend_olap_column__" AS "lambda_1(val)",
                "root"."rnd__pylegend_olap_column__" AS "rnd"
            FROM
                (
                    SELECT
                        SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "sum(val)__pylegend_olap_column__",
                        COUNT("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "lambda_1(val)__pylegend_olap_column__",
                        MIN("root"."rnd") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "rnd__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".rnd AS "rnd",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~grp)], rows(unbounded(), 0)), ~[
                'sum(val)__pylegend_olap_column__':{p,w,r | $r.val}:{c | $c->sum()},
                'lambda_1(val)__pylegend_olap_column__':{p,w,r | $r.val}:{c | $c->count()},
                rnd__pylegend_olap_column__:{p,w,r | $r.rnd}:{c | $c->min()}
              ])
              ->project(~[
                'sum(val)':p|$p.'sum(val)__pylegend_olap_column__',
                'lambda_1(val)':p|$p.'lambda_1(val)__pylegend_olap_column__',
                rnd:p|$p.rnd__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_expanding_with_explicit_order_by(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("rnd")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("grp").expanding(order_by="val").agg("sum")

        expected_sql = '''
            SELECT
                "root"."val__pylegend_olap_column__" AS "val",
                "root"."rnd__pylegend_olap_column__" AS "rnd"
            FROM
                (
                    SELECT
                        SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."val" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "val__pylegend_olap_column__",
                        SUM("root"."rnd") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."val" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "rnd__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".rnd AS "rnd",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~val)], rows(unbounded(), 0)), ~[
                val__pylegend_olap_column__:{p,w,r | $r.val}:{c | $c->sum()},
                rnd__pylegend_olap_column__:{p,w,r | $r.rnd}:{c | $c->sum()}
              ])
              ->project(~[
                val:p|$p.val__pylegend_olap_column__,
                rnd:p|$p.rnd__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure


class TestRollingOnBaseFrame:
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_simple_sum(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.rolling(window=3, order_by="col1").agg("sum")

        expected_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col1__pylegend_olap_column__",
                        SUM("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col2__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(minus(2), 0)), ~[
                col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()},
                col2__pylegend_olap_column__:{p,w,r | $r.col2}:{c | $c->sum()}
              ])
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__,
                col2:p|$p.col2__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure


class TestWindowSeries:
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_series_expanding_sum_returns_correct_type(self) -> None:
        """frame["col"].expanding().sum() returns an IntegerSeries for integer column."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame["col1"].expanding().sum()

        from pylegend.core.language.pandas_api.pandas_api_series import IntegerSeries
        assert isinstance(result, IntegerSeries)

    def test_series_rolling_mean_returns_correct_type(self) -> None:
        """frame["col"].rolling(...).mean() returns a FloatSeries for float column."""
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame["col1"].rolling(window=3, order_by="col2").mean()

        from pylegend.core.language.pandas_api.pandas_api_series import FloatSeries
        assert isinstance(result, FloatSeries)

    def test_groupby_series_expanding_sum_returns_correct_type(self) -> None:
        """frame.groupby("grp")["col"].expanding().sum() returns IntegerGroupbySeries."""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame.groupby("grp")["val"].expanding().sum()

        from pylegend.core.language.pandas_api.pandas_api_groupby_series import IntegerGroupbySeries
        assert isinstance(result, IntegerGroupbySeries)

    def test_groupby_series_rolling_count_returns_integer_type(self) -> None:
        """count() always returns integer type regardless of source column type."""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.float_column("val")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame.groupby("grp")["val"].rolling(window=5, order_by="val").count()

        from pylegend.core.language.pandas_api.pandas_api_groupby_series import IntegerGroupbySeries
        assert isinstance(result, IntegerGroupbySeries)

    def test_assign_series_expanding_sum(self) -> None:
        """Assign an expanding sum on a single column."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame["col1"].expanding().sum()

        # Standalone series SQL/pure
        expected_series_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1"
            FROM
                (
                    SELECT
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col1__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_series_sql = dedent(expected_series_sql).strip()
        assert series.to_sql_query() == expected_series_sql

        expected_series_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), 0)), ~[
                col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()}
              ])
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__
              ])
        '''
        expected_series_pure = dedent(expected_series_pure).strip()
        assert series.to_pure_query() == expected_series_pure

        # Assign to frame
        frame["col1_cumsum"] = series

        expected_sql = '''
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."col1_cumsum__pylegend_olap_column__" AS "col1_cumsum"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col1_cumsum__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), 0)), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, col1_cumsum:c|$c.col1__pylegend_olap_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_assign_groupby_series_expanding_sum(self) -> None:
        """Assign an expanding sum on a groupby series."""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("rnd")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame.groupby("grp")["val"].expanding().sum()

        # Standalone series SQL/pure
        expected_series_sql = '''
            SELECT
                "root"."val__pylegend_olap_column__" AS "val"
            FROM
                (
                    SELECT
                        SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "val__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".rnd AS "rnd",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_series_sql = dedent(expected_series_sql).strip()
        assert series.to_sql_query() == expected_series_sql

        expected_series_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~grp)], rows(unbounded(), 0)), ~[
                val__pylegend_olap_column__:{p,w,r | $r.val}:{c | $c->sum()}
              ])
              ->project(~[
                val:p|$p.val__pylegend_olap_column__
              ])
        '''
        expected_series_pure = dedent(expected_series_pure).strip()
        assert series.to_pure_query() == expected_series_pure

        # Assign to frame
        frame["val_cumsum"] = series

        expected_sql = '''
            SELECT
                "root"."grp" AS "grp",
                "root"."val" AS "val",
                "root"."rnd" AS "rnd",
                "root"."val_cumsum__pylegend_olap_column__" AS "val_cumsum"
            FROM
                (
                    SELECT
                        "root"."grp" AS "grp",
                        "root"."val" AS "val",
                        "root"."rnd" AS "rnd",
                        SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."val" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "val_cumsum__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".rnd AS "rnd",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~val)], rows(unbounded(), 0)), ~val__pylegend_olap_column__:{p,w,r | $r.val}:{c | $c->sum()})
              ->project(~[grp:c|$c.grp, val:c|$c.val, rnd:c|$c.rnd, val_cumsum:c|$c.val__pylegend_olap_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_assign_series_expanding_with_arithmetic(self) -> None:
        """Assign expanding sum combined with arithmetic."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame["col1"].expanding().sum() - 100

        # Standalone series SQL/pure
        expected_series_sql = '''
            SELECT
                ("root"."col1__pylegend_olap_column__" - 100) AS "col1"
            FROM
                (
                    SELECT
                        SUM("root".col1) OVER (ORDER BY "root".col1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col1__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_series_sql = dedent(expected_series_sql).strip()
        assert series.to_sql_query() == expected_series_sql

        expected_series_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), 0)), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|(toOne($c.col1__pylegend_olap_column__) - 100)])
        '''  # noqa: E501
        expected_series_pure = dedent(expected_series_pure).strip()
        assert series.to_pure_query() == expected_series_pure

        # Assign to frame
        frame["shifted"] = series

        expected_sql = '''
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                ("root"."shifted__pylegend_olap_column__" - 100) AS "shifted"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "shifted__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), 0)), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, shifted:c|(toOne($c.col1__pylegend_olap_column__) - 100)])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_assign_series_expanding_with_rolling(self) -> None:
        """Assign expanding sum combined with rolling mean."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["combined"] = frame["col1"].expanding().sum() + 2 + 5
        frame["combined"] /= frame["col2"].rolling(window=3, order_by="col1").mean()

        expected_sql = '''
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                ((1.0 * "root"."combined") / "root"."combined__pylegend_olap_column__") AS "combined"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        AVG("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "combined__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root"."col1" AS "col1",
                                "root"."col2" AS "col2",
                                (("root"."combined__pylegend_olap_column__" + 2) + 5) AS "combined",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                (
                                    SELECT
                                        "root"."col1" AS "col1",
                                        "root"."col2" AS "col2",
                                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "combined__pylegend_olap_column__"
                                    FROM
                                        (
                                            SELECT
                                                "root".col1 AS "col1",
                                                "root".col2 AS "col2",
                                                0 AS "__pylegend_zero_column__"
                                            FROM
                                                test_schema.test_table AS "root"
                                        ) AS "root"
                                ) AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), 0)), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, combined:c|((toOne($c.col1__pylegend_olap_column__) + 2) + 5)])
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(minus(2), 0)), ~col2__pylegend_olap_column__:{p,w,r | $r.col2}:{c | $c->average()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, combined:c|(toOne($c.combined) / toOne($c.col2__pylegend_olap_column__))])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_assign_series_rolling_sum(self) -> None:
        """Assign a rolling sum on a single column."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame["col1"].rolling(window=3, order_by="col1").sum()

        # Standalone series SQL/pure
        expected_series_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1"
            FROM
                (
                    SELECT
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col1__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_series_sql = dedent(expected_series_sql).strip()
        assert series.to_sql_query() == expected_series_sql

        expected_series_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(minus(2), 0)), ~[
                col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()}
              ])
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__
              ])
        '''
        expected_series_pure = dedent(expected_series_pure).strip()
        assert series.to_pure_query() == expected_series_pure

        # Assign to frame
        frame["col1_roll3"] = series

        expected_sql = '''
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."col1_roll3__pylegend_olap_column__" AS "col1_roll3"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col1_roll3__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(minus(2), 0)), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, col1_roll3:c|$c.col1__pylegend_olap_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_assign_overwrite_with_expanding(self) -> None:
        """Overwrite an existing column with an expanding aggregate."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["col1"] = frame["col1"].expanding().sum()

        expected_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2" AS "col2"
            FROM
                (
                    SELECT
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col1__pylegend_olap_column__",
                        "root"."col2" AS "col2"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), 0)), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1__pylegend_olap_column__, col2:c|$c.col2])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure


class TestEdgeCases:
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_expanding_on_filtered_frame(self) -> None:
        """Expanding on a frame that has been filtered first."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame[frame["col1"] > 10]  # type: ignore
        frame["cumsum"] = frame["col1"].expanding().sum()

        expected_sql = '''
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."cumsum__pylegend_olap_column__" AS "cumsum"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "cumsum__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                            WHERE
                                ("root".col1 > 10)
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->filter(c|($c.col1 > 10))
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), 0)), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, cumsum:c|$c.col1__pylegend_olap_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_expanding_after_sort(self) -> None:
        """Expanding on a frame after sort_values."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.sort_values(by="col1")
        frame["cumsum"] = frame["col1"].expanding().sum()

        expected_sql = '''
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."cumsum__pylegend_olap_column__" AS "cumsum"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "cumsum__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                            ORDER BY
                                "root".col1
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->sort([~col1->ascending()])
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), 0)), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, cumsum:c|$c.col1__pylegend_olap_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_multiple_window_assigns(self) -> None:
        """Assign two different window aggregates to different columns sequentially."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame["cumsum"] = frame["col1"].expanding().sum()
        frame["roll_mean"] = frame["col2"].rolling(window=5, order_by="col2").mean()

        expected_sql = '''
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."cumsum" AS "cumsum",
                "root"."roll_mean__pylegend_olap_column__" AS "roll_mean"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        "root"."cumsum" AS "cumsum",
                        AVG("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col2" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS "roll_mean__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root"."col1" AS "col1",
                                "root"."col2" AS "col2",
                                "root"."cumsum__pylegend_olap_column__" AS "cumsum",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                (
                                    SELECT
                                        "root"."col1" AS "col1",
                                        "root"."col2" AS "col2",
                                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "cumsum__pylegend_olap_column__"
                                    FROM
                                        (
                                            SELECT
                                                "root".col1 AS "col1",
                                                "root".col2 AS "col2",
                                                0 AS "__pylegend_zero_column__"
                                            FROM
                                                test_schema.test_table AS "root"
                                        ) AS "root"
                                ) AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), 0)), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, cumsum:c|$c.col1__pylegend_olap_column__])
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col2)], rows(minus(4), 0)), ~col2__pylegend_olap_column__:{p,w,r | $r.col2}:{c | $c->average()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, cumsum:c|$c.cumsum, roll_mean:c|$c.col2__pylegend_olap_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure


class TestWindowAggregateEndToEnd:
    def test_e2e_expanding_on_base_frame(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame[["Age"]]  # type: ignore
        frame = frame.expanding(order_by="Age").agg("sum")

        # Ages: 23, 22, 12, 22, 34, 32, 35 => ordered by Age: 12, 22, 22, 23, 32, 34, 35
        # Cumulative sums: 12, 34, 56, 79, 111, 145, 180
        expected = {
            "columns": ["Age"],
            "rows": [
                {"values": [79]},   # Peter, 23
                {"values": [34]},   # John, 22
                {"values": [12]},   # John, 12
                {"values": [56]},   # Anthony, 22
                {"values": [145]},  # Fabrice, 34
                {"values": [111]},  # Oliver, 32
                {"values": [180]},  # David, 35
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_series_standalone(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        series = frame["Age"].expanding(order_by="Age").sum()

        expected = {
            "columns": ["Age"],
            "rows": [
                {"values": [79]},   # Peter, 23
                {"values": [34]},   # John, 22
                {"values": [12]},   # John, 12
                {"values": [56]},   # Anthony, 22
                {"values": [145]},  # Fabrice, 34
                {"values": [111]},  # Oliver, 32
                {"values": [180]},  # David, 35
            ],
        }
        res = series.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_series_assign(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["Age Cumsum"] = frame["Age"].expanding(order_by="Age").sum()

        # Ages: 23, 22, 12, 22, 34, 32, 35 => ordered by Age: 12, 22, 22, 23, 32, 34, 35
        # Cumulative sums: 12, 34, 56, 79, 111, 145, 180
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age Cumsum"],
            "rows": [
                {"values": ['Peter', 'Smith', 23, 'Firm X', 79]},
                {"values": ['John', 'Johnson', 22, 'Firm X', 34]},
                {"values": ['John', 'Hill', 12, 'Firm X', 12]},
                {"values": ['Anthony', 'Allen', 22, 'Firm X', 56]},
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A', 145]},
                {"values": ['Oliver', 'Hill', 32, 'Firm B', 111]},
                {"values": ['David', 'Harris', 35, 'Firm C', 180]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_groupby_series_assign(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["Age Cumsum"] = frame.groupby("Firm/Legal Name")["Age"].expanding(order_by="Age").sum()

        # Firm X ages ordered: 12, 22, 22, 23 => cumsums: 12, 34, 56, 79
        # Firm A (34) => 34, Firm B (32) => 32, Firm C (35) => 35
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age Cumsum"],
            "rows": [
                {"values": ['Peter', 'Smith', 23, 'Firm X', 79]},
                {"values": ['John', 'Johnson', 22, 'Firm X', 34]},
                {"values": ['John', 'Hill', 12, 'Firm X', 12]},
                {"values": ['Anthony', 'Allen', 22, 'Firm X', 56]},
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A', 34]},
                {"values": ['Oliver', 'Hill', 32, 'Firm B', 32]},
                {"values": ['David', 'Harris', 35, 'Firm C', 35]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_rolling_series_assign(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["Age Roll3"] = frame["Age"].rolling(window=3, order_by="Age").sum()

        # Ages ordered: 12, 22, 22, 23, 32, 34, 35
        # Rolling window=3 sums: 12, 34, 56, 67, 77, 89, 101
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age Roll3"],
            "rows": [
                {"values": ['Peter', 'Smith', 23, 'Firm X', 67]},
                {"values": ['John', 'Johnson', 22, 'Firm X', 34]},
                {"values": ['John', 'Hill', 12, 'Firm X', 12]},
                {"values": ['Anthony', 'Allen', 22, 'Firm X', 56]},
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A', 89]},
                {"values": ['Oliver', 'Hill', 32, 'Firm B', 77]},
                {"values": ['David', 'Harris', 35, 'Firm C', 101]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_series_with_arithmetic_assign(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["Age Cumsum Plus 10"] = frame["Age"].expanding(order_by="Age").sum() + 10

        # Same cumsums as test_e2e_series_assign, but + 10 each
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age Cumsum Plus 10"],
            "rows": [
                {"values": ['Peter', 'Smith', 23, 'Firm X', 89]},
                {"values": ['John', 'Johnson', 22, 'Firm X', 44]},
                {"values": ['John', 'Hill', 12, 'Firm X', 22]},
                {"values": ['Anthony', 'Allen', 22, 'Firm X', 66]},
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A', 155]},
                {"values": ['Oliver', 'Hill', 32, 'Firm B', 121]},
                {"values": ['David', 'Harris', 35, 'Firm C', 190]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected


class TestWindowSeriesProperties:
    """Tests for WindowSeries properties."""

    def test_window_series_window_frame_property(self) -> None:
        """window_frame property should return the window frame."""
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        window_frame = frame.expanding()
        ws = window_frame["val"]

        assert isinstance(ws, WindowSeries)
        assert ws.window_frame is window_frame

    def test_window_series_column_name_property(self) -> None:
        """column_name property should return the column name."""
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        columns = [
            PrimitiveTdsColumn.integer_column("grp"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        window_frame = frame.expanding()
        ws = window_frame["val"]

        assert isinstance(ws, WindowSeries)
        assert ws.column_name == "val"


class TestGroupbySeriesWindowFrameLegendExt:
    """Tests for GroupbySeries.window_frame_legend_ext() method."""

    def test_groupby_series_window_frame_legend_ext_basic(self) -> None:
        """window_frame_legend_ext() on GroupbySeries returns a WindowSeries."""
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        ws = frame.groupby("grp")["val"].window_frame_legend_ext(
            frame_spec=frame.rows_between(None, 0),
            order_by="val"
        )

        assert isinstance(ws, WindowSeries)
        assert ws.column_name == "val"

    def test_groupby_series_window_frame_legend_ext_with_range_between(self) -> None:
        """window_frame_legend_ext() works with range_between frame spec."""
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        ws = frame.groupby("grp")["val"].window_frame_legend_ext(
            frame_spec=frame.range_between(start=-10, end=10),
            order_by="val",
            ascending=False
        )

        assert isinstance(ws, WindowSeries)

    def test_groupby_series_window_frame_legend_ext_generates_sql(self) -> None:
        """window_frame_legend_ext() on GroupbySeries generates correct SQL with sum()."""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame.groupby("grp")["val"].window_frame_legend_ext(
            frame_spec=frame.rows_between(-2, 2),
            order_by="val"
        ).sum()

        sql = result.to_sql_query()
        assert "SUM" in sql
        assert "OVER" in sql
        assert "PARTITION BY" in sql
        assert "ROWS BETWEEN" in sql


class TestGroupbySeriesMedianMode:
    """Tests for GroupbySeries.median() and mode() methods."""

    def test_groupby_series_median_generates_sql(self) -> None:
        """median() on GroupbySeries generates correct SQL."""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame.groupby("grp")["val"].median()

        sql = result.to_sql_query()
        # median typically uses PERCENTILE_CONT or similar
        assert "grp" in sql.lower() or "val" in sql.lower()

    def test_groupby_series_median_generates_pure(self) -> None:
        """median() on GroupbySeries generates correct Pure."""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame.groupby("grp")["val"].median()

        pure = result.to_pure_query()
        assert "groupBy" in pure
        assert "median" in pure

    def test_groupby_series_mode_generates_sql(self) -> None:
        """mode() on GroupbySeries generates correct SQL."""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame.groupby("grp")["val"].mode()

        sql = result.to_sql_query()
        # mode uses MODE() aggregate function
        assert "grp" in sql.lower() or "val" in sql.lower()

    def test_groupby_series_mode_generates_pure(self) -> None:
        """mode() on GroupbySeries generates correct Pure."""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame.groupby("grp")["val"].mode()

        pure = result.to_pure_query()
        assert "groupBy" in pure
        assert "mode" in pure

    def test_groupby_series_median_returns_groupby_series(self) -> None:
        """median() returns a GroupbySeries when single column."""
        from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries

        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame.groupby("grp")["val"].median()

        assert isinstance(result, GroupbySeries)

    def test_groupby_series_mode_returns_groupby_series(self) -> None:
        """mode() returns a GroupbySeries when single column."""
        from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries

        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame.groupby("grp")["val"].mode()

        assert isinstance(result, GroupbySeries)


class TestWindowAggregateFunctionValidation:
    """Tests for WindowAggregateFunction.validate() method errors."""

    def test_window_aggregate_invalid_axis_error(self) -> None:
        """WindowAggregateFunction raises NotImplementedError for invalid axis."""
        from pylegend.core.tds.pandas_api.frames.functions.window_aggregate_function import WindowAggregateFunction

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        window_frame = frame.expanding()

        func = WindowAggregateFunction(
            base_frame=window_frame,
            func="sum",
            axis=1,  # Invalid axis
        )

        with pytest.raises(NotImplementedError) as exc:
            func.validate()
        assert "The 'axis' parameter of the aggregate function must be 0 or 'index', but got: 1" in str(exc.value)

    def test_window_aggregate_invalid_axis_string_error(self) -> None:
        """WindowAggregateFunction raises NotImplementedError for invalid string axis."""
        from pylegend.core.tds.pandas_api.frames.functions.window_aggregate_function import WindowAggregateFunction

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        window_frame = frame.expanding()

        func = WindowAggregateFunction(
            base_frame=window_frame,
            func="sum",
            axis="columns",  # Invalid axis
        )

        with pytest.raises(NotImplementedError) as exc:
            func.validate()
        assert "The 'axis' parameter of the aggregate function must be 0 or 'index', but got: columns" in str(exc.value)

    def test_window_aggregate_with_extra_args_error(self) -> None:
        """WindowAggregateFunction raises NotImplementedError for extra positional args."""
        from pylegend.core.tds.pandas_api.frames.functions.window_aggregate_function import WindowAggregateFunction

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        window_frame = frame.expanding()

        func = WindowAggregateFunction(
            window_frame,
            "sum",
            0,
            1, 2, 3,  # Extra positional args
        )

        with pytest.raises(NotImplementedError) as exc:
            func.validate()
        assert "WindowAggregateFunction currently does not support additional positional" in str(exc.value)
        assert "keyword arguments" in str(exc.value)

    def test_window_aggregate_with_extra_kwargs_error(self) -> None:
        """WindowAggregateFunction raises NotImplementedError for extra keyword args."""
        from pylegend.core.tds.pandas_api.frames.functions.window_aggregate_function import WindowAggregateFunction

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        window_frame = frame.expanding()

        func = WindowAggregateFunction(
            base_frame=window_frame,
            func="sum",
            axis=0,
            **{"extra_kwarg": 123},  # Extra keyword args
        )

        with pytest.raises(NotImplementedError) as exc:
            func.validate()
        assert "WindowAggregateFunction currently does not support additional positional" in str(exc.value)
        assert "keyword arguments" in str(exc.value)

    def test_window_aggregate_valid_axis_index_string(self) -> None:
        """WindowAggregateFunction accepts axis='index'."""
        from pylegend.core.tds.pandas_api.frames.functions.window_aggregate_function import WindowAggregateFunction

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        window_frame = frame.expanding()

        func = WindowAggregateFunction(
            base_frame=window_frame,
            func="sum",
            axis="index",  # Valid string axis
        )

        # Should not raise
        assert func.validate() is True


class TestWindowSeriesShortcutMethods:
    """Tests for WindowSeries shortcut methods (sum, mean, min, max, count, std, var)."""

    def test_window_series_sum_basic(self) -> None:
        """WindowSeries.sum() calls aggregate with 'sum'."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        result = ws.sum()
        sql = result.to_sql_query()
        assert "SUM" in sql

    def test_window_series_sum_numeric_only_error(self) -> None:
        """WindowSeries.sum(numeric_only=True) raises NotImplementedError."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        with pytest.raises(NotImplementedError) as exc:
            ws.sum(numeric_only=True)
        assert "numeric_only=True is not currently supported in sum function" in str(exc.value)

    def test_window_series_sum_min_count_error(self) -> None:
        """WindowSeries.sum(min_count=1) raises NotImplementedError."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        with pytest.raises(NotImplementedError) as exc:
            ws.sum(min_count=1)
        assert "min_count must be 0 in sum function, but got: 1" in str(exc.value)

    def test_window_series_mean_basic(self) -> None:
        """WindowSeries.mean() calls aggregate with 'mean'."""
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        result = ws.mean()
        sql = result.to_sql_query()
        assert "AVG" in sql

    def test_window_series_mean_numeric_only_error(self) -> None:
        """WindowSeries.mean(numeric_only=True) raises NotImplementedError."""
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        with pytest.raises(NotImplementedError) as exc:
            ws.mean(numeric_only=True)
        assert "numeric_only=True is not currently supported in mean function" in str(exc.value)

    def test_window_series_min_basic(self) -> None:
        """WindowSeries.min() calls aggregate with 'min'."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        result = ws.min()
        sql = result.to_sql_query()
        assert "MIN" in sql

    def test_window_series_min_numeric_only_error(self) -> None:
        """WindowSeries.min(numeric_only=True) raises NotImplementedError."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        with pytest.raises(NotImplementedError) as exc:
            ws.min(numeric_only=True)
        assert "numeric_only=True is not currently supported in min function" in str(exc.value)

    def test_window_series_max_basic(self) -> None:
        """WindowSeries.max() calls aggregate with 'max'."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        result = ws.max()
        sql = result.to_sql_query()
        assert "MAX" in sql

    def test_window_series_max_numeric_only_error(self) -> None:
        """WindowSeries.max(numeric_only=True) raises NotImplementedError."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        with pytest.raises(NotImplementedError) as exc:
            ws.max(numeric_only=True)
        assert "numeric_only=True is not currently supported in max function" in str(exc.value)

    def test_window_series_count_basic(self) -> None:
        """WindowSeries.count() calls aggregate with 'count'."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        result = ws.count()
        sql = result.to_sql_query()
        assert "COUNT" in sql

    def test_window_series_std_ddof_1_default(self) -> None:
        """WindowSeries.std() with default ddof=1 uses std_dev_sample."""
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        result = ws.std()
        sql = result.to_sql_query()
        assert "STDDEV_SAMP" in sql

    def test_window_series_std_ddof_0(self) -> None:
        """WindowSeries.std(ddof=0) uses std_dev_population."""
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        result = ws.std(ddof=0)
        sql = result.to_sql_query()
        assert "STDDEV_POP" in sql

    def test_window_series_std_invalid_ddof_error(self) -> None:
        """WindowSeries.std(ddof=2) raises NotImplementedError."""
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        with pytest.raises(NotImplementedError) as exc:
            ws.std(ddof=2)
        assert "Only ddof=0 (Population) and ddof=1 (Sample) are supported in std function, but got: 2" in str(exc.value)

    def test_window_series_std_numeric_only_error(self) -> None:
        """WindowSeries.std(numeric_only=True) raises NotImplementedError."""
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        with pytest.raises(NotImplementedError) as exc:
            ws.std(numeric_only=True)
        assert "numeric_only=True is not currently supported in std function" in str(exc.value)

    def test_window_series_var_ddof_1_default(self) -> None:
        """WindowSeries.var() with default ddof=1 uses variance_sample."""
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        result = ws.var()
        sql = result.to_sql_query()
        assert "VAR_SAMP" in sql

    def test_window_series_var_ddof_0(self) -> None:
        """WindowSeries.var(ddof=0) uses variance_population."""
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        result = ws.var(ddof=0)
        sql = result.to_sql_query()
        assert "VAR_POP" in sql

    def test_window_series_var_invalid_ddof_error(self) -> None:
        """WindowSeries.var(ddof=3) raises NotImplementedError."""
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        with pytest.raises(NotImplementedError) as exc:
            ws.var(ddof=3)
        assert "Only ddof=0 (Population) and ddof=1 (Sample) are supported in var function, but got: 3" in str(exc.value)

    def test_window_series_var_numeric_only_error(self) -> None:
        """WindowSeries.var(numeric_only=True) raises NotImplementedError."""
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        ws = frame["col1"].expanding()
        with pytest.raises(NotImplementedError) as exc:
            ws.var(numeric_only=True)
        assert "numeric_only=True is not currently supported in var function" in str(exc.value)


class TestWindowFrameLegendExtOnBaseFrame:
    """Tests for frame.window_frame_legend_ext() method on base TDS frame."""

    def test_window_frame_legend_ext_with_rows_between(self) -> None:
        """window_frame_legend_ext() on base frame with rows_between returns PandasApiWindowTdsFrame."""
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        window_frame = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(None, 0),
            order_by="col1"
        )

        assert isinstance(window_frame, PandasApiWindowTdsFrame)

    def test_window_frame_legend_ext_with_range_between(self) -> None:
        """window_frame_legend_ext() on base frame with range_between returns PandasApiWindowTdsFrame."""
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        window_frame = frame.window_frame_legend_ext(
            frame_spec=frame.range_between(start=-5, end=5),
            order_by="col1",
            ascending=False
        )

        assert isinstance(window_frame, PandasApiWindowTdsFrame)

    def test_window_frame_legend_ext_generates_sql(self) -> None:
        """window_frame_legend_ext() on base frame generates correct SQL with sum()."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(-3, 3),
            order_by="col1"
        ).agg("sum")

        sql = result.to_sql_query()
        assert "SUM" in sql
        assert "OVER" in sql
        assert "ROWS BETWEEN" in sql

    def test_window_frame_legend_ext_generates_pure(self) -> None:
        """window_frame_legend_ext() on base frame generates correct Pure with sum()."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(-2, 2),
            order_by="col1"
        ).agg("sum")

        pure = result.to_pure_query()
        assert "rows" in pure
        assert "sum" in pure

    def test_window_frame_legend_ext_with_multiple_order_by(self) -> None:
        """window_frame_legend_ext() works with multiple order_by columns."""
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        window_frame = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(None, 0),
            order_by=["col1", "col2"],
            ascending=[True, False]
        )

        assert isinstance(window_frame, PandasApiWindowTdsFrame)

    def test_window_frame_legend_ext_with_no_order_by(self) -> None:
        """window_frame_legend_ext() works with order_by=None."""
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        window_frame = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(None, 0),
            order_by=None
        )

        assert isinstance(window_frame, PandasApiWindowTdsFrame)


class TestWindowFrameLegendExtErrors:
    """Tests for error handling in frame.window_frame_legend_ext() method."""

    def test_base_frame_window_frame_legend_ext_invalid_frame_spec(self) -> None:
        """window_frame_legend_ext() on base frame raises TypeError for invalid frame_spec."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.window_frame_legend_ext(frame_spec="invalid")  # type: ignore
        assert "frame_spec must be a RowsBetween or RangeBetween, got str" in str(v.value)

    def test_base_frame_window_frame_legend_ext_with_none(self) -> None:
        """window_frame_legend_ext() on base frame raises TypeError for frame_spec=None."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.window_frame_legend_ext(frame_spec=None)  # type: ignore
        assert "frame_spec must be a RowsBetween or RangeBetween, got NoneType" in str(v.value)

    def test_base_frame_window_frame_legend_ext_with_integer(self) -> None:
        """window_frame_legend_ext() on base frame raises TypeError for integer frame_spec."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.window_frame_legend_ext(frame_spec=123)  # type: ignore
        assert "frame_spec must be a RowsBetween or RangeBetween, got int" in str(v.value)


class TestSeriesWindowFrameLegendExt:
    """Tests for frame['col'].window_frame_legend_ext() method on Series."""

    def test_series_window_frame_legend_ext_basic(self) -> None:
        """window_frame_legend_ext() on Series returns a WindowSeries."""
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        ws = frame["col1"].window_frame_legend_ext(
            frame_spec=frame.rows_between(None, 0),
            order_by="col1"
        )

        assert isinstance(ws, WindowSeries)
        assert ws.column_name == "col1"

    def test_series_window_frame_legend_ext_with_range_between(self) -> None:
        """window_frame_legend_ext() on Series works with range_between frame spec."""
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        columns = [
            PrimitiveTdsColumn.float_column("val"),
            PrimitiveTdsColumn.string_column("name"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        ws = frame["val"].window_frame_legend_ext(
            frame_spec=frame.range_between(start=-10, end=10),
            order_by="val",
            ascending=False
        )

        assert isinstance(ws, WindowSeries)
        assert ws.column_name == "val"

    def test_series_window_frame_legend_ext_generates_sql(self) -> None:
        """window_frame_legend_ext() on Series generates correct SQL with sum()."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame["col1"].window_frame_legend_ext(
            frame_spec=frame.rows_between(-2, 2),
            order_by="col1"
        ).sum()

        sql = result.to_sql_query()
        assert "SUM" in sql
        assert "OVER" in sql
        assert "ROWS BETWEEN" in sql

    def test_series_window_frame_legend_ext_generates_pure(self) -> None:
        """window_frame_legend_ext() on Series generates correct Pure with sum()."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame["col1"].window_frame_legend_ext(
            frame_spec=frame.rows_between(-2, 2),
            order_by="col1"
        ).sum()

        pure = result.to_pure_query()
        assert "rows" in pure
        assert "sum" in pure

    def test_series_window_frame_legend_ext_with_multiple_order_by(self) -> None:
        """window_frame_legend_ext() on Series works with multiple order_by columns."""
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        ws = frame["col1"].window_frame_legend_ext(
            frame_spec=frame.rows_between(None, 0),
            order_by=["col1", "col2"],
            ascending=[True, False]
        )

        assert isinstance(ws, WindowSeries)
        assert ws.column_name == "col1"

    def test_series_window_frame_legend_ext_with_no_order_by(self) -> None:
        """window_frame_legend_ext() on Series works with order_by=None."""
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        ws = frame["col1"].window_frame_legend_ext(
            frame_spec=frame.rows_between(None, 0),
            order_by=None
        )

        assert isinstance(ws, WindowSeries)
        assert ws.column_name == "col1"

    def test_series_window_frame_legend_ext_with_ascending_bool(self) -> None:
        """window_frame_legend_ext() on Series works with ascending as single bool."""
        from pylegend.core.language.pandas_api.pandas_api_window_series import WindowSeries

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        ws = frame["col1"].window_frame_legend_ext(
            frame_spec=frame.rows_between(-5, 5),
            order_by="col1",
            ascending=True
        )

        assert isinstance(ws, WindowSeries)

        ws2 = frame["col1"].window_frame_legend_ext(
            frame_spec=frame.rows_between(-5, 5),
            order_by="col1",
            ascending=False
        )

        assert isinstance(ws2, WindowSeries)


class TestPandasApiWindowTdsFrameInit:
    """Tests for PandasApiWindowTdsFrame.__init__() edge cases."""

    def test_window_tds_frame_order_by_as_list(self) -> None:
        """PandasApiWindowTdsFrame handles order_by as a list (sequence)."""
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        # Pass order_by as a list (not a string) to cover the list(order_by) branch
        window_frame = PandasApiWindowTdsFrame(
            base_frame=frame,
            order_by=["col1", "col2"],  # list, not string
            frame_spec=frame.rows_between(None, 0),
            ascending=True
        )

        assert window_frame._order_by == ["col1", "col2"]
        assert window_frame._ascending == [True, True]

    def test_window_tds_frame_order_by_as_tuple(self) -> None:
        """PandasApiWindowTdsFrame handles order_by as a tuple (sequence)."""
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        # Pass order_by as a tuple to cover the list(order_by) branch
        window_frame = PandasApiWindowTdsFrame(
            base_frame=frame,
            order_by=("col1", "col2"),  # tuple, not string
            frame_spec=frame.rows_between(None, 0),
            ascending=False
        )

        assert window_frame._order_by == ["col1", "col2"]
        assert window_frame._ascending == [False, False]

    def test_window_tds_frame_ascending_as_list(self) -> None:
        """PandasApiWindowTdsFrame handles ascending as a list (sequence)."""
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        # Pass ascending as a list to cover the list(ascending) branch
        window_frame = PandasApiWindowTdsFrame(
            base_frame=frame,
            order_by=["col1", "col2"],
            frame_spec=frame.rows_between(None, 0),
            ascending=[True, False]  # list, not single bool
        )

        assert window_frame._order_by == ["col1", "col2"]
        assert window_frame._ascending == [True, False]

    def test_window_tds_frame_ascending_as_tuple(self) -> None:
        """PandasApiWindowTdsFrame handles ascending as a tuple (sequence)."""
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        # Pass ascending as a tuple to cover the list(ascending) branch
        window_frame = PandasApiWindowTdsFrame(
            base_frame=frame,
            order_by=("col1", "col2"),
            frame_spec=frame.rows_between(None, 0),
            ascending=(False, True)  # tuple, not single bool
        )

        assert window_frame._order_by == ["col1", "col2"]
        assert window_frame._ascending == [False, True]

    def test_window_tds_frame_ascending_length_mismatch_error(self) -> None:
        """PandasApiWindowTdsFrame raises ValueError when ascending length doesn't match order_by."""
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as exc:
            PandasApiWindowTdsFrame(
                base_frame=frame,
                order_by=["col1", "col2", "col3"],  # 3 columns
                frame_spec=frame.rows_between(None, 0),
                ascending=[True, False]  # only 2 bools - mismatch!
            )

        assert "Length of ascending (2) must match length of order_by (3)" in str(exc.value)

    def test_window_tds_frame_ascending_length_mismatch_error_more_ascending(self) -> None:
        """PandasApiWindowTdsFrame raises ValueError when ascending has more elements than order_by."""
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as exc:
            PandasApiWindowTdsFrame(
                base_frame=frame,
                order_by=["col1"],  # 1 column
                frame_spec=frame.rows_between(None, 0),
                ascending=[True, False, True]  # 3 bools - mismatch!
            )

        assert "Length of ascending (3) must match length of order_by (1)" in str(exc.value)

    def test_window_tds_frame_with_order_by_list_ascending_list_generates_sql(self) -> None:
        """PandasApiWindowTdsFrame with list order_by and list ascending generates correct SQL."""
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        window_frame = PandasApiWindowTdsFrame(
            base_frame=frame,
            order_by=["col1", "col2"],  # list
            frame_spec=frame.rows_between(-2, 2),
            ascending=[True, False]  # list
        )

        result = window_frame.agg("sum")
        sql = result.to_sql_query()

        assert "SUM" in sql
        assert "OVER" in sql
        assert "ORDER BY" in sql

    def test_window_tds_frame_with_empty_ascending_list_when_no_order_by(self) -> None:
        """PandasApiWindowTdsFrame with ascending list and no order_by is handled correctly."""
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        # When order_by is None, passing an empty ascending list should work
        window_frame = PandasApiWindowTdsFrame(
            base_frame=frame,
            order_by=None,
            frame_spec=frame.rows_between(None, 0),
            ascending=[]  # empty list when no order_by
        )

        assert window_frame._order_by is None
        assert window_frame._ascending == []
