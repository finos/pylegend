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
from pylegend.extensions.tds.abstract.csv_tds_frame import CsvInputFrameAbstract
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_frame_spec import rows_between, range_between
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


class TestPylegendExtensionWindowFrame:
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_unbounded_both_sides(self) -> None:
        """ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING — not achievable via expanding or rolling."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.window_frame_legend_ext(rows_between(), order_by="col1").agg("sum")

        expected_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col1__pylegend_olap_column__",
                        SUM("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col2__pylegend_olap_column__"
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
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), unbounded())), ~[
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

    def test_custom_preceding_and_following(self) -> None:
        """ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING — not achievable via rolling (which always has following=0)."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.window_frame_legend_ext(
            rows_between(-2, 3), order_by="col1"
        ).agg("sum")

        expected_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING) AS "col1__pylegend_olap_column__",
                        SUM("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING) AS "col2__pylegend_olap_column__"
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
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(minus(2), 3)), ~[
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

    def test_on_series(self) -> None:
        """Single column via frame['col'].window_frame_legend_ext(...)."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame["col1"].window_frame_legend_ext(
            rows_between(-1, 1), order_by="col1"
        ).sum()

        expected_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1"
            FROM
                (
                    SELECT
                        SUM("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS "col1__pylegend_olap_column__"
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
        assert series.to_sql_query() == expected_sql

    def test_on_groupby_frame(self) -> None:
        """Groupby + custom window — PARTITION BY should include grouping columns."""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("grp").window_frame_legend_ext(
            rows_between(None, 0), order_by="val"
        ).agg("sum")

        expected_sql = '''
            SELECT
                "root"."val__pylegend_olap_column__" AS "val"
            FROM
                (
                    SELECT
                        SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."val" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "val__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
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
                val__pylegend_olap_column__:{p,w,r | $r.val}:{c | $c->sum()}
              ])
              ->project(~[
                val:p|$p.val__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_assign_with_custom_window(self) -> None:
        """Assign a custom-window aggregate to a frame column."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["col1_window_sum"] = frame["col1"].window_frame_legend_ext(
            rows_between(), order_by="col1"
        ).sum()

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), unbounded())), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, col1_window_sum:c|$c.col1__pylegend_olap_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    @pytest.mark.parametrize(
        "start, end, expected_sql_rows_clause, expected_pure_rows_expr",
        [
            (None, None, "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", "rows(unbounded(), unbounded())"),
            (-1, None, "ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING", "rows(minus(1), unbounded())"),
            (-1, 0, "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW", "rows(minus(1), 0)"),
            (0, 1, "ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING", "rows(0, 1)"),
            (-1, 1, "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING", "rows(minus(1), 1)"),
            (-3, -1, "ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING", "rows(minus(3), minus(1))"),
            (1, 3, "ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING", "rows(1, 3)"),
            (0, 0, "ROWS BETWEEN CURRENT ROW AND CURRENT ROW", "rows(0, 0)"),
        ],
    )
    def test_rows_between_sign_convention_parametrized(
            self,
            start: PyLegendOptional[int],
            end: PyLegendOptional[int],
            expected_sql_rows_clause: str,
            expected_pure_rows_expr: str,
    ) -> None:
        """Verify that each rows_between(start, end) pair produces the correct SQL and Pure rows clause."""
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame.window_frame_legend_ext(
            rows_between(start, end), order_by="col1"
        ).agg("sum")

        sql = result.to_sql_query()
        assert expected_sql_rows_clause in sql

        pure = result.to_pure_query()
        assert expected_pure_rows_expr in pure

    @pytest.mark.parametrize(
        "start, end, expected_sql_range_clause, expected_pure_range_expr",
        [
            (None, None, "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", "_range(unbounded(), unbounded())"),
            (-1, 0, "RANGE BETWEEN 1 PRECEDING AND CURRENT ROW", "_range(minus(1), 0)"),
            (0, 1, "RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING", "_range(0, 1)"),
            (-100, 0, "RANGE BETWEEN 100 PRECEDING AND CURRENT ROW", "_range(minus(100), 0)"),
            (-1, 1, "RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING", "_range(minus(1), 1)"),
        ],
    )
    def test_range_between_parametrized(
            self,
            start: PyLegendOptional[int],
            end: PyLegendOptional[int],
            expected_sql_range_clause: str,
            expected_pure_range_expr: str,
    ) -> None:
        """Verify that each range_between(start, end) pair produces the correct SQL and Pure clause."""
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame.window_frame_legend_ext(
            range_between(start, end), order_by="col1"
        ).agg("sum")

        sql = result.to_sql_query()
        assert expected_sql_range_clause in sql

        pure = result.to_pure_query()
        assert expected_pure_range_expr in pure

    def test_error_start_greater_than_end(self) -> None:
        """start > end should raise ValueError."""
        with pytest.raises(ValueError) as v:
            rows_between(2, -1)
        assert "start (2) must be <= end (-1)" in str(v.value)

    def test_error_start_greater_than_end_range(self) -> None:
        """start > end should raise ValueError for range_between too."""
        with pytest.raises(ValueError) as v:
            range_between(0, -2)
        assert "start (0) must be <= end (-2)" in str(v.value)

    def test_error_invalid_frame_spec_type(self) -> None:
        """Passing a non-FrameSpec object should raise TypeError."""
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.window_frame_legend_ext("not_a_frame_spec", order_by="col1")  # type: ignore
        assert "frame_spec must be a RowsBetween or RangeBetween" in str(v.value)

    @pytest.mark.parametrize(
        "kwargs, expected_sql_clause, expected_pure_expr",
        [
            (
                dict(duration_start="unbounded", duration_end="unbounded"),
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
                "_range(unbounded(), unbounded())",
            ),
            (
                dict(duration_start=-1, duration_start_unit="DAYS", duration_end="unbounded"),
                "RANGE BETWEEN INTERVAL '1 DAY' PRECEDING AND UNBOUNDED FOLLOWING",
                "_range(minus(1), DurationUnit.DAYS, unbounded())",
            ),
            (
                dict(duration_start=-1, duration_start_unit="DAYS", duration_end=0, duration_end_unit="HOURS"),
                "RANGE BETWEEN INTERVAL '1 DAY' PRECEDING AND CURRENT ROW",
                "_range(minus(1), DurationUnit.DAYS, 0, DurationUnit.HOURS)",
            ),
            (
                dict(duration_start=0, duration_start_unit="DAYS", duration_end=1, duration_end_unit="HOURS"),
                "RANGE BETWEEN CURRENT ROW AND INTERVAL '1 HOUR' FOLLOWING",
                "_range(0, DurationUnit.DAYS, 1, DurationUnit.HOURS)",
            ),
            (
                dict(
                    duration_start=-1, duration_start_unit="DAYS",
                    duration_end=1, duration_end_unit="MONTHS",
                ),
                "RANGE BETWEEN INTERVAL '1 DAY' PRECEDING AND INTERVAL '1 MONTH' FOLLOWING",
                "_range(minus(1), DurationUnit.DAYS, 1, DurationUnit.MONTHS)",
            ),
            (
                dict(duration_start="unbounded", duration_end=-1, duration_end_unit="DAYS"),
                "RANGE BETWEEN UNBOUNDED PRECEDING AND INTERVAL '1 DAY' PRECEDING",
                "_range(unbounded(), minus(1), DurationUnit.DAYS)",
            ),
        ],
    )
    def test_range_between_duration_parametrized(
            self,
            kwargs: dict,
            expected_sql_clause: str,
            expected_pure_expr: str,
    ) -> None:
        """Verify duration-based range_between produces correct SQL and Pure."""
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        result = frame.window_frame_legend_ext(
            range_between(**kwargs), order_by="col1"
        ).agg("sum")

        sql = result.to_sql_query()
        assert expected_sql_clause in sql

        pure = result.to_pure_query()
        assert expected_pure_expr in pure

    def test_error_mix_simple_and_duration(self) -> None:
        """Cannot mix positional start/end with duration kwargs."""
        with pytest.raises(ValueError) as v:
            range_between(start=-1, duration_end=1, duration_end_unit="DAYS")
        assert "Cannot mix" in str(v.value)

    def test_error_invalid_duration_string(self) -> None:
        """Duration bound string must be 'unbounded'."""
        with pytest.raises(ValueError) as v:
            range_between(duration_start="invalid", duration_end="unbounded")
        assert "must be 'unbounded'" in str(v.value)

    def test_error_invalid_duration_unit_string(self) -> None:
        """Invalid duration unit string should raise ValueError."""
        with pytest.raises(ValueError) as v:
            range_between(duration_start=-1, duration_start_unit="LIGHTYEARS", duration_end="unbounded")
        assert "Invalid duration unit" in str(v.value)

