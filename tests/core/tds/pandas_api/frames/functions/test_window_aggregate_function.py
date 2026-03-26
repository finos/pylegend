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

import numpy as np
import pytest

from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile


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
                SUM("root"."col1") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col1",
                SUM("root"."col2") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        0 AS "__internal_pylegend_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[__internal_pylegend_column__], [ascending(~col1)], rows(unbounded(), 0)), ~[
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
                SUM("root"."col1") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "sum(col1)",
                COUNT("root"."col1") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "lambda_1(col1)",
                MIN("root"."col2") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        0 AS "__internal_pylegend_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[__internal_pylegend_column__], [ascending(~col1)], rows(unbounded(), 0)), ~[
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
                SUM("root"."col1") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col2" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col1",
                SUM("root"."col2") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col2" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        0 AS "__internal_pylegend_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[__internal_pylegend_column__], [ascending(~col2)], rows(unbounded(), 0)), ~[
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
                SUM("root"."col1") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col1",
                SUM("root"."col2") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        0 AS "__internal_pylegend_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[__internal_pylegend_column__], [ascending(~col1)], rows(2, 0)), ~[
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
                SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__internal_pylegend_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "val",
                SUM("root"."rnd") OVER (PARTITION BY "root"."grp", "root"."__internal_pylegend_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "rnd"
            FROM
                (
                    SELECT
                        "root".grp AS "grp",
                        "root".val AS "val",
                        "root".rnd AS "rnd",
                        0 AS "__internal_pylegend_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[grp, __internal_pylegend_column__], [ascending(~grp)], rows(unbounded(), 0)), ~[
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
                SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__internal_pylegend_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "sum(val)",
                COUNT("root"."val") OVER (PARTITION BY "root"."grp", "root"."__internal_pylegend_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "lambda_1(val)",
                MIN("root"."rnd") OVER (PARTITION BY "root"."grp", "root"."__internal_pylegend_column__" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "rnd"
            FROM
                (
                    SELECT
                        "root".grp AS "grp",
                        "root".val AS "val",
                        "root".rnd AS "rnd",
                        0 AS "__internal_pylegend_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[grp, __internal_pylegend_column__], [ascending(~grp)], rows(unbounded(), 0)), ~[
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


class TestRollingOnGroupbyFrame:
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

        frame = frame.groupby("grp").rolling(window=3, order_by="val").agg("sum")

        expected_sql = '''
            SELECT
                SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__internal_pylegend_column__" ORDER BY "root"."val" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "val",
                SUM("root"."rnd") OVER (PARTITION BY "root"."grp", "root"."__internal_pylegend_column__" ORDER BY "root"."val" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "rnd"
            FROM
                (
                    SELECT
                        "root".grp AS "grp",
                        "root".val AS "val",
                        "root".rnd AS "rnd",
                        0 AS "__internal_pylegend_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[grp, __internal_pylegend_column__], [ascending(~val)], rows(2, 0)), ~[
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

        frame["col1_cumsum"] = frame["col1"].expanding().sum()

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
                        SUM("root"."col1") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col1_cumsum__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__internal_pylegend_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[__internal_pylegend_column__], [ascending(~col1)], rows(unbounded(), 0)), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
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

        frame["val_cumsum"] = frame.groupby("grp")["val"].expanding().sum()

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
                        SUM("root"."val") OVER (PARTITION BY "root"."grp", "root"."__internal_pylegend_column__" ORDER BY "root"."val" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "val_cumsum__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".rnd AS "rnd",
                                0 AS "__internal_pylegend_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[grp, __internal_pylegend_column__], [ascending(~val)], rows(unbounded(), 0)), ~val__pylegend_olap_column__:{p,w,r | $r.val}:{c | $c->sum()})
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

        frame["shifted"] = frame["col1"].expanding().sum() - 100

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
                        SUM("root"."col1") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "shifted__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__internal_pylegend_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[__internal_pylegend_column__], [ascending(~col1)], rows(unbounded(), 0)), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
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
                        AVG("root"."col2") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "combined__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root"."col1" AS "col1",
                                "root"."col2" AS "col2",
                                (("root"."combined__pylegend_olap_column__" + 2) + 5) AS "combined",
                                0 AS "__internal_pylegend_column__"
                            FROM
                                (
                                    SELECT
                                        "root"."col1" AS "col1",
                                        "root"."col2" AS "col2",
                                        SUM("root"."col1") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "combined__pylegend_olap_column__"
                                    FROM
                                        (
                                            SELECT
                                                "root".col1 AS "col1",
                                                "root".col2 AS "col2",
                                                0 AS "__internal_pylegend_column__"
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
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[__internal_pylegend_column__], [ascending(~col1)], rows(unbounded(), 0)), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, combined:c|((toOne($c.col1__pylegend_olap_column__) + 2) + 5)])
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[__internal_pylegend_column__], [ascending(~col1)], rows(2, 0)), ~col2__pylegend_olap_column__:{p,w,r | $r.col2}:{c | $c->average()})
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

        frame["col1_roll3"] = frame["col1"].rolling(window=3, order_by="col1").sum()

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
                        SUM("root"."col1") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col1_roll3__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__internal_pylegend_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[__internal_pylegend_column__], [ascending(~col1)], rows(2, 0)), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
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
                        SUM("root"."col1") OVER (PARTITION BY "root"."__internal_pylegend_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col1__pylegend_olap_column__",
                        "root"."col2" AS "col2"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__internal_pylegend_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[__internal_pylegend_column__], [ascending(~col1)], rows(unbounded(), 0)), ~col1__pylegend_olap_column__:{p,w,r | $r.col1}:{c | $c->sum()})
              ->project(~[col1:c|$c.col1__pylegend_olap_column__, col2:c|$c.col2])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure
