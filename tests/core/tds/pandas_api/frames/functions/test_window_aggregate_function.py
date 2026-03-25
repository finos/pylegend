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

from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame


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
    def test_simple_sum(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.expanding().agg("sum")

        expected_sql = '''
            SELECT
                SUM("root"."col1") OVER (ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col1",
                SUM("root"."col2") OVER (ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)], rows(unbounded(), 0)), ~[
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
                SUM("root"."col1") OVER (ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "sum(col1)",
                COUNT("root"."col1") OVER (ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "lambda_1(col1)",
                MIN("root"."col2") OVER (ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)], rows(unbounded(), 0)), ~[
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

    def test_expanding_with_explicit_order_by(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.expanding(order_by="col2").agg("sum")

        expected_sql = '''
            SELECT
                SUM("root"."col1") OVER (ORDER BY "root"."col2" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col1",
                SUM("root"."col2") OVER (ORDER BY "root"."col2" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col2)], rows(unbounded(), 0)), ~[
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


class TestRollingOnBaseFrame:
    def test_simple_sum(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.rolling(window=3, order_by="col1").agg("sum")

        expected_sql = '''
            SELECT
                SUM("root"."col1") OVER (ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col1",
                SUM("root"."col2") OVER (ORDER BY "root"."col1" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)], rows(2, 0)), ~[
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


class TestExpandingOnGroupbyFrame:
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
                SUM("root"."val") OVER (PARTITION BY "root"."grp" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "val",
                SUM("root"."rnd") OVER (PARTITION BY "root"."grp" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "rnd"
            FROM
                (
                    SELECT
                        "root".grp AS "grp",
                        "root".val AS "val",
                        "root".rnd AS "rnd"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[grp], [ascending(~grp)], rows(unbounded(), 0)), ~[
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
                SUM("root"."val") OVER (PARTITION BY "root"."grp" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "sum(val)",
                COUNT("root"."val") OVER (PARTITION BY "root"."grp" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "lambda_1(val)",
                MIN("root"."rnd") OVER (PARTITION BY "root"."grp" ORDER BY "root"."grp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "rnd"
            FROM
                (
                    SELECT
                        "root".grp AS "grp",
                        "root".val AS "val",
                        "root".rnd AS "rnd"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[grp], [ascending(~grp)], rows(unbounded(), 0)), ~[
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


class TestRollingOnGroupbyFrame:
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
                SUM("root"."val") OVER (PARTITION BY "root"."grp" ORDER BY "root"."val" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "val",
                SUM("root"."rnd") OVER (PARTITION BY "root"."grp" ORDER BY "root"."val" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "rnd"
            FROM
                (
                    SELECT
                        "root".grp AS "grp",
                        "root".val AS "val",
                        "root".rnd AS "rnd"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[grp], [ascending(~val)], rows(2, 0)), ~[
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

