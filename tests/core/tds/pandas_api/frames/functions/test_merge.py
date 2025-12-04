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

import json
from textwrap import dedent

import pytest

from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend.core.request.legend_client import LegendClient


class TestMergeFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_merge_on_type_errors(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        columns2 = [
            PrimitiveTdsColumn.integer_column("pol1"),
            PrimitiveTdsColumn.string_column("pol2"),
            PrimitiveTdsColumn.float_column("pol3")
        ]
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table_2'], columns2)

        # other_frame type error
        with pytest.raises(TypeError) as v:
            frame.merge(123, how="inner")  # type: ignore
        assert v.value.args[0] == "Can only merge TdsFrame objects, a <class 'int'> was passed"

        # how type error
        with pytest.raises(TypeError) as v:
            frame.merge(frame2, how=123)  # type: ignore
        assert v.value.args[0] == "'how' must be str, got <class 'int'>"

        # on type error
        with pytest.raises(TypeError) as v:
            frame.merge(frame2, how="inner", on=123)  # type: ignore
        assert v.value.args[0] == "Passing 'on' as a <class 'int'> is not supported. Provide 'on' as a tuple instead."

        # on type error
        with pytest.raises(TypeError) as v:
            frame.merge(frame2, how="inner", on=['col1', 2])  # type: ignore
        assert v.value.args[0] == "'on' must contain only str elements"

        # left_on type error
        with pytest.raises(TypeError) as v:
            frame.merge(frame2, how="inner", left_on={"a": 1}, right_on='col1')  # type: ignore
        assert v.value.args[0] == (
            "Passing 'left_on' as a <class 'dict'> is not supported. "
            "Provide 'left_on' as a tuple instead."
        )

        # right_on type error
        with pytest.raises(TypeError) as v:
            frame.merge(frame2, how="inner", left_on='col1', right_on={1, 2})  # type: ignore
        assert v.value.args[0] == (
            "Passing 'right_on' as a <class 'set'> is not supported. "
            "Provide 'right_on' as a tuple instead."
        )

        # suffixes type error
        with pytest.raises(TypeError) as v:
            frame.merge(frame2, how="inner", suffixes={"x", "y"})  # type: ignore
        assert v.value.args[0] == (
            "Passing 'suffixes' as <class 'set'>, is not supported. "
            "Provide 'suffixes' as a tuple instead."
        )

        with pytest.raises(TypeError) as v:
            frame.join(frame2, how="inner", lsuffix=2, rsuffix='y')  # type: ignore
        assert v.value.args[0] == "'suffixes' elements must be str or None"

        # suffixes value error
        with pytest.raises(ValueError) as v1:
            frame.merge(frame2, how="inner", suffixes=('_x', '_y', '_z'))  # type: ignore
        assert v1.value.args[0] == "too many values to unpack (expected 2)"

    def test_merge_on_unsupported_errors(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        columns2 = [
            PrimitiveTdsColumn.integer_column("pol1"),
            PrimitiveTdsColumn.string_column("pol2"),
            PrimitiveTdsColumn.float_column("pol3")
        ]
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table_2'], columns2)

        # same frame merge unsupported
        with pytest.raises(NotImplementedError) as v:
            frame.merge(frame, how="inner")
        assert v.value.args[0] == "Merging the same TdsFrame is not supported yet"

        # left_index unsupported
        with pytest.raises(NotImplementedError) as v:
            frame.merge(frame2, how="inner", left_index=True)
        assert v.value.args[0] == "Merging on index is not supported yet in PandasApi merge function"

        # right_index unsupported
        with pytest.raises(NotImplementedError) as v:
            frame.merge(frame2, how="inner", right_index=True)
        assert v.value.args[0] == "Merging on index is not supported yet in PandasApi merge function"

        # indicator unsupported
        with pytest.raises(NotImplementedError) as v:
            frame.merge(frame2, how="inner", indicator=True)
        assert v.value.args[0] == "Indicator parameter is not supported yet in PandasApi merge function"

        # validate unsupported
        with pytest.raises(NotImplementedError) as v:
            frame.merge(frame2, how="inner", validate="one_to_one")
        assert v.value.args[0] == "Validate parameter is not supported yet in PandasApi merge function"

    def test_merge_on_validation_errors(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        columns2 = [
            PrimitiveTdsColumn.integer_column("pol1"),
            PrimitiveTdsColumn.string_column("pol2"),
            PrimitiveTdsColumn.float_column("pol3")
        ]
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table_2'], columns2)
        frame3: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table_3'], columns)

        # on and left_on/right_on both provided
        with pytest.raises(ValueError) as v:
            frame.merge(frame2, how="inner", on='col1', left_on='col1', right_on='pol1')
        assert v.value.args[0] == 'Can only pass argument "on" OR "left_on" and "right_on", not a combination of both.'

        # on key error
        with pytest.raises(KeyError) as k:
            frame.merge(frame2, how="inner", on='nol')
        assert k.value.args[0] == "'nol' not found"

        # left_on key error
        with pytest.raises(KeyError) as k:
            frame.merge(frame2, how="inner", left_on='nol', right_on='pol1')
        assert k.value.args[0] == "'nol' not found"

        # right_on key error
        with pytest.raises(KeyError) as k:
            frame.merge(frame2, how="inner", left_on='col1', right_on='nol')
        assert k.value.args[0] == "'nol' not found"

        # left_on and right_on length mismatch
        with pytest.raises(ValueError) as v:
            frame.merge(frame2, how="inner", left_on=['col1', 'col2'], right_on='pol1')
        assert v.value.args[0] == "len(right_on) must equal len(left_on)"

        # suffix
        with pytest.raises(ValueError) as v:
            frame.join(frame3, on='col1')
        assert v.value.args[0] == "Resulting merged columns contain duplicates after suffix application"

        # no resolution specified
        with pytest.raises(ValueError) as v:
            frame.merge(frame2, how="inner")
        assert v.value.args[0] == "No merge keys resolved. Specify 'on' or 'left_on'/'right_on', or ensure common columns."

        # how = cross
        with pytest.raises(ValueError) as v:
            frame.merge(frame2, how="cross", on='col1')
        assert v.value.args[0] == "Can not pass on, right_on, left_on for how='cross'"

    def test_merge_on_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        columns2 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("pol2"),
            PrimitiveTdsColumn.float_column("pol3")
        ]
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table_2'], columns2)

        frame3: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table_3'], columns2)

        # Inner
        merged_frame = frame.merge(frame2, how="inner", on='col1')

        expected = '''\
        SELECT
            "root"."col1" AS "col1",
            "root"."col2" AS "col2",
            "root"."col3" AS "col3",
            "root"."col4" AS "col4",
            "root"."col5" AS "col5",
            "root"."pol2" AS "pol2",
            "root"."pol3" AS "pol3"
        FROM
            (
                SELECT
                    "left"."col1" AS "col1",
                    "left"."col2" AS "col2",
                    "left"."col3" AS "col3",
                    "left"."col4" AS "col4",
                    "left"."col5" AS "col5",
                    "right"."pol2" AS "pol2",
                    "right"."pol3" AS "pol3"
                FROM
                    (
                        SELECT
                            "root".col1 AS "col1",
                            "root".col2 AS "col2",
                            "root".col3 AS "col3",
                            "root".col4 AS "col4",
                            "root".col5 AS "col5"
                        FROM
                            test_schema.test_table AS "root"
                    ) AS "left"
                    INNER JOIN
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".pol2 AS "pol2",
                                "root".pol3 AS "pol3"
                            FROM
                                test_schema.test_table_2 AS "root"
                        ) AS "right"
                        ON ("left"."col1" = "right"."col1")
            ) AS "root"'''
        assert merged_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(), self.legend_client) == dedent(
            (
                "  #Table(test_schema.test_table)#\n"
                "    ->join(\n"
                "      #Table(test_schema.test_table_2)#\n"
                "        ->project(\n"
                "          ~[col1__right_key_tmp:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]\n"
                "        ),\n"
                "      JoinKind.INNER,\n"
                "      {l, r | $l.col1 == $r.col1__right_key_tmp}\n"
                "    )\n"
                "    ->project(\n"
                "      ~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, "
                "col4:x|$x.col4, col5:x|$x.col5, pol2:x|$x.pol2, pol3:x|$x.pol3]\n"
                "    )"
            )
        )

        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->join(#Table(test_schema.test_table_2)#"
            "->project(~[col1__right_key_tmp:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]), "
            "JoinKind.INNER, {l, r | $l.col1 == $r.col1__right_key_tmp})"
            "->project(~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, "
            "col5:x|$x.col5, pol2:x|$x.pol2, pol3:x|$x.pol3])"
        )

        # Left with suffix
        merged_frame = frame.merge(frame2, how="left", on='col1', suffixes=('_left', '_right'))

        expected = '''\
        SELECT
            "root"."col1" AS "col1",
            "root"."col2" AS "col2",
            "root"."col3" AS "col3",
            "root"."col4" AS "col4",
            "root"."col5" AS "col5",
            "root"."pol2" AS "pol2",
            "root"."pol3" AS "pol3"
        FROM
            (
                SELECT
                    "left"."col1" AS "col1",
                    "left"."col2" AS "col2",
                    "left"."col3" AS "col3",
                    "left"."col4" AS "col4",
                    "left"."col5" AS "col5",
                    "right"."pol2" AS "pol2",
                    "right"."pol3" AS "pol3"
                FROM
                    (
                        SELECT
                            "root".col1 AS "col1",
                            "root".col2 AS "col2",
                            "root".col3 AS "col3",
                            "root".col4 AS "col4",
                            "root".col5 AS "col5"
                        FROM
                            test_schema.test_table AS "root"
                    ) AS "left"
                    LEFT OUTER JOIN
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".pol2 AS "pol2",
                                "root".pol3 AS "pol3"
                            FROM
                                test_schema.test_table_2 AS "root"
                        ) AS "right"
                        ON ("left"."col1" = "right"."col1")
            ) AS "root"'''
        assert merged_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(), self.legend_client) == dedent(
            (
                "  #Table(test_schema.test_table)#\n"
                "    ->join(\n"
                "      #Table(test_schema.test_table_2)#\n"
                "        ->project(\n"
                "          ~[col1__right_key_tmp:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]\n"
                "        ),\n"
                "      JoinKind.LEFT,\n"
                "      {l, r | $l.col1 == $r.col1__right_key_tmp}\n"
                "    )\n"
                "    ->project(\n"
                "      ~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, "
                "col4:x|$x.col4, col5:x|$x.col5, pol2:x|$x.pol2, pol3:x|$x.pol3]\n"
                "    )"
            )
        )
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->join(#Table(test_schema.test_table_2)#"
            "->project(~[col1__right_key_tmp:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]), "
            "JoinKind.LEFT, {l, r | $l.col1 == $r.col1__right_key_tmp})"
            "->project(~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, "
            "col5:x|$x.col5, pol2:x|$x.pol2, pol3:x|$x.pol3])"
        )

        # Right
        merged_frame = frame2.merge(frame3, on=['col1', 'pol2'], how="right")
        expected = dedent(
            '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."pol2" AS "pol2",
                "root"."pol3_x" AS "pol3_x",
                "root"."pol3_y" AS "pol3_y"
            FROM
                (
                    SELECT
                        "left"."col1" AS "col1",
                        "left"."pol2" AS "pol2",
                        "left"."pol3" AS "pol3_x",
                        "right"."pol3" AS "pol3_y"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".pol2 AS "pol2",
                                "root".pol3 AS "pol3"
                            FROM
                                test_schema.test_table_2 AS "root"
                        ) AS "left"
                        RIGHT OUTER JOIN
                            (
                                SELECT
                                    "root".col1 AS "col1",
                                    "root".pol2 AS "pol2",
                                    "root".pol3 AS "pol3"
                                FROM
                                    test_schema.test_table_3 AS "root"
                            ) AS "right"
                            ON (("left"."col1" = "right"."col1") AND ("left"."pol2" = "right"."pol2"))
                ) AS "root"'''
        )
        assert merged_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
                #Table(test_schema.test_table_2)#
                  ->project(
                    ~[col1:x|$x.col1, pol2:x|$x.pol2, pol3_x:x|$x.pol3]
                  )
                  ->join(
                    #Table(test_schema.test_table_3)#
                      ->project(
                        ~[col1__right_key_tmp:x|$x.col1, pol2__right_key_tmp:x|$x.pol2, pol3_y:x|$x.pol3]
                      ),
                    JoinKind.RIGHT,
                    {l, r | ($l.col1 == $r.col1__right_key_tmp) && ($l.pol2 == $r.pol2__right_key_tmp)}
                  )
                  ->project(
                    ~[col1:x|$x.col1, pol2:x|$x.pol2, pol3_x:x|$x.pol3_x, pol3_y:x|$x.pol3_y]
                  )'''
        )
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table_2)#->project(~[col1:x|$x.col1, pol2:x|$x.pol2, '
            'pol3_x:x|$x.pol3])->join(#Table(test_schema.test_table_3)#->project(~[col1__right_key_tmp:x|$x.col1, '
            'pol2__right_key_tmp:x|$x.pol2, pol3_y:x|$x.pol3]), JoinKind.RIGHT, {l, r | '
            '($l.col1 == $r.col1__right_key_tmp) && ($l.pol2 == '
            '$r.pol2__right_key_tmp)})->project(~[col1:x|$x.col1, pol2:x|$x.pol2, '
            'pol3_x:x|$x.pol3_x, pol3_y:x|$x.pol3_y])'
        )

        # Full with suffix
        merged_frame = frame2.merge(frame3, on=['col1', 'pol2'], how="outer", suffixes=('_left', '_right'))
        expected = '''\
                SELECT
                    "root"."col1" AS "col1",
                    "root"."pol2" AS "pol2",
                    "root"."pol3_left" AS "pol3_left",
                    "root"."pol3_right" AS "pol3_right"
                FROM
                    (
                        SELECT
                            "left"."col1" AS "col1",
                            "left"."pol2" AS "pol2",
                            "left"."pol3" AS "pol3_left",
                            "right"."pol3" AS "pol3_right"
                        FROM
                            (
                                SELECT
                                    "root".col1 AS "col1",
                                    "root".pol2 AS "pol2",
                                    "root".pol3 AS "pol3"
                                FROM
                                    test_schema.test_table_2 AS "root"
                            ) AS "left"
                            FULL OUTER JOIN
                                (
                                    SELECT
                                        "root".col1 AS "col1",
                                        "root".pol2 AS "pol2",
                                        "root".pol3 AS "pol3"
                                    FROM
                                        test_schema.test_table_3 AS "root"
                                ) AS "right"
                                ON (("left"."col1" = "right"."col1") AND ("left"."pol2" = "right"."pol2"))
                    ) AS "root"'''
        assert merged_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        expected_pure_pretty = dedent(
            '''\
              #Table(test_schema.test_table_2)#
                ->project(
                  ~[col1:x|$x.col1, pol2:x|$x.pol2, pol3_left:x|$x.pol3]
                )
                ->join(
                  #Table(test_schema.test_table_3)#
                    ->project(
                      ~[col1__right_key_tmp:x|$x.col1, pol2__right_key_tmp:x|$x.pol2, pol3_right:x|$x.pol3]
                    ),
                  JoinKind.FULL,
                  {l, r | ($l.col1 == $r.col1__right_key_tmp) && ($l.pol2 == $r.pol2__right_key_tmp)}
                )
                ->project(
                  ~[col1:x|$x.col1, pol2:x|$x.pol2, pol3_left:x|$x.pol3_left, pol3_right:x|$x.pol3_right]
                )'''
        )
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(), self.legend_client) == expected_pure_pretty
        expected_pure_compact = (
            "#Table(test_schema.test_table_2)#"
            "->project(~[col1:x|$x.col1, pol2:x|$x.pol2, pol3_left:x|$x.pol3])"
            "->join(#Table(test_schema.test_table_3)#"
            "->project(~[col1__right_key_tmp:x|$x.col1, pol2__right_key_tmp:x|$x.pol2, pol3_right:x|$x.pol3]), "
            "JoinKind.FULL, {l, r | ($l.col1 == $r.col1__right_key_tmp) && ($l.pol2 == $r.pol2__right_key_tmp)})"
            "->project(~[col1:x|$x.col1, pol2:x|$x.pol2, pol3_left:x|$x.pol3_left, pol3_right:x|$x.pol3_right])"
        )
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501

        # CROSS
        merged_frame = frame.merge(frame2, how="cross")

        expected_pure_pretty = dedent(
            '''\
              #Table(test_schema.test_table)#
                ->project(
                  ~[col1_x:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5]
                )
                ->join(
                  #Table(test_schema.test_table_2)#
                    ->project(
                      ~[col1_y:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]
                    ),
                  JoinKind.INNER,
                  {l, r | 1==1}
                )'''
        )
        expected_pure_compact = (
            "#Table(test_schema.test_table)#"
            "->project(~[col1_x:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5])"
            "->join(#Table(test_schema.test_table_2)#"
            "->project(~[col1_y:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]), "
            "JoinKind.INNER, {l, r | 1==1})"
        )
        expected_sql = (
            'SELECT\n'
            '    "root"."col1_x" AS "col1_x",\n'
            '    "root"."col2" AS "col2",\n'
            '    "root"."col3" AS "col3",\n'
            '    "root"."col4" AS "col4",\n'
            '    "root"."col5" AS "col5",\n'
            '    "root"."col1_y" AS "col1_y",\n'
            '    "root"."pol2" AS "pol2",\n'
            '    "root"."pol3" AS "pol3"\n'
            'FROM\n'
            '    (\n'
            '        SELECT\n'
            '            "left"."col1" AS "col1_x",\n'
            '            "left"."col2" AS "col2",\n'
            '            "left"."col3" AS "col3",\n'
            '            "left"."col4" AS "col4",\n'
            '            "left"."col5" AS "col5",\n'
            '            "right"."col1" AS "col1_y",\n'
            '            "right"."pol2" AS "pol2",\n'
            '            "right"."pol3" AS "pol3"\n'
            '        FROM\n'
            '            (\n'
            '                SELECT\n'
            '                    "root".col1 AS "col1",\n'
            '                    "root".col2 AS "col2",\n'
            '                    "root".col3 AS "col3",\n'
            '                    "root".col4 AS "col4",\n'
            '                    "root".col5 AS "col5"\n'
            '                FROM\n'
            '                    test_schema.test_table AS "root"\n'
            '            ) AS "left"\n'
            '            CROSS JOIN\n'
            '                (\n'
            '                    SELECT\n'
            '                        "root".col1 AS "col1",\n'
            '                        "root".pol2 AS "pol2",\n'
            '                        "root".pol3 AS "pol3"\n'
            '                    FROM\n'
            '                        test_schema.test_table_2 AS "root"\n'
            '                ) AS "right"\n'
            '                \n'
            '    ) AS "root"'
        )
        assert merged_frame.to_sql_query(FrameToSqlConfig()) == expected_sql
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(), self.legend_client) == expected_pure_pretty
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501

    def test_merge_left_right_on_parameters(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        columns2 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("pol2"),
            PrimitiveTdsColumn.float_column("pol3")
        ]
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table_2'], columns2)

        # inner
        merged_frame = frame.merge(frame2, how="inner", left_on='col1', right_on='col1')
        expected = '''\
                SELECT
                    "root"."col1" AS "col1",
                    "root"."col2" AS "col2",
                    "root"."col3" AS "col3",
                    "root"."col4" AS "col4",
                    "root"."col5" AS "col5",
                    "root"."pol2" AS "pol2",
                    "root"."pol3" AS "pol3"
                FROM
                    (
                        SELECT
                            "left"."col1" AS "col1",
                            "left"."col2" AS "col2",
                            "left"."col3" AS "col3",
                            "left"."col4" AS "col4",
                            "left"."col5" AS "col5",
                            "right"."pol2" AS "pol2",
                            "right"."pol3" AS "pol3"
                        FROM
                            (
                                SELECT
                                    "root".col1 AS "col1",
                                    "root".col2 AS "col2",
                                    "root".col3 AS "col3",
                                    "root".col4 AS "col4",
                                    "root".col5 AS "col5"
                                FROM
                                    test_schema.test_table AS "root"
                            ) AS "left"
                            INNER JOIN
                                (
                                    SELECT
                                        "root".col1 AS "col1",
                                        "root".pol2 AS "pol2",
                                        "root".pol3 AS "pol3"
                                    FROM
                                        test_schema.test_table_2 AS "root"
                                ) AS "right"
                                ON ("left"."col1" = "right"."col1")
                    ) AS "root"'''
        assert merged_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(), self.legend_client) == dedent(
            (
                "  #Table(test_schema.test_table)#\n"
                "    ->join(\n"
                "      #Table(test_schema.test_table_2)#\n"
                "        ->project(\n"
                "          ~[col1__right_key_tmp:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]\n"
                "        ),\n"
                "      JoinKind.INNER,\n"
                "      {l, r | $l.col1 == $r.col1__right_key_tmp}\n"
                "    )\n"
                "    ->project(\n"
                "      ~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, "
                "col4:x|$x.col4, col5:x|$x.col5, pol2:x|$x.pol2, pol3:x|$x.pol3]\n"
                "    )"
            )
        )
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->join(#Table(test_schema.test_table_2)#"
            "->project(~[col1__right_key_tmp:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]), "
            "JoinKind.INNER, {l, r | $l.col1 == $r.col1__right_key_tmp})"
            "->project(~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, "
            "col5:x|$x.col5, pol2:x|$x.pol2, pol3:x|$x.pol3])"
        )

        # left
        merged_frame = frame.merge(frame2, how="left", left_on='col1', right_on='pol3', suffixes=('_left', '_right'))
        expected = '''\
                SELECT
                    "root"."col1_left" AS "col1_left",
                    "root"."col2" AS "col2",
                    "root"."col3" AS "col3",
                    "root"."col4" AS "col4",
                    "root"."col5" AS "col5",
                    "root"."col1_right" AS "col1_right",
                    "root"."pol2" AS "pol2",
                    "root"."pol3" AS "pol3"
                FROM
                    (
                        SELECT
                            "left"."col1" AS "col1_left",
                            "left"."col2" AS "col2",
                            "left"."col3" AS "col3",
                            "left"."col4" AS "col4",
                            "left"."col5" AS "col5",
                            "right"."col1" AS "col1_right",
                            "right"."pol2" AS "pol2",
                            "right"."pol3" AS "pol3"
                        FROM
                            (
                                SELECT
                                    "root".col1 AS "col1",
                                    "root".col2 AS "col2",
                                    "root".col3 AS "col3",
                                    "root".col4 AS "col4",
                                    "root".col5 AS "col5"
                                FROM
                                    test_schema.test_table AS "root"
                            ) AS "left"
                            LEFT OUTER JOIN
                                (
                                    SELECT
                                        "root".col1 AS "col1",
                                        "root".pol2 AS "pol2",
                                        "root".pol3 AS "pol3"
                                    FROM
                                        test_schema.test_table_2 AS "root"
                                ) AS "right"
                                ON ("left"."col1" = "right"."pol3")
                    ) AS "root"'''
        assert merged_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        expected_pure_pretty = dedent(
            '''\
              #Table(test_schema.test_table)#
                ->project(
                  ~[col1_left:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5]
                )
                ->join(
                  #Table(test_schema.test_table_2)#
                    ->project(
                      ~[col1_right:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]
                    ),
                  JoinKind.LEFT,
                  {l, r | $l.col1_left == $r.pol3}
                )'''
        )
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(), self.legend_client) == expected_pure_pretty
        expected_pure_compact = (
            "#Table(test_schema.test_table)#"
            "->project(~[col1_left:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5])"
            "->join(#Table(test_schema.test_table_2)#"
            "->project(~[col1_right:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]), "
            "JoinKind.LEFT, {l, r | $l.col1_left == $r.pol3})"
        )
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501

        # right
        merged_frame = frame.merge(frame2, how="right", left_on=['col1', 'col2'], right_on=['col1', 'pol2'])
        expected_sql = '''\
                SELECT
                    "root"."col1" AS "col1",
                    "root"."col2" AS "col2",
                    "root"."col3" AS "col3",
                    "root"."col4" AS "col4",
                    "root"."col5" AS "col5",
                    "root"."pol2" AS "pol2",
                    "root"."pol3" AS "pol3"
                FROM
                    (
                        SELECT
                            "left"."col1" AS "col1",
                            "left"."col2" AS "col2",
                            "left"."col3" AS "col3",
                            "left"."col4" AS "col4",
                            "left"."col5" AS "col5",
                            "right"."pol2" AS "pol2",
                            "right"."pol3" AS "pol3"
                        FROM
                            (
                                SELECT
                                    "root".col1 AS "col1",
                                    "root".col2 AS "col2",
                                    "root".col3 AS "col3",
                                    "root".col4 AS "col4",
                                    "root".col5 AS "col5"
                                FROM
                                    test_schema.test_table AS "root"
                            ) AS "left"
                            RIGHT OUTER JOIN
                                (
                                    SELECT
                                        "root".col1 AS "col1",
                                        "root".pol2 AS "pol2",
                                        "root".pol3 AS "pol3"
                                    FROM
                                        test_schema.test_table_2 AS "root"
                                ) AS "right"
                                ON (("left"."col1" = "right"."col1") AND ("left"."col2" = "right"."pol2"))
                    ) AS "root"'''

        assert merged_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure_pretty = dedent(
            '''\
              #Table(test_schema.test_table)#
                ->join(
                  #Table(test_schema.test_table_2)#
                    ->project(
                      ~[col1__right_key_tmp:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]
                    ),
                  JoinKind.RIGHT,
                  {l, r | ($l.col1 == $r.col1__right_key_tmp) && ($l.col2 == $r.pol2)}
                )
                ->project(
                  ~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5, '''
            '''pol2:x|$x.pol2, pol3:x|$x.pol3]
                )'''
        )
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(), self.legend_client) == expected_pure_pretty
        expected_pure_compact = (
            "#Table(test_schema.test_table)#"
            "->join(#Table(test_schema.test_table_2)#"
            "->project(~[col1__right_key_tmp:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]), "
            "JoinKind.RIGHT, {l, r | ($l.col1 == $r.col1__right_key_tmp) && ($l.col2 == $r.pol2)})"
            "->project(~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5, pol2:x|$x.pol2, pol3:x|$x.pol3])"  # noqa: E501
        )
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501

    def test_merge_sort_join_parameters(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        columns2 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("pol2"),
            PrimitiveTdsColumn.float_column("pol3")
        ]
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table_2'], columns2)

        # sort
        merged_frame = frame.merge(frame2, how="inner", left_on='col2', right_on='col1', sort=True)

        expected_pure_pretty = dedent(
            '''\
              #Table(test_schema.test_table)#
                ->project(
                  ~[col1_x:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5]
                )
                ->join(
                  #Table(test_schema.test_table_2)#
                    ->project(
                      ~[col1_y:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]
                    ),
                  JoinKind.INNER,
                  {l, r | $l.col2 == $r.col1_y}
                )
                ->sort([~col2->ascending(), ~col1_y->ascending()])'''
        )
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(), self.legend_client) == expected_pure_pretty

        expected_pure_compact = (
            "#Table(test_schema.test_table)#"
            "->project(~[col1_x:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5])"
            "->join(#Table(test_schema.test_table_2)#"
            "->project(~[col1_y:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]), "
            "JoinKind.INNER, {l, r | $l.col2 == $r.col1_y})"
            "->sort([~col2->ascending(), ~col1_y->ascending()])"
        )
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501

        expected_sql = dedent(
            '''\
            SELECT
                "root"."col1_x" AS "col1_x",
                "root"."col2" AS "col2",
                "root"."col3" AS "col3",
                "root"."col4" AS "col4",
                "root"."col5" AS "col5",
                "root"."col1_y" AS "col1_y",
                "root"."pol2" AS "pol2",
                "root"."pol3" AS "pol3"
            FROM
                (
                    SELECT
                        "left"."col1" AS "col1_x",
                        "left"."col2" AS "col2",
                        "left"."col3" AS "col3",
                        "left"."col4" AS "col4",
                        "left"."col5" AS "col5",
                        "right"."col1" AS "col1_y",
                        "right"."pol2" AS "pol2",
                        "right"."pol3" AS "pol3"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3",
                                "root".col4 AS "col4",
                                "root".col5 AS "col5"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "left"
                        INNER JOIN
                            (
                                SELECT
                                    "root".col1 AS "col1",
                                    "root".pol2 AS "pol2",
                                    "root".pol3 AS "pol3"
                                FROM
                                    test_schema.test_table_2 AS "root"
                            ) AS "right"
                            ON ("left"."col2" = "right"."col1")
                ) AS "root"
            ORDER BY
                "root"."col2",
                "root"."col1_y"'''
        )
        assert merged_frame.to_sql_query(FrameToSqlConfig()) == expected_sql

        # join
        merged_frame = frame.join(frame2, sort=True)

        expected_pure_pretty = dedent(
            '''\
              #Table(test_schema.test_table)#
                ->join(
                  #Table(test_schema.test_table_2)#
                    ->project(
                      ~[col1__right_key_tmp:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]
                    ),
                  JoinKind.LEFT,
                  {l, r | $l.col1 == $r.col1__right_key_tmp}
                )
                ->project(
                  ~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5, '''
            '''pol2:x|$x.pol2, pol3:x|$x.pol3]
                )
                ->sort([~col1->ascending()])'''
        )
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(), self.legend_client) == expected_pure_pretty

        expected_pure_compact = (
            "#Table(test_schema.test_table)#"
            "->join(#Table(test_schema.test_table_2)#"
            "->project(~[col1__right_key_tmp:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]), "
            "JoinKind.LEFT, {l, r | $l.col1 == $r.col1__right_key_tmp})"
            "->project(~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5, pol2:x|$x.pol2, pol3:x|$x.pol3])"  # noqa: E501
            "->sort([~col1->ascending()])"
        )
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501

        expected_sql = dedent(
            '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."col3" AS "col3",
                "root"."col4" AS "col4",
                "root"."col5" AS "col5",
                "root"."pol2" AS "pol2",
                "root"."pol3" AS "pol3"
            FROM
                (
                    SELECT
                        "left"."col1" AS "col1",
                        "left"."col2" AS "col2",
                        "left"."col3" AS "col3",
                        "left"."col4" AS "col4",
                        "left"."col5" AS "col5",
                        "right"."pol2" AS "pol2",
                        "right"."pol3" AS "pol3"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3",
                                "root".col4 AS "col4",
                                "root".col5 AS "col5"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "left"
                        LEFT OUTER JOIN
                            (
                                SELECT
                                    "root".col1 AS "col1",
                                    "root".pol2 AS "pol2",
                                    "root".pol3 AS "pol3"
                                FROM
                                    test_schema.test_table_2 AS "root"
                            ) AS "right"
                            ON ("left"."col1" = "right"."col1")
                ) AS "root"
            ORDER BY
                "root"."col1"'''
        )
        assert merged_frame.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_merge_chained(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        columns2 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("pol2"),
            PrimitiveTdsColumn.float_column("pol3")
        ]
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table_2'], columns2)

        # Merge
        merged_frame = frame.merge(frame2, how="inner", on='col1').merge(frame2, how="left", left_on='col1', right_on='pol2', suffixes=('_left', '_right'))  # noqa: E501
        expected_pure_pretty = dedent(
            '''\
              #Table(test_schema.test_table)#
                ->join(
                  #Table(test_schema.test_table_2)#
                    ->project(
                      ~[col1__right_key_tmp:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]
                    ),
                  JoinKind.INNER,
                  {l, r | $l.col1 == $r.col1__right_key_tmp}
                )
                ->project(
                  ~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5, '''
            '''pol2:x|$x.pol2, pol3:x|$x.pol3]
                )
                ->project(
                  ~[col1_left:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5, '''
            '''pol2_left:x|$x.pol2, pol3_left:x|$x.pol3]
                )
                ->join(
                  #Table(test_schema.test_table_2)#
                    ->project(
                      ~[col1_right:x|$x.col1, pol2_right:x|$x.pol2, pol3_right:x|$x.pol3]
                    ),
                  JoinKind.LEFT,
                  {l, r | $l.col1_left == $r.pol2_right}
                )'''
        )
        expected_pure_compact = (
            "#Table(test_schema.test_table)#"
            "->join(#Table(test_schema.test_table_2)#"
            "->project(~[col1__right_key_tmp:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]), "
            "JoinKind.INNER, {l, r | $l.col1 == $r.col1__right_key_tmp})"
            "->project(~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5, pol2:x|$x.pol2, pol3:x|$x.pol3])"  # noqa: E501
            "->project(~[col1_left:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5, pol2_left:x|$x.pol2, pol3_left:x|$x.pol3])"  # noqa: E501
            "->join(#Table(test_schema.test_table_2)#"
            "->project(~[col1_right:x|$x.col1, pol2_right:x|$x.pol2, pol3_right:x|$x.pol3]), "
            "JoinKind.LEFT, {l, r | $l.col1_left == $r.pol2_right})"
        )
        expected_sql = dedent(
            '''\
            SELECT
                "root"."col1_left" AS "col1_left",
                "root"."col2" AS "col2",
                "root"."col3" AS "col3",
                "root"."col4" AS "col4",
                "root"."col5" AS "col5",
                "root"."pol2_left" AS "pol2_left",
                "root"."pol3_left" AS "pol3_left",
                "root"."col1_right" AS "col1_right",
                "root"."pol2_right" AS "pol2_right",
                "root"."pol3_right" AS "pol3_right"
            FROM
                (
                    SELECT
                        "left"."col1" AS "col1_left",
                        "left"."col2" AS "col2",
                        "left"."col3" AS "col3",
                        "left"."col4" AS "col4",
                        "left"."col5" AS "col5",
                        "left"."pol2" AS "pol2_left",
                        "left"."pol3" AS "pol3_left",
                        "right"."col1" AS "col1_right",
                        "right"."pol2" AS "pol2_right",
                        "right"."pol3" AS "pol3_right"
                    FROM
                        (
                            SELECT
                                "root"."col1" AS "col1",
                                "root"."col2" AS "col2",
                                "root"."col3" AS "col3",
                                "root"."col4" AS "col4",
                                "root"."col5" AS "col5",
                                "root"."pol2" AS "pol2",
                                "root"."pol3" AS "pol3"
                            FROM
                                (
                                    SELECT
                                        "left"."col1" AS "col1",
                                        "left"."col2" AS "col2",
                                        "left"."col3" AS "col3",
                                        "left"."col4" AS "col4",
                                        "left"."col5" AS "col5",
                                        "right"."pol2" AS "pol2",
                                        "right"."pol3" AS "pol3"
                                    FROM
                                        (
                                            SELECT
                                                "root".col1 AS "col1",
                                                "root".col2 AS "col2",
                                                "root".col3 AS "col3",
                                                "root".col4 AS "col4",
                                                "root".col5 AS "col5"
                                            FROM
                                                test_schema.test_table AS "root"
                                        ) AS "left"
                                        INNER JOIN
                                            (
                                                SELECT
                                                    "root".col1 AS "col1",
                                                    "root".pol2 AS "pol2",
                                                    "root".pol3 AS "pol3"
                                                FROM
                                                    test_schema.test_table_2 AS "root"
                                            ) AS "right"
                                            ON ("left"."col1" = "right"."col1")
                                ) AS "root"
                        ) AS "left"
                        LEFT OUTER JOIN
                            (
                                SELECT
                                    "root".col1 AS "col1",
                                    "root".pol2 AS "pol2",
                                    "root".pol3 AS "pol3"
                                FROM
                                    test_schema.test_table_2 AS "root"
                            ) AS "right"
                            ON ("left"."col1" = "right"."pol2")
                ) AS "root"'''
        )
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(), self.legend_client) == expected_pure_pretty
        assert generate_pure_query_and_compile(merged_frame, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501
        assert merged_frame.to_sql_query(FrameToSqlConfig()) == expected_sql

        # Truncate
        merged_frame = frame.merge(frame2)
        newframe = merged_frame.truncate(before=1, after=3)

        expected_pure_pretty = dedent(
            '''\
              #Table(test_schema.test_table)#
                ->join(
                  #Table(test_schema.test_table_2)#
                    ->project(
                      ~[col1__right_key_tmp:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]
                    ),
                  JoinKind.INNER,
                  {l, r | $l.col1 == $r.col1__right_key_tmp}
                )
                ->project(
                  ~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5, '''
            '''pol2:x|$x.pol2, pol3:x|$x.pol3]
                )
                ->slice(1, 4)'''
        )
        expected_pure_compact = (
            "#Table(test_schema.test_table)#"
            "->join(#Table(test_schema.test_table_2)#"
            "->project(~[col1__right_key_tmp:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]), "
            "JoinKind.INNER, {l, r | $l.col1 == $r.col1__right_key_tmp})"
            "->project(~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3, col4:x|$x.col4, col5:x|$x.col5, pol2:x|$x.pol2, pol3:x|$x.pol3])"  # noqa: E501
            "->slice(1, 4)"
        )
        expected_sql = dedent(
            '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."col3" AS "col3",
                "root"."col4" AS "col4",
                "root"."col5" AS "col5",
                "root"."pol2" AS "pol2",
                "root"."pol3" AS "pol3"
            FROM
                (
                    SELECT
                        "left"."col1" AS "col1",
                        "left"."col2" AS "col2",
                        "left"."col3" AS "col3",
                        "left"."col4" AS "col4",
                        "left"."col5" AS "col5",
                        "right"."pol2" AS "pol2",
                        "right"."pol3" AS "pol3"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3",
                                "root".col4 AS "col4",
                                "root".col5 AS "col5"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "left"
                        INNER JOIN
                            (
                                SELECT
                                    "root".col1 AS "col1",
                                    "root".pol2 AS "pol2",
                                    "root".pol3 AS "pol3"
                                FROM
                                    test_schema.test_table_2 AS "root"
                            ) AS "right"
                            ON ("left"."col1" = "right"."col1")
                ) AS "root"
            LIMIT 3
            OFFSET 1'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == expected_pure_pretty
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501
        assert newframe.to_sql_query(FrameToSqlConfig()) == expected_sql

        # Filter
        newframe = frame.filter(items=['col1', 'col2']).merge(frame2, how="inner", left_on='col2', right_on='col1')

        expected_pure_pretty = dedent(
            '''\
              #Table(test_schema.test_table)#
                ->select(~[col1, col2])
                ->project(
                  ~[col1_x:x|$x.col1, col2:x|$x.col2]
                )
                ->join(
                  #Table(test_schema.test_table_2)#
                    ->project(
                      ~[col1_y:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]
                    ),
                  JoinKind.INNER,
                  {l, r | $l.col2 == $r.col1_y}
                )'''
        )
        expected_pure_compact = (
            "#Table(test_schema.test_table)#"
            "->select(~[col1, col2])"
            "->project(~[col1_x:x|$x.col1, col2:x|$x.col2])"
            "->join(#Table(test_schema.test_table_2)#"
            "->project(~[col1_y:x|$x.col1, pol2:x|$x.pol2, pol3:x|$x.pol3]), "
            "JoinKind.INNER, {l, r | $l.col2 == $r.col1_y})"
        )
        expected_sql = dedent(
            '''\
            SELECT
                "root"."col1_x" AS "col1_x",
                "root"."col2" AS "col2",
                "root"."col1_y" AS "col1_y",
                "root"."pol2" AS "pol2",
                "root"."pol3" AS "pol3"
            FROM
                (
                    SELECT
                        "left"."col1" AS "col1_x",
                        "left"."col2" AS "col2",
                        "right"."col1" AS "col1_y",
                        "right"."pol2" AS "pol2",
                        "right"."pol3" AS "pol3"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "left"
                        INNER JOIN
                            (
                                SELECT
                                    "root".col1 AS "col1",
                                    "root".pol2 AS "pol2",
                                    "root".pol3 AS "pol3"
                                FROM
                                    test_schema.test_table_2 AS "root"
                            ) AS "right"
                            ON ("left"."col2" = "right"."col1")
                ) AS "root"'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == expected_pure_pretty
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501
        assert newframe.to_sql_query(FrameToSqlConfig()) == expected_sql

    def test_e2e_merge(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame2: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        # on with suffix
        newframe = frame.merge(frame2, how='left', on=['First Name', 'Age'])
        expected = {
            "columns": [
                "First Name",
                "Last Name_x",
                "Age",
                "Firm/Legal Name_x",
                "Last Name_y",
                "Firm/Legal Name_y",
            ],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", "Smith", "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X", "Johnson", "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X", "Hill", "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X", "Allen", "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", "Roberts", "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B", "Hill", "Firm B"]},
                {"values": ["David", "Harris", 35, "Firm C", "Harris", "Firm C"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # left/right on
        frame = frame.rename({"First Name": "FirstName"})
        newframe = frame.merge(frame2, how='left', left_on=['FirstName'], right_on=['First Name'])
        expected = {
            "columns": [
                "FirstName",
                "Last Name_x",
                "Age_x",
                "Firm/Legal Name_x",
                "First Name",
                "Last Name_y",
                "Age_y",
                "Firm/Legal Name_y",
            ],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", "Peter", "Smith", 23, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X", "John", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X", "John", "Hill", 12, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X", "John", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X", "John", "Hill", 12, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X", "Anthony", "Allen", 22, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", "Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B", "Oliver", "Hill", 32, "Firm B"]},
                {"values": ["David", "Harris", 35, "Firm C", "David", "Harris", 35, "Firm C"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # right join
        newframe = frame.merge(frame2, how='right', left_on=['FirstName'], right_on=['First Name'])
        expected = {
            "columns": [
                "FirstName",
                "Last Name_x",
                "Age_x",
                "Firm/Legal Name_x",
                "First Name",
                "Last Name_y",
                "Age_y",
                "Firm/Legal Name_y",
            ],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", "Peter", "Smith", 23, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X", "John", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X", "John", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X", "John", "Hill", 12, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X", "John", "Hill", 12, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X", "Anthony", "Allen", 22, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", "Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B", "Oliver", "Hill", 32, "Firm B"]},
                {"values": ["David", "Harris", 35, "Firm C", "David", "Harris", 35, "Firm C"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # full join
        newframe = frame.merge(frame2, how='outer', left_on=['FirstName'], right_on=['First Name'])
        expected = {
            "columns": [
                "FirstName",
                "Last Name_x",
                "Age_x",
                "Firm/Legal Name_x",
                "First Name",
                "Last Name_y",
                "Age_y",
                "Firm/Legal Name_y",
            ],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", "Peter", "Smith", 23, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X", "John", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X", "John", "Hill", 12, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X", "John", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X", "John", "Hill", 12, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X", "Anthony", "Allen", 22, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", "Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B", "Oliver", "Hill", 32, "Firm B"]},
                {"values": ["David", "Harris", 35, "Firm C", "David", "Harris", 35, "Firm C"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # cross join
        newframe = frame.merge(frame2, how='cross')
        expected = {
            "columns": [
                "FirstName",
                "Last Name_x",
                "Age_x",
                "Firm/Legal Name_x",
                "First Name",
                "Last Name_y",
                "Age_y",
                "Firm/Legal Name_y",
            ],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", "Peter", "Smith", 23, "Firm X"]},
                {"values": ["Peter", "Smith", 23, "Firm X", "John", "Johnson", 22, "Firm X"]},
                {"values": ["Peter", "Smith", 23, "Firm X", "John", "Hill", 12, "Firm X"]},
                {"values": ["Peter", "Smith", 23, "Firm X", "Anthony", "Allen", 22, "Firm X"]},
                {"values": ["Peter", "Smith", 23, "Firm X", "Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Peter", "Smith", 23, "Firm X", "Oliver", "Hill", 32, "Firm B"]},
                {"values": ["Peter", "Smith", 23, "Firm X", "David", "Harris", 35, "Firm C"]},
                {"values": ["John", "Johnson", 22, "Firm X", "Peter", "Smith", 23, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X", "John", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X", "John", "Hill", 12, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X", "Anthony", "Allen", 22, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X", "Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["John", "Johnson", 22, "Firm X", "Oliver", "Hill", 32, "Firm B"]},
                {"values": ["John", "Johnson", 22, "Firm X", "David", "Harris", 35, "Firm C"]},
                {"values": ["John", "Hill", 12, "Firm X", "Peter", "Smith", 23, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X", "John", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X", "John", "Hill", 12, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X", "Anthony", "Allen", 22, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X", "Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["John", "Hill", 12, "Firm X", "Oliver", "Hill", 32, "Firm B"]},
                {"values": ["John", "Hill", 12, "Firm X", "David", "Harris", 35, "Firm C"]},
                {"values": ["Anthony", "Allen", 22, "Firm X", "Peter", "Smith", 23, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X", "John", "Johnson", 22, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X", "John", "Hill", 12, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X", "Anthony", "Allen", 22, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X", "Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Anthony", "Allen", 22, "Firm X", "Oliver", "Hill", 32, "Firm B"]},
                {"values": ["Anthony", "Allen", 22, "Firm X", "David", "Harris", 35, "Firm C"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", "Peter", "Smith", 23, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", "John", "Johnson", 22, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", "John", "Hill", 12, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", "Anthony", "Allen", 22, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", "Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", "Oliver", "Hill", 32, "Firm B"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", "David", "Harris", 35, "Firm C"]},
                {"values": ["Oliver", "Hill", 32, "Firm B", "Peter", "Smith", 23, "Firm X"]},
                {"values": ["Oliver", "Hill", 32, "Firm B", "John", "Johnson", 22, "Firm X"]},
                {"values": ["Oliver", "Hill", 32, "Firm B", "John", "Hill", 12, "Firm X"]},
                {"values": ["Oliver", "Hill", 32, "Firm B", "Anthony", "Allen", 22, "Firm X"]},
                {"values": ["Oliver", "Hill", 32, "Firm B", "Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B", "Oliver", "Hill", 32, "Firm B"]},
                {"values": ["Oliver", "Hill", 32, "Firm B", "David", "Harris", 35, "Firm C"]},
                {"values": ["David", "Harris", 35, "Firm C", "Peter", "Smith", 23, "Firm X"]},
                {"values": ["David", "Harris", 35, "Firm C", "John", "Johnson", 22, "Firm X"]},
                {"values": ["David", "Harris", 35, "Firm C", "John", "Hill", 12, "Firm X"]},
                {"values": ["David", "Harris", 35, "Firm C", "Anthony", "Allen", 22, "Firm X"]},
                {"values": ["David", "Harris", 35, "Firm C", "Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["David", "Harris", 35, "Firm C", "Oliver", "Hill", 32, "Firm B"]},
                {"values": ["David", "Harris", 35, "Firm C", "David", "Harris", 35, "Firm C"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_merge_sort_join(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame2: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        # join with sort
        newframe = frame.join(frame2, on=['First Name', 'Age'], sort=True, lsuffix='_x', rsuffix='_y')
        expected = {
            "columns": [
                "First Name",
                "Last Name_x",
                "Age",
                "Firm/Legal Name_x",
                "Last Name_y",
                "Firm/Legal Name_y",
            ],
            "rows": [
                {"values": ["Anthony", "Allen", 22, "Firm X", "Allen", "Firm X"]},
                {"values": ["David", "Harris", 35, "Firm C", "Harris", "Firm C"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", "Roberts", "Firm A"]},
                {"values": ["John", "Hill", 12, "Firm X", "Hill", "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X", "Johnson", "Firm X"]},
                {"values": ["Oliver", "Hill", 32, "Firm B", "Hill", "Firm B"]},
                {"values": ["Peter", "Smith", 23, "Firm X", "Smith", "Firm X"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_merge_chained(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame2: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        # merge
        newframe = frame.merge(frame2, how='left', on=['First Name', 'Age']).merge(frame2, how='left', on=['First Name'])
        expected = {
            "columns": [
                "First Name",
                "Last Name_x",
                "Age_x",
                "Firm/Legal Name_x",
                "Last Name_y",
                "Firm/Legal Name_y",
                "Last Name",
                "Age_y",
                "Firm/Legal Name",
            ],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", "Smith", "Firm X", "Smith", 23, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X", "Johnson", "Firm X", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X", "Johnson", "Firm X", "Hill", 12, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X", "Hill", "Firm X", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X", "Hill", "Firm X", "Hill", 12, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X", "Allen", "Firm X", "Allen", 22, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", "Roberts", "Firm A", "Roberts", 34, "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B", "Hill", "Firm B", "Hill", 32, "Firm B"]},
                {"values": ["David", "Harris", 35, "Firm C", "Harris", "Firm C", "Harris", 35, "Firm C"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # truncate
        newframe = frame.merge(frame2, how='left', on=['First Name', 'Age']).truncate(before=2, after=5)
        expected = {
            "columns": [
                "First Name",
                "Last Name_x",
                "Age",
                "Firm/Legal Name_x",
                "Last Name_y",
                "Firm/Legal Name_y",
            ],
            "rows": [
                {"values": ["John", "Hill", 12, "Firm X", "Hill", "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X", "Allen", "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", "Roberts", "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B", "Hill", "Firm B"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # filter
        newframe = frame.filter(items=['First Name', 'Age']).merge(frame2, how='left', left_on=['First Name'], right_on=['Last Name'])  # noqa: E501
        expected = {
            "columns": [
                "First Name_x",
                "Age_x",
                "First Name_y",
                "Last Name",
                "Age_y",
                "Firm/Legal Name",
            ],
            "rows": [
                {"values": ["Peter", 23, None, None, None, None]},
                {"values": ["John", 22, None, None, None, None]},
                {"values": ["John", 12, None, None, None, None]},
                {"values": ["Anthony", 22, None, None, None, None]},
                {"values": ["Fabrice", 34, None, None, None, None]},
                {"values": ["Oliver", 32, None, None, None, None]},
                {"values": ["David", 35, None, None, None, None]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
