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
from pylegend.core.databse.sql_to_string import (
    SqlToStringFormat,
    SqlToStringConfig,
    SqlToStringDbExtension,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.extensions.tds.legend_api.frames.legend_api_table_spec_input_frame import LegendApiTableSpecInputFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.language import TdsRow, PyLegendBoolean, PyLegendString, PyLegendNumber, \
    PyLegendInteger, PyLegendFloat


class TestTdsRow:
    frame_to_sql_config = FrameToSqlConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))

    def test_error_on_unknown_column(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.boolean_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        tds_row = TdsRow.from_tds_frame("t", frame)

        with pytest.raises(ValueError) as v:
            tds_row.get_boolean("UNKNOWN_COL")

        assert v.value.args[0] == \
               "Column - 'UNKNOWN_COL' doesn't exist in the current frame. Current frame columns: ['col1', 'col2']"

    def test_error_on_unknown_column_getitem(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.boolean_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        tds_row = TdsRow.from_tds_frame("t", frame)

        with pytest.raises(ValueError) as v:
            tds_row["UNKNOWN_COL"]

        assert v.value.args[0] == \
               "Column - 'UNKNOWN_COL' doesn't exist in the current frame. Current frame columns: ['col1', 'col2']"

    def test_error_on_invalid_type(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("col1"),
            PrimitiveTdsColumn.boolean_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        tds_row = TdsRow.from_tds_frame("t", frame)

        with pytest.raises(RuntimeError) as v:
            tds_row.get_boolean("col1")

        assert v.value.args[0] == \
               "Column expression for 'col1' is of type " \
               "'<class 'pylegend.core.language.column_expressions.PyLegendStringColumnExpression'>'. " \
               "get_boolean method is not valid on this column."

    def test_get_boolean_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.boolean_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        tds_row = TdsRow.from_tds_frame("t", frame)
        col_expr = tds_row.get_boolean("col2")

        assert isinstance(col_expr, PyLegendBoolean)
        assert self.db_extension.process_expression(
            col_expr.to_sql_expression(
                {"t": frame.to_sql_query_object(self.frame_to_sql_config)},
                self.frame_to_sql_config
            ),
            config=self.sql_to_string_config
        ) == '"root".col2'

    def test_getitem_boolean_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.boolean_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        tds_row = TdsRow.from_tds_frame("t", frame)
        col_expr = tds_row["col2"]

        assert isinstance(col_expr, PyLegendBoolean)
        assert self.db_extension.process_expression(
            col_expr.to_sql_expression(
                {"t": frame.to_sql_query_object(self.frame_to_sql_config)},
                self.frame_to_sql_config
            ),
            config=self.sql_to_string_config
        ) == '"root".col2'

    def test_get_string_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        tds_row = TdsRow.from_tds_frame("t", frame)
        col_expr = tds_row.get_string("col2")

        assert isinstance(col_expr, PyLegendString)
        assert self.db_extension.process_expression(
            col_expr.to_sql_expression(
                {"t": frame.to_sql_query_object(self.frame_to_sql_config)},
                self.frame_to_sql_config
            ),
            config=self.sql_to_string_config
        ) == '"root".col2'

    def test_getitem_string_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        tds_row = TdsRow.from_tds_frame("t", frame)
        col_expr = tds_row["col2"]

        assert isinstance(col_expr, PyLegendString)
        assert self.db_extension.process_expression(
            col_expr.to_sql_expression(
                {"t": frame.to_sql_query_object(self.frame_to_sql_config)},
                self.frame_to_sql_config
            ),
            config=self.sql_to_string_config
        ) == '"root".col2'

    def test_get_number_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.number_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        tds_row = TdsRow.from_tds_frame("t", frame)
        col_expr = tds_row.get_number("col1")

        assert isinstance(col_expr, PyLegendNumber)
        assert self.db_extension.process_expression(
            col_expr.to_sql_expression(
                {"t": frame.to_sql_query_object(self.frame_to_sql_config)},
                self.frame_to_sql_config
            ),
            config=self.sql_to_string_config
        ) == '"root".col1'

    def test_getitem_number_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.number_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        tds_row = TdsRow.from_tds_frame("t", frame)
        col_expr = tds_row["col1"]

        assert isinstance(col_expr, PyLegendNumber)
        assert self.db_extension.process_expression(
            col_expr.to_sql_expression(
                {"t": frame.to_sql_query_object(self.frame_to_sql_config)},
                self.frame_to_sql_config
            ),
            config=self.sql_to_string_config
        ) == '"root".col1'

    def test_get_number_col_from_integer(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        tds_row = TdsRow.from_tds_frame("t", frame)
        col_expr = tds_row.get_number("col1")

        assert isinstance(col_expr, PyLegendNumber)
        assert self.db_extension.process_expression(
            col_expr.to_sql_expression(
                {"t": frame.to_sql_query_object(self.frame_to_sql_config)},
                self.frame_to_sql_config
            ),
            config=self.sql_to_string_config
        ) == '"root".col1'

    def test_get_number_col_from_float(self) -> None:
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        tds_row = TdsRow.from_tds_frame("t", frame)
        col_expr = tds_row.get_number("col1")

        assert isinstance(col_expr, PyLegendNumber)
        assert self.db_extension.process_expression(
            col_expr.to_sql_expression(
                {"t": frame.to_sql_query_object(self.frame_to_sql_config)},
                self.frame_to_sql_config
            ),
            config=self.sql_to_string_config
        ) == '"root".col1'

    def test_get_integer_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        tds_row = TdsRow.from_tds_frame("t", frame)
        col_expr = tds_row.get_integer("col1")

        assert isinstance(col_expr, PyLegendInteger)
        assert self.db_extension.process_expression(
            col_expr.to_sql_expression(
                {"t": frame.to_sql_query_object(self.frame_to_sql_config)},
                self.frame_to_sql_config
            ),
            config=self.sql_to_string_config
        ) == '"root".col1'

    def test_getitem_integer_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        tds_row = TdsRow.from_tds_frame("t", frame)
        col_expr = tds_row["col1"]

        assert isinstance(col_expr, PyLegendInteger)
        assert self.db_extension.process_expression(
            col_expr.to_sql_expression(
                {"t": frame.to_sql_query_object(self.frame_to_sql_config)},
                self.frame_to_sql_config
            ),
            config=self.sql_to_string_config
        ) == '"root".col1'

    def test_get_float_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        tds_row = TdsRow.from_tds_frame("t", frame)
        col_expr = tds_row.get_float("col1")

        assert isinstance(col_expr, PyLegendFloat)
        assert self.db_extension.process_expression(
            col_expr.to_sql_expression(
                {"t": frame.to_sql_query_object(self.frame_to_sql_config)},
                self.frame_to_sql_config
            ),
            config=self.sql_to_string_config
        ) == '"root".col1'

    def test_getitem_float_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        tds_row = TdsRow.from_tds_frame("t", frame)
        col_expr = tds_row["col1"]

        assert isinstance(col_expr, PyLegendFloat)
        assert self.db_extension.process_expression(
            col_expr.to_sql_expression(
                {"t": frame.to_sql_query_object(self.frame_to_sql_config)},
                self.frame_to_sql_config
            ),
            config=self.sql_to_string_config
        ) == '"root".col1'
