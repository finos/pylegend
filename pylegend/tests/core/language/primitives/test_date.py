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

from pylegend.core.databse.sql_to_string import (
    SqlToStringFormat,
    SqlToStringConfig,
    SqlToStringDbExtension,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.extensions.tds.legend_api.frames.legend_api_table_spec_input_frame import LegendApiTableSpecInputFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.language import TdsRow


class TestPyLegendDate:
    frame_to_sql_config = FrameToSqlConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    test_frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.date_column("col1"),
        PrimitiveTdsColumn.date_column("col2")
    ])
    tds_row = TdsRow.from_tds_frame("t", test_frame)
    base_query = test_frame.to_sql_query_object(frame_to_sql_config)

    def test_date_col_access(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2")) == '"root".col2'

    def test_first_day_of_year(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_year()) == \
               'DATE_TRUNC(\'year\', "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_quarter().first_day_of_year()) == \
               'DATE_TRUNC(\'year\', DATE_TRUNC(\'quarter\', "root".col2))'

    def test_first_day_of_quarter(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_quarter()) == \
               'DATE_TRUNC(\'quarter\', "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_year().first_day_of_quarter()) == \
               'DATE_TRUNC(\'quarter\', DATE_TRUNC(\'year\', "root".col2))'

    def test_first_day_of_month(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_month()) == \
               'DATE_TRUNC(\'month\', "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_year().first_day_of_month()) == \
               'DATE_TRUNC(\'month\', DATE_TRUNC(\'year\', "root".col2))'

    def test_first_day_of_week(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_week()) == \
               'DATE_TRUNC(\'week\', "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_year().first_day_of_week()) == \
               'DATE_TRUNC(\'week\', DATE_TRUNC(\'year\', "root".col2))'

    def __generate_sql_string(self, f) -> str:  # type: ignore
        return self.db_extension.process_expression(
            f(self.tds_row).to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
