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
from pylegend.core.language import LegendApiTdsRow, today, now


class TestLegendApiDate:
    frame_to_sql_config = FrameToSqlConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    test_frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.date_column("col1"),
        PrimitiveTdsColumn.date_column("col2")
    ])
    tds_row = LegendApiTdsRow.from_tds_frame("t", test_frame)
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

    def test_first_hour_of_day(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_hour_of_day()) == \
               'DATE_TRUNC(\'day\', "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_year().first_hour_of_day()) == \
               'DATE_TRUNC(\'day\', DATE_TRUNC(\'year\', "root".col2))'

    def test_first_minute_of_hour(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_minute_of_hour()) == \
               'DATE_TRUNC(\'hour\', "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_year().first_minute_of_hour()) == \
               'DATE_TRUNC(\'hour\', DATE_TRUNC(\'year\', "root".col2))'

    def test_first_second_of_minute(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_second_of_minute()) == \
               'DATE_TRUNC(\'minute\', "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_year().first_second_of_minute()) ==\
               'DATE_TRUNC(\'minute\', DATE_TRUNC(\'year\', "root".col2))'

    def test_first_millisecond_of_second(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_millisecond_of_second()) == \
               'DATE_TRUNC(\'second\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_day_of_year().first_millisecond_of_second()) ==
                'DATE_TRUNC(\'second\', DATE_TRUNC(\'year\', "root".col2))')

    def test_year(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").year()) == \
               'DATE_PART(\'year\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().year()) ==
                'DATE_PART(\'year\', DATE_TRUNC(\'hour\', "root".col2))')

    def test_month(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").month()) == \
               'DATE_PART(\'month\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().month()) ==
                'DATE_PART(\'month\', DATE_TRUNC(\'hour\', "root".col2))')

    def test_day(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").day()) == \
               'DATE_PART(\'day\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().day()) ==
                'DATE_PART(\'day\', DATE_TRUNC(\'hour\', "root".col2))')

    def test_hour(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").hour()) == \
               'DATE_PART(\'hour\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().hour()) ==
                'DATE_PART(\'hour\', DATE_TRUNC(\'hour\', "root".col2))')

    def test_minute(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").minute()) == \
               'DATE_PART(\'minute\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().minute()) ==
                'DATE_PART(\'minute\', DATE_TRUNC(\'hour\', "root".col2))')

    def test_second(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").second()) == \
               'DATE_PART(\'second\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().second()) ==
                'DATE_PART(\'second\', DATE_TRUNC(\'hour\', "root".col2))')

    def test_epoch_value(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").epoch_value()) == \
               'DATE_PART(\'epoch\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().epoch_value()) ==
                'DATE_PART(\'epoch\', DATE_TRUNC(\'hour\', "root".col2))')

    def test_quarter(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").quarter()) == \
               'DATE_PART(\'quarter\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().quarter()) ==
                'DATE_PART(\'quarter\', DATE_TRUNC(\'hour\', "root".col2))')

    def test_week_of_year(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").week_of_year()) == \
               'DATE_PART(\'week\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().week_of_year()) ==
                'DATE_PART(\'week\', DATE_TRUNC(\'hour\', "root".col2))')

    def test_day_of_year(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").day_of_year()) == \
               'DATE_PART(\'doy\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().day_of_year()) ==
                'DATE_PART(\'doy\', DATE_TRUNC(\'hour\', "root".col2))')

    def test_day_of_week(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").day_of_week()) == \
               'DATE_PART(\'dow\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().day_of_week()) ==
                'DATE_PART(\'dow\', DATE_TRUNC(\'hour\', "root".col2))')

    def test_today(self) -> None:
        assert self.__generate_sql_string(lambda x: today()) == \
               'CURRENT_DATE'
        assert (self.__generate_sql_string(
            lambda x: today().first_minute_of_hour().day_of_week()) ==
                'DATE_PART(\'dow\', DATE_TRUNC(\'hour\', CURRENT_DATE))')

    def test_now(self) -> None:
        assert self.__generate_sql_string(lambda x: now()) == \
               'CURRENT_TIMESTAMP'
        assert (self.__generate_sql_string(
            lambda x: now().first_minute_of_hour().day_of_week()) ==
                'DATE_PART(\'dow\', DATE_TRUNC(\'hour\', CURRENT_TIMESTAMP))')

    def __generate_sql_string(self, f) -> str:  # type: ignore
        return self.db_extension.process_expression(
            f(self.tds_row).to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
