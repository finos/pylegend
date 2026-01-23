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
import json
import datetime
import pytest
from pylegend.core.database.sql_to_string import (
    SqlToStringFormat,
    SqlToStringConfig,
    SqlToStringDbExtension,
)
from pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame import LegendQLApiTdsFrame
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.language import today, now
from pylegend.core.request.legend_client import LegendClient
from pylegend._typing import PyLegendDict, PyLegendUnion
from tests.core.language.shared import TestTableSpecInputFrame, TestTdsRow
from tests.test_helpers.test_legend_service_frames import simple_relation_trade_service_frame_legendql_api


class TestPyLegendDate:
    frame_to_sql_config = FrameToSqlConfig()
    frame_to_pure_config = FrameToPureConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    test_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.date_column("col1"),
        PrimitiveTdsColumn.date_column("col2")
    ])
    tds_row = TestTdsRow.from_tds_frame("t", test_frame)
    base_query = test_frame.to_sql_query_object(frame_to_sql_config)

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_date_col_access(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2")) == '"root".col2'
        assert self.__generate_pure_string(lambda x: x.get_date("col2")) == '$t.col2'

    def test_first_day_of_year(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_year()) == \
               'DATE_TRUNC(\'year\', "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_quarter().first_day_of_year()) == \
               'DATE_TRUNC(\'year\', DATE_TRUNC(\'quarter\', "root".col2))'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_day_of_year()) == \
               'toOne($t.col2)->firstDayOfYear()'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_day_of_quarter().first_day_of_year()) == \
               'toOne($t.col2)->firstDayOfQuarter()->firstDayOfYear()'

    def test_first_day_of_quarter(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_quarter()) == \
               'DATE_TRUNC(\'quarter\', "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_year().first_day_of_quarter()) == \
               'DATE_TRUNC(\'quarter\', DATE_TRUNC(\'year\', "root".col2))'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_day_of_quarter()) == \
               'toOne($t.col2)->firstDayOfQuarter()'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_day_of_year().first_day_of_quarter()) == \
               'toOne($t.col2)->firstDayOfYear()->firstDayOfQuarter()'

    def test_first_day_of_month(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_month()) == \
               'DATE_TRUNC(\'month\', "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_year().first_day_of_month()) == \
               'DATE_TRUNC(\'month\', DATE_TRUNC(\'year\', "root".col2))'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_day_of_month()) == \
               'toOne($t.col2)->firstDayOfMonth()'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_day_of_year().first_day_of_month()) == \
               'toOne($t.col2)->firstDayOfYear()->firstDayOfMonth()'

    def test_first_day_of_week(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_week()) == \
               'DATE_TRUNC(\'week\', "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_year().first_day_of_week()) == \
               'DATE_TRUNC(\'week\', DATE_TRUNC(\'year\', "root".col2))'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_day_of_week()) == \
               'toOne($t.col2)->firstDayOfWeek()'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_day_of_year().first_day_of_week()) == \
               'toOne($t.col2)->firstDayOfYear()->firstDayOfWeek()'

    def test_first_hour_of_day(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_hour_of_day()) == \
               'DATE_TRUNC(\'day\', "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_year().first_hour_of_day()) == \
               'DATE_TRUNC(\'day\', DATE_TRUNC(\'year\', "root".col2))'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_hour_of_day()) == \
               'toOne($t.col2)->firstHourOfDay()'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_day_of_year().first_hour_of_day()) == \
               'toOne($t.col2)->firstDayOfYear()->firstHourOfDay()'

    def test_first_minute_of_hour(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_minute_of_hour()) == \
               'DATE_TRUNC(\'hour\', "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_year().first_minute_of_hour()) == \
               'DATE_TRUNC(\'hour\', DATE_TRUNC(\'year\', "root".col2))'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_minute_of_hour()) == \
               'toOne($t.col2)->firstMinuteOfHour()'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_day_of_year().first_minute_of_hour()) == \
               'toOne($t.col2)->firstDayOfYear()->firstMinuteOfHour()'

    def test_first_second_of_minute(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_second_of_minute()) == \
               'DATE_TRUNC(\'minute\', "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_day_of_year().first_second_of_minute()) ==\
               'DATE_TRUNC(\'minute\', DATE_TRUNC(\'year\', "root".col2))'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_second_of_minute()) == \
               'toOne($t.col2)->firstSecondOfMinute()'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_day_of_year().first_second_of_minute()) ==\
               'toOne($t.col2)->firstDayOfYear()->firstSecondOfMinute()'

    def test_first_millisecond_of_second(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").first_millisecond_of_second()) == \
               'DATE_TRUNC(\'second\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_day_of_year().first_millisecond_of_second()) ==
                'DATE_TRUNC(\'second\', DATE_TRUNC(\'year\', "root".col2))')
        assert self.__generate_pure_string(lambda x: x.get_date("col2").first_millisecond_of_second()) == \
               'toOne($t.col2)->firstMillisecondOfSecond()'
        assert (self.__generate_pure_string(
            lambda x: x.get_date("col2").first_day_of_year().first_millisecond_of_second()) ==
               'toOne($t.col2)->firstDayOfYear()->firstMillisecondOfSecond()')

    def test_year(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").year()) == \
               'DATE_PART(\'year\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().year()) ==
                'DATE_PART(\'year\', DATE_TRUNC(\'hour\', "root".col2))')
        assert self.__generate_pure_string(lambda x: x.get_date("col2").year()) == \
               'toOne($t.col2)->year()'
        assert (self.__generate_pure_string(
            lambda x: x.get_date("col2").first_minute_of_hour().year()) ==
               'toOne($t.col2)->firstMinuteOfHour()->year()')

    def test_month(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").month()) == \
               'DATE_PART(\'month\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().month()) ==
                'DATE_PART(\'month\', DATE_TRUNC(\'hour\', "root".col2))')
        assert self.__generate_pure_string(lambda x: x.get_date("col2").month()) == \
               'toOne($t.col2)->month()'
        assert (self.__generate_pure_string(
            lambda x: x.get_date("col2").first_minute_of_hour().month()) ==
               'toOne($t.col2)->firstMinuteOfHour()->month()')

    def test_day(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").day()) == \
               'DATE_PART(\'day\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().day()) ==
                'DATE_PART(\'day\', DATE_TRUNC(\'hour\', "root".col2))')
        assert self.__generate_pure_string(lambda x: x.get_date("col2").day()) == \
               'toOne($t.col2)->dayOfMonth()'
        assert (self.__generate_pure_string(
            lambda x: x.get_date("col2").first_minute_of_hour().day()) ==
               'toOne($t.col2)->firstMinuteOfHour()->dayOfMonth()')

    def test_hour(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").hour()) == \
               'DATE_PART(\'hour\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().hour()) ==
                'DATE_PART(\'hour\', DATE_TRUNC(\'hour\', "root".col2))')
        assert self.__generate_pure_string(lambda x: x.get_date("col2").hour()) == \
               'toOne($t.col2)->hour()'
        assert (self.__generate_pure_string(
            lambda x: x.get_date("col2").first_minute_of_hour().hour()) ==
               'toOne($t.col2)->firstMinuteOfHour()->hour()')

    def test_minute(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").minute()) == \
               'DATE_PART(\'minute\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().minute()) ==
                'DATE_PART(\'minute\', DATE_TRUNC(\'hour\', "root".col2))')
        assert self.__generate_pure_string(lambda x: x.get_date("col2").minute()) == \
               'toOne($t.col2)->minute()'
        assert (self.__generate_pure_string(
            lambda x: x.get_date("col2").first_minute_of_hour().minute()) ==
               'toOne($t.col2)->firstMinuteOfHour()->minute()')

    def test_second(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").second()) == \
               'DATE_PART(\'second\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().second()) ==
                'DATE_PART(\'second\', DATE_TRUNC(\'hour\', "root".col2))')
        assert self.__generate_pure_string(lambda x: x.get_date("col2").second()) == \
               'toOne($t.col2)->second()'
        assert (self.__generate_pure_string(
            lambda x: x.get_date("col2").first_minute_of_hour().second()) ==
               'toOne($t.col2)->firstMinuteOfHour()->second()')

    def test_epoch_value(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").epoch_value()) == \
               'DATE_PART(\'epoch\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().epoch_value()) ==
                'DATE_PART(\'epoch\', DATE_TRUNC(\'hour\', "root".col2))')
        assert self.__generate_pure_string(lambda x: x.get_date("col2").epoch_value()) == \
               'toOne($t.col2)->toEpochValue()'
        assert (self.__generate_pure_string(
            lambda x: x.get_date("col2").first_minute_of_hour().epoch_value()) ==
               'toOne($t.col2)->firstMinuteOfHour()->toEpochValue()')

    def test_quarter(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").quarter()) == \
               'DATE_PART(\'quarter\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().quarter()) ==
                'DATE_PART(\'quarter\', DATE_TRUNC(\'hour\', "root".col2))')
        assert self.__generate_pure_string(lambda x: x.get_date("col2").quarter()) == \
               'toOne($t.col2)->quarter()'
        assert (self.__generate_pure_string(
            lambda x: x.get_date("col2").first_minute_of_hour().quarter()) ==
               'toOne($t.col2)->firstMinuteOfHour()->quarter()')

    def test_week_of_year(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").week_of_year()) == \
               'DATE_PART(\'week\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().week_of_year()) ==
                'DATE_PART(\'week\', DATE_TRUNC(\'hour\', "root".col2))')
        assert self.__generate_pure_string(lambda x: x.get_date("col2").week_of_year()) == \
               'toOne($t.col2)->weekOfYear()'
        assert (self.__generate_pure_string(
            lambda x: x.get_date("col2").first_minute_of_hour().week_of_year()) ==
               'toOne($t.col2)->firstMinuteOfHour()->weekOfYear()')

    def test_day_of_year(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").day_of_year()) == \
               'DATE_PART(\'doy\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().day_of_year()) ==
                'DATE_PART(\'doy\', DATE_TRUNC(\'hour\', "root".col2))')
        assert self.__generate_pure_string(lambda x: x.get_date("col2").day_of_year()) == \
               'toOne($t.col2)->dayOfYear()'
        assert (self.__generate_pure_string(
            lambda x: x.get_date("col2").first_minute_of_hour().day_of_year()) ==
               'toOne($t.col2)->firstMinuteOfHour()->dayOfYear()')

    def test_day_of_week(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").day_of_week()) == \
               'DATE_PART(\'dow\', "root".col2)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().day_of_week()) ==
                'DATE_PART(\'dow\', DATE_TRUNC(\'hour\', "root".col2))')
        assert self.__generate_pure_string(lambda x: x.get_date("col2").day_of_week()) == \
               'toOne($t.col2)->dayOfWeekNumber()'
        assert (self.__generate_pure_string(
            lambda x: x.get_date("col2").first_minute_of_hour().day_of_week()) ==
               'toOne($t.col2)->firstMinuteOfHour()->dayOfWeekNumber()')

    def test_today(self) -> None:
        assert self.__generate_sql_string(lambda x: today()) == \
               'CURRENT_DATE'
        assert (self.__generate_sql_string(
            lambda x: today().first_minute_of_hour().day_of_week()) ==
                'DATE_PART(\'dow\', DATE_TRUNC(\'hour\', CURRENT_DATE))')
        assert self.__generate_pure_string(lambda x: today()) == \
               'today()'
        assert (self.__generate_pure_string(
            lambda x: today().first_minute_of_hour().day_of_week()) ==
                'today()->firstMinuteOfHour()->dayOfWeekNumber()')

    def test_now(self) -> None:
        assert self.__generate_sql_string(lambda x: now()) == \
               'CURRENT_TIMESTAMP'
        assert (self.__generate_sql_string(
            lambda x: now().first_minute_of_hour().day_of_week()) ==
                'DATE_PART(\'dow\', DATE_TRUNC(\'hour\', CURRENT_TIMESTAMP))')
        assert self.__generate_pure_string(lambda x: now()) == \
               'now()'
        assert (self.__generate_pure_string(
            lambda x: now().first_minute_of_hour().day_of_week()) ==
                'now()->firstMinuteOfHour()->dayOfWeekNumber()')

    def test_date_part(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").date_part()) == \
               'CAST("root".col2 AS DATE)'
        assert (self.__generate_sql_string(
            lambda x: x.get_date("col2").first_minute_of_hour().date_part()) ==
                'CAST(DATE_TRUNC(\'hour\', "root".col2) AS DATE)')
        assert self.__generate_pure_string(lambda x: x.get_date("col2").date_part()) == \
               'toOne($t.col2)->datePart()->cast(@StrictDate)'
        assert (self.__generate_pure_string(
            lambda x: x.get_date("col2").first_minute_of_hour().date_part()) ==
               'toOne($t.col2)->firstMinuteOfHour()->datePart()->cast(@StrictDate)')

    def test_date_lt_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2") < x.get_date("col1")) == \
               '("root".col2 < "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2") < datetime.date(2025, 1, 1)) == \
               '("root".col2 < CAST(\'2025-01-01\' AS DATE))'
        assert self.__generate_sql_string(lambda x: datetime.datetime(2025, 1, 1, 10, 00, 00) < x.get_date("col2")) == \
               '("root".col2 > CAST(\'2025-01-01T10:00:00\' AS TIMESTAMP))'
        assert self.__generate_pure_string(lambda x: x.get_date("col2") < x.get_date("col1")) == \
               '($t.col2 < $t.col1)'
        assert self.__generate_pure_string(lambda x: x.get_date("col2") < datetime.date(2025, 1, 1)) == \
               '($t.col2 < %2025-01-01)'
        assert self.__generate_pure_string(lambda x: datetime.datetime(2025, 1, 1, 10, 00, 00) < x.get_date("col2")) == \
               '($t.col2 > %2025-01-01T10:00:00)'

    def test_date_le_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2") <= x.get_date("col1")) == \
               '("root".col2 <= "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2") <= datetime.date(2025, 1, 1)) == \
               '("root".col2 <= CAST(\'2025-01-01\' AS DATE))'
        assert self.__generate_sql_string(lambda x: datetime.datetime(2025, 1, 1, 10, 00, 00) <= x.get_date("col2")) == \
               '("root".col2 >= CAST(\'2025-01-01T10:00:00\' AS TIMESTAMP))'
        assert self.__generate_pure_string(lambda x: x.get_date("col2") <= x.get_date("col1")) == \
               '($t.col2 <= $t.col1)'
        assert self.__generate_pure_string(lambda x: x.get_date("col2") <= datetime.date(2025, 1, 1)) == \
               '($t.col2 <= %2025-01-01)'
        assert self.__generate_pure_string(lambda x: datetime.datetime(2025, 1, 1, 10, 00, 00) <= x.get_date("col2")) == \
               '($t.col2 >= %2025-01-01T10:00:00)'

    def test_date_gt_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2") > x.get_date("col1")) == \
               '("root".col2 > "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2") > datetime.date(2025, 1, 1)) == \
               '("root".col2 > CAST(\'2025-01-01\' AS DATE))'
        assert self.__generate_sql_string(lambda x: datetime.datetime(2025, 1, 1, 10, 00, 00) > x.get_date("col2")) == \
               '("root".col2 < CAST(\'2025-01-01T10:00:00\' AS TIMESTAMP))'
        assert self.__generate_pure_string(lambda x: x.get_date("col2") > x.get_date("col1")) == \
               '($t.col2 > $t.col1)'
        assert self.__generate_pure_string(lambda x: x.get_date("col2") > datetime.date(2025, 1, 1)) == \
               '($t.col2 > %2025-01-01)'
        assert self.__generate_pure_string(lambda x: datetime.datetime(2025, 1, 1, 10, 00, 00) > x.get_date("col2")) == \
               '($t.col2 < %2025-01-01T10:00:00)'

    def test_date_ge_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2") >= x.get_date("col1")) == \
               '("root".col2 >= "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2") >= datetime.date(2025, 1, 1)) == \
               '("root".col2 >= CAST(\'2025-01-01\' AS DATE))'
        assert self.__generate_sql_string(lambda x: datetime.datetime(2025, 1, 1, 10, 00, 00) >= x.get_date("col2")) == \
               '("root".col2 <= CAST(\'2025-01-01T10:00:00\' AS TIMESTAMP))'
        assert self.__generate_pure_string(lambda x: x.get_date("col2") >= x.get_date("col1")) == \
               '($t.col2 >= $t.col1)'
        assert self.__generate_pure_string(lambda x: x.get_date("col2") >= datetime.date(2025, 1, 1)) == \
               '($t.col2 >= %2025-01-01)'
        assert self.__generate_pure_string(lambda x: datetime.datetime(2025, 1, 1, 10, 00, 00) >= x.get_date("col2")) == \
               '($t.col2 <= %2025-01-01T10:00:00)'

    def test_date_adjust_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").timedelta(2, "YEARS")) == \
               '("root".col2::DATE + (INTERVAL \'2 YEARS\'))::DATE'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").timedelta(-2, "MONTHS")) == \
               '("root".col2::DATE + (INTERVAL \'-2 MONTHS\'))::DATE'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").timedelta(2, "YEARS")) == \
               'toOne($t.col2)->adjust(2, DurationUnit.\'YEARS\')'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").timedelta(-2, "YEARS")) == \
               'toOne($t.col2)->adjust(minus(2), DurationUnit.\'YEARS\')'

        with pytest.raises(ValueError) as t:
            self.__generate_sql_string(lambda x: x.get_date("col2").timedelta(2, "Invalid"))
        assert t.value.args[0] == ("Unknown duration unit - Invalid. Supported values are - YEARS, MONTHS, WEEKS, "
                                   "DAYS, HOURS, MINUTES, SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS")

    def test_date_diff_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_date("col2").diff(x.get_date("col1"), "YEARS")) == \
               'CAST((EXTRACT(YEAR FROM "root".col2) - EXTRACT(YEAR FROM "root".col1)) AS INTEGER)'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").diff(x.get_date("col1"), "YEARS")) == \
               'toOne($t.col2)->dateDiff(toOne($t.col1), DurationUnit.\'YEARS\')'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").diff(x.get_date("col1"), "years")) == \
               'CAST((EXTRACT(YEAR FROM "root".col2) - EXTRACT(YEAR FROM "root".col1)) AS INTEGER)'
        assert self.__generate_pure_string(lambda x: x.get_date("col2").diff(x.get_date("col1"), "years")) == \
               'toOne($t.col2)->dateDiff(toOne($t.col1), DurationUnit.\'YEARS\')'

        assert self.__generate_sql_string(lambda x: x.get_date("col2").diff(x.get_date("col1"), "MONTHS")) == \
               ('(CAST((EXTRACT(YEAR FROM "root".col2) - EXTRACT(YEAR FROM "root".col1)) AS INTEGER) * 12 + '
                'CAST((EXTRACT(MONTH FROM "root".col2) - EXTRACT(MONTH FROM "root".col1)) AS INTEGER))')

        assert self.__generate_sql_string(lambda x: x.get_date("col2").diff(x.get_date("col1"), "WEEKS")) == \
               'CAST(FLOOR(CAST(CAST("root".col2 AS DATE) - CAST("root".col1 AS DATE) AS INTEGER) / 7) AS INTEGER)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").diff(x.get_date("col1"), "DAYS")) == \
               'CAST(CAST("root".col2 AS DATE) - CAST("root".col1 AS DATE) AS INTEGER)'
        assert self.__generate_sql_string(lambda x: x.get_date("col2").diff(x.get_date("col1"), "HOURS")) == \
               ('CAST((CAST(CAST("root".col2 AS DATE) - CAST("root".col1 AS DATE) AS INTEGER) * 24 + '
                'CAST((EXTRACT(HOUR FROM "root".col2) - EXTRACT(HOUR FROM "root".col1)) AS INTEGER)) AS INTEGER)')
        assert self.__generate_sql_string(lambda x: x.get_date("col2").diff(x.get_date("col1"), "MINUTES")) == \
               ('CAST((CAST((CAST(CAST("root".col2 AS DATE) - CAST("root".col1 AS DATE) AS INTEGER) * 24 +'
                ' CAST((EXTRACT(HOUR FROM "root".col2) - EXTRACT(HOUR FROM "root".col1)) AS INTEGER)) AS INTEGER) * 60 + '
                'CAST((EXTRACT(MINUTE FROM "root".col2) - EXTRACT(MINUTE FROM "root".col1)) AS INTEGER)) AS INTEGER)')
        assert self.__generate_sql_string(lambda x: x.get_date("col2").diff(x.get_date("col1"), "SECONDS")) == \
               ('CAST((CAST((CAST((CAST(CAST("root".col2 AS DATE) - CAST("root".col1 AS DATE) AS INTEGER) * 24 + '
                'CAST((EXTRACT(HOUR FROM "root".col2) - EXTRACT(HOUR FROM "root".col1)) AS INTEGER)) AS INTEGER) * 60 + '
                'CAST((EXTRACT(MINUTE FROM "root".col2) - EXTRACT(MINUTE FROM "root".col1)) AS INTEGER)) AS INTEGER) * 60 + '
                'CAST((EXTRACT(SECOND FROM "root".col2) - EXTRACT(SECOND FROM "root".col1)) AS INTEGER)) AS INTEGER)')

        with pytest.raises(ValueError) as t:
            self.__generate_sql_string(lambda x: x.get_date("col2").diff(x.get_date("col1"), "invalid"))
        assert t.value.args[0] == ("Unknown duration unit - invalid. Supported values are - YEARS, MONTHS, WEEKS, "
                                   "DAYS, HOURS, MINUTES, SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS")

        with pytest.raises(ValueError) as t:
            assert self.__generate_sql_string(lambda x: x.get_date("col2").diff(x.get_date("col1"), "MILLISECONDS"))
        assert t.value.args[0] == "Unsupported DATE DIFF unit: MILLISECONDS"

    def test_e2e_date_adjust_expr_multiple_units(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]
    ) -> None:
        frame: LegendQLApiTdsFrame = simple_relation_trade_service_frame_legendql_api(
            legend_test_server["engine_port"]
        )

        frame = frame.select([
            "Settlement Date Time"
        ])

        frame = frame.extend([
            ("Add 2 Days", lambda r: r["Settlement Date Time"].timedelta(2, "DAYS")),
            ("Sub 1 Day", lambda r: r["Settlement Date Time"].timedelta(-1, "DAYS")),
            ("Add 3 Months", lambda r: r["Settlement Date Time"].timedelta(3, "MONTHS")),
            ("Sub 1 Month", lambda r: r["Settlement Date Time"].timedelta(-1, "MONTHS")),
            ("Add 1 Year", lambda r: r["Settlement Date Time"].timedelta(1, "YEARS")),
            ("Sub 2 Years", lambda r: r["Settlement Date Time"].timedelta(-2, "YEARS")),
            ("Add 5 Hours", lambda r: r["Settlement Date Time"].timedelta(5, "HOURS")),
            ("Sub 3 Hours", lambda r: r["Settlement Date Time"].timedelta(-3, "HOURS")),
            ("Add 30 Minutes", lambda r: r["Settlement Date Time"].timedelta(30, "MINUTES")),
            ("Sub 15 Minutes", lambda r: r["Settlement Date Time"].timedelta(-15, "MINUTES")),
            ("Add 45 Seconds", lambda r: r["Settlement Date Time"].timedelta(45, "SECONDS")),
            ("Sub 10 Seconds", lambda r: r["Settlement Date Time"].timedelta(-10, "SECONDS"))]
        ).sort(
            lambda r: r["Settlement Date Time"].descending()
        ).limit(1)
        print(frame.to_sql_query())
        res = frame.execute_frame_to_string()

        assert json.loads(res)["result"] == {
            "columns": ["Settlement Date Time", "Add 2 Days", "Sub 1 Day", "Add 3 Months", "Sub 1 Month", "Add 1 Year",
                        "Sub 2 Years", "Add 5 Hours", "Sub 3 Hours", "Add 30 Minutes", "Sub 15 Minutes",
                        "Add 45 Seconds", "Sub 10 Seconds",
                        ],
            "rows": [{
                "values": [
                    "2014-12-05T21:00:00.000000000+0000", "2014-12-07T21:00:00.000000000+0000",
                    "2014-12-04T21:00:00.000000000+0000",
                    "2015-03-05T21:00:00.000000000+0000", "2014-11-05T21:00:00.000000000+0000",
                    "2015-12-05T21:00:00.000000000+0000",
                    "2012-12-05T21:00:00.000000000+0000", "2014-12-06T02:00:00.000000000+0000",
                    "2014-12-05T18:00:00.000000000+0000",
                    "2014-12-05T21:30:00.000000000+0000", "2014-12-05T20:45:00.000000000+0000",
                    "2014-12-05T21:00:45.000000000+0000",
                    "2014-12-05T20:59:50.000000000+0000",
                ]
            }]
        }

    def test_e2e_date_diff_expr_multiple_units(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]
    ) -> None:
        frame: LegendQLApiTdsFrame = simple_relation_trade_service_frame_legendql_api(
            legend_test_server["engine_port"]
        )

        frame = frame.select([
            "Settlement Date Time"
        ]).sort(lambda r: r["Settlement Date Time"].descending()).limit(1)

        frame = frame.extend([
            ("dt_minus_5_days", lambda r: r["Settlement Date Time"].timedelta(-5, "DAYS")),
            ("dt_minus_3_weeks", lambda r: r["Settlement Date Time"].timedelta(-3, "WEEKS")),
            ("dt_plus_4_months", lambda r: r["Settlement Date Time"].timedelta(4, "MONTHS")),
            ("dt_minus_1_year", lambda r: r["Settlement Date Time"].timedelta(-1, "YEARS")),
            ("dt_plus_12_hours", lambda r: r["Settlement Date Time"].timedelta(12, "HOURS")),
            ("dt_minus_30_minutes", lambda r: r["Settlement Date Time"].timedelta(-30, "MINUTES")),
            ("dt_plus_90_seconds", lambda r: r["Settlement Date Time"].timedelta(90, "SECONDS"))
        ])

        frame = frame.extend([
            ("days_diff", lambda r: r["dt_minus_5_days"].diff(r["Settlement Date Time"], "DAYS")),
            ("weeks_diff", lambda r: r["dt_minus_3_weeks"].diff(r["Settlement Date Time"], "WEEKS")),
            ("months_diff", lambda r: r["dt_plus_4_months"].diff(r["Settlement Date Time"], "MONTHS")),
            ("months_diff_2", lambda r: r["dt_minus_5_days"].diff(r["Settlement Date Time"], "MONTHS")),
            ("years_diff", lambda r: r["dt_minus_1_year"].diff(r["Settlement Date Time"], "YEARS")),
            ("hours_diff", lambda r: r["dt_plus_12_hours"].diff(r["Settlement Date Time"], "HOURS")),
            ("minutes_diff", lambda r: r["dt_minus_30_minutes"].diff(r["Settlement Date Time"], "MINUTES")),
            ("seconds_diff", lambda r: r["dt_plus_90_seconds"].diff(r["Settlement Date Time"], "SECONDS"))
        ])
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == {
            'columns': ["Settlement Date Time", "dt_minus_5_days", "dt_minus_3_weeks", "dt_plus_4_months",
                        "dt_minus_1_year", "dt_plus_12_hours", "dt_minus_30_minutes", "dt_plus_90_seconds",
                        "days_diff", "weeks_diff", "months_diff", "months_diff_2", "years_diff",
                        "hours_diff", "minutes_diff", "seconds_diff"],
            'rows': [{"values": ["2014-12-05T21:00:00.000000000+0000", "2014-11-30T21:00:00.000000000+0000",
                                 "2014-11-14T21:00:00.000000000+0000", "2015-04-05T21:00:00.000000000+0000",
                                 "2013-12-05T21:00:00.000000000+0000", "2014-12-06T09:00:00.000000000+0000",
                                 "2014-12-05T20:30:00.000000000+0000", "2014-12-05T21:01:30.000000000+0000",
                                 5, 3, 4, -1, -1, -36, -30, 90]}]
        }

    def test_e2e_time_bucket_expr_multiple_units(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]
    ) -> None:
        frame: LegendQLApiTdsFrame = simple_relation_trade_service_frame_legendql_api(
            legend_test_server["engine_port"]
        )

        frame = frame.select([
            "Settlement Date Time",
            "Date"
        ])

        frame = frame.extend([
            ("dt_bucket_5_days", lambda r: r["Settlement Date Time"].time_bucket(5, "DAYS")),
            ("dt_bucket_4_months", lambda r: r["Settlement Date Time"].time_bucket(4, "MONTHS")),
            ("dt_bucket_3_years", lambda r: r["Settlement Date Time"].time_bucket(3, "YEARS")),
            ("dt_bucket_12_hours", lambda r: r["Settlement Date Time"].time_bucket(12, "HOURS")),
            ("dt_bucket_30_minutes", lambda r: r["Settlement Date Time"].time_bucket(30, "MINUTES")),
            ("dt_bucket_90_seconds", lambda r: r["Settlement Date Time"].time_bucket(90, "SECONDS")),
            ("date_bucket_5_days", lambda r: r["Date"].time_bucket(5, "DAYS")),
            ("date_bucket_4_months", lambda r: r["Date"].time_bucket(4, "MONTHS")),
            ("date_bucket_3_years", lambda r: r["Date"].time_bucket(3, "YEARS")),
        ]).sort(lambda r: r["Settlement Date Time"].descending()).limit(1)

        res = frame.execute_frame_to_string()

        assert json.loads(res)["result"] == {
            'columns': [
                'Settlement Date Time',
                'Date',
                'dt_bucket_5_days',
                'dt_bucket_4_months',
                'dt_bucket_3_years',
                'dt_bucket_12_hours',
                'dt_bucket_30_minutes',
                'dt_bucket_90_seconds',
                'date_bucket_5_days',
                'date_bucket_4_months',
                'date_bucket_3_years',
            ],
            'rows': [{
                'values': [
                    '2014-12-05T21:00:00.000000000+0000',
                    '2014-12-04',
                    '2014-12-01T00:00:00.000000000+0000',
                    '2014-09-01T00:00:00.000000000+0000',
                    '2012-01-01T00:00:00.000000000+0000',
                    '2014-12-05T12:00:00.000000000+0000',
                    '2014-12-05T21:00:00.000000000+0000',
                    '2014-12-05T21:00:00.000000000+0000',
                    '2014-12-01',
                    '2014-09-01',
                    '2012-01-01',
                ]
            }]
        }

    def __generate_sql_string(self, f) -> str:  # type: ignore
        return self.db_extension.process_expression(
            f(self.tds_row).to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )

    def __generate_pure_string(self, f) -> str:  # type: ignore
        expr = str(f(self.tds_row).to_pure_expression(self.frame_to_pure_config))
        model_code = """
        function test::testFunc(): Any[*]
        {
            []->toOne()->cast(
                @meta::pure::metamodel::relation::Relation<(col1: Date[0..1], col2: Date[0..1])>
            )
            ->extend(~new_col:t|<<expression>>)
        }
        """
        self.__legend_client.parse_and_compile_model(model_code.replace("<<expression>>", expr))
        return expr
