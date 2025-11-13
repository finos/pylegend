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


from datetime import date
from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendUnion,
)
from pylegend.core.sql.metamodel import LongLiteral, QuerySpecification
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.sql_query_helpers import create_sub_query, copy_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig


class TruncateFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __before: int
    __after: PyLegendUnion[int, None]
    __axis: PyLegendUnion[str, int]
    __copy: bool

    @classmethod
    def name(cls) -> str:
        return "truncate"  # pragma: no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            before: PyLegendUnion[date, str, int, None],
            after: PyLegendUnion[date, str, int, None],
            axis: PyLegendUnion[str, int],
            copy: bool,
    ) -> None:
        self.__base_frame = base_frame
        self.__before_input = before
        self.__after_input = after
        self.__axis = axis
        self.__copy = copy

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query: QuerySpecification = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = (base_query.offset is not None) or (base_query.limit is not None)
        new_query = create_sub_query(base_query, config, "root") if should_create_sub_query else copy_query(base_query)
        new_query.offset = LongLiteral(self.__before)
        if self.__after is not None:
            new_query.limit = LongLiteral(self.__after - self.__before + 1)
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        if self.__after is None:
            return f"{self.__base_frame.to_pure(config)}{config.separator(1)}" f"->drop({self.__before})"

        start_row = self.__before
        end_row = self.__after + 1
        return f"{self.__base_frame.to_pure(config)}{config.separator(1)}" f"->slice({start_row}, {end_row})"

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        if self.__axis not in [0, "index"]:
            raise NotImplementedError(
                f"The 'axis' parameter of the truncate function must be 0 or 'index', but got: {self.__axis}"
            )

        if self.__copy not in [True]:
            raise NotImplementedError(f"The 'copy' parameter of the truncate function must be True, but got: {self.__copy}")

        if self.__before_input is None:
            self.__before = 0
        else:
            self.__before = self.get_positive_integer_or_raise_exception(self.__before_input, variable_name="before")

        if self.__after_input is None:
            self.__after = None
            return True
        else:
            self.__after = self.get_positive_integer_or_raise_exception(self.__after_input, variable_name="after")

            if self.__before > self.__after:
                raise ValueError(
                    f"The 'before' parameter of the truncate function must be less than or equal to the 'after' parameter, "
                    f"but got: before={self.__before}, after={self.__after}"
                )

        return True

    def get_positive_integer_or_raise_exception(
        self, variable: PyLegendUnion[date, str, int, None], variable_name: str
    ) -> int:
        if type(variable) is not int:
            raise NotImplementedError(
                f"The '{variable_name}' parameter of the truncate function must be an integer, "
                f"but got: {variable} (type: {type(variable).__name__})"
            )

        if variable < 0:
            return 0
        return variable
