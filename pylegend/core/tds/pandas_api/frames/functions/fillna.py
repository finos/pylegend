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

frome datetime import date, datetime
from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
    PyLegendList,
    PyLegendUnion,
    PyLegendDict
)
from pylegend.core.language import (
    PyLegendColumnExpression,
    convert_literal_to_literal_expression
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    Expression,
    SingleColumn, FunctionCall, QualifiedName,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
    PandasApiAppliedFunction,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import (
    PandasApiBaseTdsFrame,
)
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig

__all__: PyLegendSequence[str] = ["PandasApiFillnaFunction"]


class PandasApiFillnaFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __value: PyLegendUnion[
        int, float, str, bool, date, datetime,
        PyLegendDict[str, PyLegendUnion[int, float, str, bool, date, datetime]]
    ]
    __axis: PyLegendOptional[PyLegendUnion[int, str]]
    __inplace: bool
    __limit: PyLegendOptional[int]

    @classmethod
    def name(cls) -> str:
        return "fillna"

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            value: PyLegendUnion[
                int, float, str, bool, date, datetime,
                PyLegendDict[str, PyLegendUnion[int, float, str, bool, date, datetime]]
            ],
            axis: PyLegendOptional[PyLegendUnion[int, str]],
            inplace: bool,
            limit: PyLegendOptional[int]
    ) -> None:
        self.__base_frame = base_frame
        self.__value = value
        self.__axis = axis
        self.__inplace = inplace
        self.__limit = limit

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        new_query = create_sub_query(base_query, config, "root")

        tds_row = PandasApiTdsRow.from_tds_frame("c", self.__base_frame)
        db_extension = config.sql_to_string_generator().get_db_extension()
        select_items = []

        for col in self.__base_frame.columns():
            col_name = col.get_name()
            fill_value = self.__value if not isinstance(self.__value, dict) else self.__value.get(col_name)
            col_expr = tds_row[col_name]
            col_sql_expr = col_expr.to_sql_expression({"c": new_query}, config)

            if fill_value is not None:
                fill_expr = convert_literal_to_literal_expression(fill_value)
                fill_sql_expr = fill_expr.to_sql_expression({"c": new_query}, config)
                sql_expr = FunctionCall(
                    name=QualifiedName(parts=['coalesce']),
                    distinct=False,
                    arguments=[col_sql_expr, fill_sql_expr],
                    filter_=None,
                    window=None
                )
            else:
                sql_expr = col_sql_expr

            select_items.append(SingleColumn(alias=db_extension.quote_identifier(col_name), expression=sql_expr))

        new_query.select.selectItems = select_items
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        base_pure = self.__base_frame.to_pure(config)
        projections = []

        for col in self.__base_frame.columns():
            col_name = col.get_name()
            fill_value = self.__value if not isinstance(self.__value, dict) else self.__value.get(col_name)

            if fill_value is not None:
                fill_expr = convert_literal_to_literal_expression(fill_value)
                fill_pure_expr = fill_expr.to_pure_expression(config)
                projections.append(f"'{col_name}':c|coalesce($c.{col_name}, {fill_pure_expr})")
            else:
                projections.append(f"'{col_name}':c|$c.{col_name}")

        projection_string = ", ".join(projections)
        return f"{base_pure}{config.separator(1)}->project(~[{projection_string}])"

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        if isinstance(self.__value, list):
            raise TypeError("Unsupported 'value' type: <class 'list'>")

        if self.__axis not in (0, 1, "index", "columns"):
            raise ValueError(f"No axis named {self.__axis} for object type TdsFrame")
        if self.__axis in (1, "columns"):
            raise NotImplementedError("axis=1 is not supported yet in Pandas API fillna")

        if self.__inplace:
            raise NotImplementedError("inplace=True is not supported yet in Pandas API fillna")

        if self.__limit is not None:
            raise NotImplementedError("limit parameter is not supported yet in Pandas API fillna")

        return True
