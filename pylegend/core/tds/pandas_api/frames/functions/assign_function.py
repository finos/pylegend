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

from datetime import date, datetime
from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendDict,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.language import (
    LegacyApiTdsRow,
    PyLegendPrimitive,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendNumber,
    PyLegendBoolean,
    PyLegendString,
)


class AssignFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __col_definitions: PyLegendDict[
        str,
        PyLegendCallable[[LegacyApiTdsRow], PyLegendUnion[int, float, bool, str, date, datetime, PyLegendPrimitive]],
    ]

    @classmethod
    def name(cls) -> str:
        return "assign"

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            col_definitions: PyLegendDict[
                str,
                PyLegendCallable[[LegacyApiTdsRow], PyLegendUnion[int, float, bool, str, date, datetime, PyLegendPrimitive]],
            ]
    ) -> None:
        self.__base_frame = base_frame
        self.__col_definitions = col_definitions

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()
        base_query = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = (len(base_query.groupBy) > 0) or base_query.select.distinct

        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )

        tds_row = LegacyApiTdsRow.from_tds_frame("frame", self.__base_frame)
        for col, func in self.__col_definitions.items():
            res = func(tds_row)
            if not isinstance(res, PyLegendPrimitive):
                raise RuntimeError("Constants not supported")
            new_col_expr = res.to_sql_expression(
                {"frame": base_query},
                config
            )
            new_query.select.selectItems.append(
                SingleColumn(alias=db_extension.quote_identifier(col), expression=new_col_expr)
            )
        return new_query

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        new_cols = [c.copy() for c in self.__base_frame.columns()]
        tds_row = LegacyApiTdsRow.from_tds_frame("frame", self.__base_frame)
        for col, func in self.__col_definitions.items():
            res = func(tds_row)
            if isinstance(res, (int, PyLegendInteger)):
                new_cols.append(PrimitiveTdsColumn.integer_column(col))
            elif isinstance(res, (float, PyLegendFloat)):
                new_cols.append(PrimitiveTdsColumn.float_column(col))
            elif isinstance(res, PyLegendNumber):
                new_cols.append(PrimitiveTdsColumn.number_column(col))
            elif isinstance(res, (bool, PyLegendBoolean)):
                new_cols.append(PrimitiveTdsColumn.boolean_column(col))
            elif isinstance(res, (str, PyLegendString)):
                new_cols.append(PrimitiveTdsColumn.string_column(col))
            else:
                raise RuntimeError("Type not supported")
        return new_cols

    def validate(self) -> bool:
        tds_row = LegacyApiTdsRow.from_tds_frame("frame", self.__base_frame)
        for col, f in self.__col_definitions.items():
            f(tds_row)
        return True
