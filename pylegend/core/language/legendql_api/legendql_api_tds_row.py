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

from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
)
from pylegend.core.language import (
    PyLegendBoolean,
    PyLegendString,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendNumber,
    PyLegendStrictDate,
    PyLegendDateTime,
    PyLegendDate,
)
from pylegend.core.language.legendql_api.legendql_api_custom_expressions import (
    LegendQLApiBoolean,
    LegendQLApiString,
    LegendQLApiInteger,
    LegendQLApiFloat,
    LegendQLApiNumber,
    LegendQLApiStrictDate,
    LegendQLApiDateTime,
    LegendQLApiDate,
    LegendQLApiPrimitive,
    LegendQLApiPartialFrame,
    LegendQLApiWindowReference,
)
from pylegend.core.language.shared.tds_row import AbstractTdsRow
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    Expression,
    FunctionCall,
    QualifiedName,
    IntegerLiteral
)
from pylegend.core.tds.tds_frame import PyLegendTdsFrame, FrameToPureConfig, FrameToSqlConfig

__all__: PyLegendSequence[str] = [
    "LegendQLApiTdsRow",
    "LegendQLApiLeadRow",
    "LegendQLApiLagRow",
    "LegendQLApiFirstRow",
    "LegendQLApiLastRow",
    "LegendQLApiNthRow",
]


class LegendQLApiTdsRow(AbstractTdsRow):

    def __init__(self, frame_name: str, frame: PyLegendTdsFrame) -> None:
        super().__init__(frame_name, frame)

    @staticmethod
    def from_tds_frame(frame_name: str, frame: PyLegendTdsFrame) -> "LegendQLApiTdsRow":
        return LegendQLApiTdsRow(frame_name=frame_name, frame=frame)

    def __getattr__(self, key: str) -> LegendQLApiPrimitive:
        return self[key]

    def get_boolean(self, column: str) -> LegendQLApiBoolean:
        return LegendQLApiBoolean(super().get_boolean(column))

    def get_string(self, column: str) -> LegendQLApiString:
        return LegendQLApiString(super().get_string(column))

    def get_number(self, column: str) -> LegendQLApiNumber:
        return LegendQLApiNumber(super().get_number(column))

    def get_integer(self, column: str) -> LegendQLApiInteger:
        return LegendQLApiInteger(super().get_integer(column))

    def get_float(self, column: str) -> LegendQLApiFloat:
        return LegendQLApiFloat(super().get_float(column))

    def get_date(self, column: str) -> LegendQLApiDate:
        return LegendQLApiDate(super().get_date(column))

    def get_datetime(self, column: str) -> LegendQLApiDateTime:
        return LegendQLApiDateTime(super().get_datetime(column))

    def get_strictdate(self, column: str) -> LegendQLApiStrictDate:
        return LegendQLApiStrictDate(super().get_strictdate(column))

    def __getitem__(self, item: str) -> LegendQLApiPrimitive:
        res = super().__getitem__(item)
        if isinstance(res, PyLegendBoolean):
            return LegendQLApiBoolean(res)
        if isinstance(res, PyLegendString):
            return LegendQLApiString(res)
        if isinstance(res, PyLegendInteger):
            return LegendQLApiInteger(res)
        if isinstance(res, PyLegendFloat):
            return LegendQLApiFloat(res)
        if isinstance(res, PyLegendNumber):
            return LegendQLApiNumber(res)
        if isinstance(res, PyLegendStrictDate):
            return LegendQLApiStrictDate(res)
        if isinstance(res, PyLegendDateTime):
            return LegendQLApiDateTime(res)
        if isinstance(res, PyLegendDate):
            return LegendQLApiDate(res)

        raise RuntimeError(f"Unhandled primitive type {type(res)} in LegendQL Api")


class LegendQLApiLeadRow(LegendQLApiTdsRow):
    __partial_frame: LegendQLApiPartialFrame
    __row: "LegendQLApiTdsRow"

    def __init__(
            self,
            partial_frame: LegendQLApiPartialFrame,
            row: "LegendQLApiTdsRow"
    ) -> None:
        super().__init__(frame_name=row.get_frame_name(), frame=partial_frame.get_base_frame())
        self.__partial_frame = partial_frame
        self.__row = row

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"{self.__partial_frame.to_pure_expression(config)}->lead({self.__row.to_pure_expression(config)})"

    def column_sql_expression(
            self,
            column: str,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["lead"]),
            distinct=False,
            arguments=[super().column_sql_expression(column, frame_name_to_base_query_map, config)],
            filter_=None,
            window=None
        )


class LegendQLApiLagRow(LegendQLApiTdsRow):
    __partial_frame: LegendQLApiPartialFrame
    __row: "LegendQLApiTdsRow"

    def __init__(
            self,
            partial_frame: LegendQLApiPartialFrame,
            row: "LegendQLApiTdsRow"
    ) -> None:
        super().__init__(frame_name=row.get_frame_name(), frame=partial_frame.get_base_frame())
        self.__partial_frame = partial_frame
        self.__row = row

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"{self.__partial_frame.to_pure_expression(config)}->lag({self.__row.to_pure_expression(config)})"

    def column_sql_expression(
            self,
            column: str,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["lag"]),
            distinct=False,
            arguments=[super().column_sql_expression(column, frame_name_to_base_query_map, config)],
            filter_=None,
            window=None
        )


class LegendQLApiFirstRow(LegendQLApiTdsRow):
    __partial_frame: LegendQLApiPartialFrame
    __window_ref: LegendQLApiWindowReference
    __row: "LegendQLApiTdsRow"

    def __init__(
            self,
            partial_frame: LegendQLApiPartialFrame,
            window_ref: LegendQLApiWindowReference,
            row: "LegendQLApiTdsRow"
    ) -> None:
        super().__init__(frame_name=row.get_frame_name(), frame=partial_frame.get_base_frame())
        self.__partial_frame = partial_frame
        self.__window_ref = window_ref
        self.__row = row

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return (f"{self.__partial_frame.to_pure_expression(config)}->first("
                f"{self.__window_ref.to_pure_expression(config)}, {self.__row.to_pure_expression(config)})")

    def column_sql_expression(
            self,
            column: str,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["first_value"]),
            distinct=False,
            arguments=[super().column_sql_expression(column, frame_name_to_base_query_map, config)],
            filter_=None,
            window=None
        )


class LegendQLApiLastRow(LegendQLApiTdsRow):
    __partial_frame: LegendQLApiPartialFrame
    __window_ref: LegendQLApiWindowReference
    __row: "LegendQLApiTdsRow"

    def __init__(
            self,
            partial_frame: LegendQLApiPartialFrame,
            window_ref: LegendQLApiWindowReference,
            row: "LegendQLApiTdsRow"
    ) -> None:
        super().__init__(frame_name=row.get_frame_name(), frame=partial_frame.get_base_frame())
        self.__partial_frame = partial_frame
        self.__window_ref = window_ref
        self.__row = row

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return (f"{self.__partial_frame.to_pure_expression(config)}->last("
                f"{self.__window_ref.to_pure_expression(config)}, {self.__row.to_pure_expression(config)})")

    def column_sql_expression(
            self,
            column: str,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["last_value"]),
            distinct=False,
            arguments=[super().column_sql_expression(column, frame_name_to_base_query_map, config)],
            filter_=None,
            window=None
        )


class LegendQLApiNthRow(LegendQLApiTdsRow):
    __partial_frame: LegendQLApiPartialFrame
    __window_ref: LegendQLApiWindowReference
    __row: "LegendQLApiTdsRow"
    __offset: int

    def __init__(
            self,
            partial_frame: LegendQLApiPartialFrame,
            window_ref: LegendQLApiWindowReference,
            row: "LegendQLApiTdsRow",
            offset: int
    ) -> None:
        super().__init__(frame_name=row.get_frame_name(), frame=partial_frame.get_base_frame())
        self.__partial_frame = partial_frame
        self.__window_ref = window_ref
        self.__row = row
        self.__offset = offset

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return (
            f"{self.__partial_frame.to_pure_expression(config)}->nth("
            f"{self.__window_ref.to_pure_expression(config)}, {self.__row.to_pure_expression(config)}, {self.__offset})"
        )

    def column_sql_expression(
            self,
            column: str,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["nth_value"]),
            distinct=False,
            arguments=[
                super().column_sql_expression(column, frame_name_to_base_query_map, config),
                IntegerLiteral(self.__offset)
            ],
            filter_=None,
            window=None
        )
