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
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiBoolean,
    PandasApiPartialFrame,
    PandasApiString,
    PandasApiInteger,
    PandasApiFloat,
    PandasApiNumber,
    PandasApiStrictDate,
    PandasApiDateTime,
    PandasApiDate,
    PandasApiPrimitive,
)
from pylegend.core.language.shared.tds_row import AbstractTdsRow
from pylegend.core.sql.metamodel import Expression, FunctionCall, IntegerLiteral, QualifiedName, QuerySpecification
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig, PyLegendTdsFrame

__all__: PyLegendSequence[str] = [
    "PandasApiTdsRow",
    "PandasApiLagRow",
    "PandasApiLeadRow",
]


class PandasApiTdsRow(AbstractTdsRow):
    def __init__(self, frame_name: str, frame: PyLegendTdsFrame) -> None:
        super().__init__(frame_name, frame)

    @staticmethod
    def from_tds_frame(frame_name: str, frame: PyLegendTdsFrame) -> "PandasApiTdsRow":
        return PandasApiTdsRow(frame_name=frame_name, frame=frame)

    def __getitem__(self, item: str) -> PandasApiPrimitive:
        res = super().__getitem__(item)
        if isinstance(res, PyLegendBoolean):
            return PandasApiBoolean(res)
        if isinstance(res, PyLegendString):
            return PandasApiString(res)
        if isinstance(res, PyLegendInteger):
            return PandasApiInteger(res)
        if isinstance(res, PyLegendFloat):
            return PandasApiFloat(res)
        if isinstance(res, PyLegendNumber):
            return PandasApiNumber(res)
        if isinstance(res, PyLegendStrictDate):
            return PandasApiStrictDate(res)
        if isinstance(res, PyLegendDateTime):
            return PandasApiDateTime(res)
        if isinstance(res, PyLegendDate):
            return PandasApiDate(res)

        raise RuntimeError(f"Unhandled primitive type {type(res)} in Pandas Api")  # pragma: no cover


class PandasApiLeadRow(PandasApiTdsRow):
    __partial_frame: PandasApiPartialFrame
    __row: "PandasApiTdsRow"
    __num_rows_to_lead_by: int

    def __init__(
            self,
            partial_frame: PandasApiPartialFrame,
            row: "PandasApiTdsRow",
            num_rows_to_lead_by: int = 1
    ) -> None:
        super().__init__(frame_name=row.get_frame_name(), frame=partial_frame.get_base_frame())
        self.__partial_frame = partial_frame
        self.__row = row
        self.__num_rows_to_lead_by = num_rows_to_lead_by

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return (
            f"{self.__partial_frame.to_pure_expression(config)}"
            f"->lead({self.__row.to_pure_expression(config)}, {self.__num_rows_to_lead_by})"
        )

    def column_sql_expression(
            self,
            column: str,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:  # pragma: no cover (SQL query execution is not supported for the shift function)
        arguments: list[Expression] = [
            super().column_sql_expression(column, frame_name_to_base_query_map, config),
            IntegerLiteral(self.__num_rows_to_lead_by)
        ]

        return FunctionCall(
            name=QualifiedName(parts=["lead"]),
            distinct=False,
            arguments=arguments,
            filter_=None,
            window=None
        )


class PandasApiLagRow(PandasApiTdsRow):
    __partial_frame: PandasApiPartialFrame
    __row: "PandasApiTdsRow"
    __num_rows_to_lag_by: int

    def __init__(
            self,
            partial_frame: PandasApiPartialFrame,
            row: "PandasApiTdsRow",
            num_rows_to_lag_by: int = 1
    ) -> None:
        super().__init__(frame_name=row.get_frame_name(), frame=partial_frame.get_base_frame())
        self.__partial_frame = partial_frame
        self.__row = row
        self.__num_rows_to_lag_by = num_rows_to_lag_by

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return (
            f"{self.__partial_frame.to_pure_expression(config)}"
            f"->lag({self.__row.to_pure_expression(config)}, {self.__num_rows_to_lag_by})"
        )

    def column_sql_expression(
            self,
            column: str,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:  # pragma: no cover (SQL query execution is not supported for the shift function)
        arguments: list[Expression] = [
            super().column_sql_expression(column, frame_name_to_base_query_map, config),
            IntegerLiteral(self.__num_rows_to_lag_by)
        ]

        return FunctionCall(
            name=QualifiedName(parts=["lag"]),
            distinct=False,
            arguments=arguments,
            filter_=None,
            window=None
        )
