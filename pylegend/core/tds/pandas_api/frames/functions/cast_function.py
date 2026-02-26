# Copyright 2026 Goldman Sachs
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
    PyLegendDict,
    PyLegendList,
    PyLegendSequence,
)
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SelectItem,
    SingleColumn,
    Cast,
    ColumnType,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.sql_query_helpers import copy_query
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveType
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.cast_helpers import (
    validate_and_build_cast_columns,
    PRIMITIVE_TYPE_TO_SQL_TYPE,
)


__all__: PyLegendSequence[str] = [
    "PandasApiCastFunction"
]


class PandasApiCastFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __column_type_map: PyLegendDict[str, PrimitiveType]

    @classmethod
    def name(cls) -> str:
        return "cast"

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            column_type_map: PyLegendDict[str, PrimitiveType]
    ) -> None:
        self.__base_frame = base_frame
        self.__column_type_map = column_type_map

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()

        quoted_map = {
            db_extension.quote_identifier(col_name): target_type
            for col_name, target_type in self.__column_type_map.items()
        }

        new_select_items: PyLegendList[SelectItem] = []
        for col in base_query.select.selectItems:
            if not isinstance(col, SingleColumn):
                raise ValueError(
                    "Cast operation not supported for queries with columns other than SingleColumn"
                )  # pragma: no cover
            if col.alias is None:
                raise ValueError(
                    "Cast operation not supported for queries with SingleColumns with missing alias"
                )  # pragma: no cover
            if col.alias in quoted_map:
                target_type = quoted_map[col.alias]
                sql_type_name = PRIMITIVE_TYPE_TO_SQL_TYPE[target_type]
                new_select_items.append(
                    SingleColumn(
                        alias=col.alias,
                        expression=Cast(
                            expression=col.expression,
                            type_=ColumnType(name=sql_type_name, parameters=[])
                        )
                    )
                )
            else:
                new_select_items.append(col)

        new_query = copy_query(base_query)
        new_query.select.selectItems = new_select_items
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        base_pure = self.__base_frame.to_pure(config)
        cast_parts: PyLegendList[str] = []
        for col_name, target_type in self.__column_type_map.items():
            cast_parts.append(f"->cast(~{col_name}, {target_type.name})")
        return f"{base_pure}{config.separator(1)}" + f"{config.separator(1)}".join(cast_parts)

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return validate_and_build_cast_columns(
            self.__base_frame.columns(), self.__column_type_map
        )

    def validate(self) -> bool:
        # Validation is done in calculate_columns via validate_and_build_cast_columns
        validate_and_build_cast_columns(
            self.__base_frame.columns(), self.__column_type_map
        )
        return True

