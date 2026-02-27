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
)
from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import LegacyApiAppliedFunction
from pylegend.core.tds.legacy_api.frames.legacy_api_base_tds_frame import LegacyApiBaseTdsFrame
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveType
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.cast_helpers import CastTarget, validate_and_build_cast_columns, pure_type_spec, _normalize_target
from pylegend.core.language.shared.helpers import escape_column_name


__all__: PyLegendSequence[str] = [
    "LegacyApiCastFunction"
]


class LegacyApiCastFunction(LegacyApiAppliedFunction):
    __base_frame: LegacyApiBaseTdsFrame
    __column_type_map: PyLegendDict[str, CastTarget]

    @classmethod
    def name(cls) -> str:
        return "cast"

    def __init__(
            self,
            base_frame: LegacyApiBaseTdsFrame,
            column_type_map: PyLegendDict[str, CastTarget]
    ) -> None:
        self.__base_frame = base_frame
        self.__column_type_map = column_type_map

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        return self.__base_frame.to_sql_query_object(config)

    def to_pure(self, config: FrameToPureConfig) -> str:
        base_pure = self.__base_frame.to_pure(config)
        new_columns = validate_and_build_cast_columns(
            self.__base_frame.columns(), self.__column_type_map
        )
        col_specs_parts: PyLegendList[str] = []
        for c in new_columns:
            name = escape_column_name(c.get_name())
            if c.get_name() in self.__column_type_map:
                ptype, params = _normalize_target(self.__column_type_map[c.get_name()])
                col_specs_parts.append(f"{name}:{pure_type_spec(ptype, params)}")
            else:
                col_specs_parts.append(f"{name}:{c.get_type()}")
        col_specs = ", ".join(col_specs_parts)
        return (
            f"{base_pure}{config.separator(1)}"
            f"->cast(@meta::pure::metamodel::relation::Relation<({col_specs})>)"
        )

    def base_frame(self) -> LegacyApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegacyApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return validate_and_build_cast_columns(
            self.__base_frame.columns(), self.__column_type_map
        )

    def validate(self) -> bool:
        validate_and_build_cast_columns(
            self.__base_frame.columns(), self.__column_type_map
        )
        return True

