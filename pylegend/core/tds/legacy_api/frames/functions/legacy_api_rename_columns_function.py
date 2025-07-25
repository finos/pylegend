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

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
)
from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import LegacyApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
    SelectItem,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legacy_api.frames.legacy_api_base_tds_frame import LegacyApiBaseTdsFrame
from pylegend.core.language.shared.helpers import escape_column_name


__all__: PyLegendSequence[str] = [
    "LegacyApiRenameColumnsFunction"
]


class LegacyApiRenameColumnsFunction(LegacyApiAppliedFunction):
    __base_frame: LegacyApiBaseTdsFrame
    __column_names: PyLegendList[str]
    __renamed_column_names: PyLegendList[str]

    @classmethod
    def name(cls) -> str:
        return "renameColumns"

    def __init__(
            self,
            base_frame: LegacyApiBaseTdsFrame,
            column_names: PyLegendList[str],
            renamed_column_names: PyLegendList[str]
    ) -> None:
        self.__base_frame = base_frame
        self.__column_names = column_names
        self.__renamed_column_names = renamed_column_names

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)

        db_extension = config.sql_to_string_generator().get_db_extension()
        quoted_column_names_to_change = [db_extension.quote_identifier(s) for s in self.__column_names]
        quoted_renamed_column_names = [db_extension.quote_identifier(s) for s in self.__renamed_column_names]

        new_select_items: PyLegendList[SelectItem] = []
        for col in base_query.select.selectItems:
            if not isinstance(col, SingleColumn):
                raise ValueError("Rename columns operation not supported for queries "
                                 "with columns other than SingleColumn")  # pragma: no cover
            if col.alias is None:
                raise ValueError("Rename columns operation not supported for queries "
                                 "with SingleColumns with missing alias")  # pragma: no cover
            if col.alias in quoted_column_names_to_change:
                new_alias = quoted_renamed_column_names[quoted_column_names_to_change.index(col.alias)]
                new_select_items.append(SingleColumn(alias=new_alias, expression=col.expression))
            else:
                new_select_items.append(col)

        new_query = copy_query(base_query)
        new_query.select.selectItems = new_select_items
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}" +
                f"{config.separator(1)}".join([
                    f"->rename(~{x}, ~{y})"
                    for x, y in zip(
                        map(escape_column_name, self.__column_names),
                        map(escape_column_name, self.__renamed_column_names)
                    )
                ]))

    def base_frame(self) -> LegacyApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegacyApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        new_columns = []
        for base_col in self.__base_frame.columns():
            if base_col.get_name() in self.__column_names:
                renamed_column_name = self.__renamed_column_names[self.__column_names.index(base_col.get_name())]
                new_columns.append(base_col.copy_with_changed_name(renamed_column_name))
            else:
                new_columns.append(base_col.copy())
        return new_columns

    def validate(self) -> bool:
        if len(self.__column_names) != len(self.__renamed_column_names):
            raise ValueError(
                "column_names list and renamed_column_names list should have same size when renaming columns.\n"
                f"column_names list - (Count: {len(self.__column_names)}) - {self.__column_names}\n"
                f"renamed_column_names_list - (Count: {len(self.__renamed_column_names)}) - {self.__renamed_column_names}\n"
            )

        if len(self.__column_names) != len(set(self.__column_names)):
            raise ValueError(
                "column_names list shouldn't have duplicates when renaming columns.\n"
                f"column_names list - (Count: {len(self.__column_names)}) - {self.__column_names}\n"
            )

        if len(self.__renamed_column_names) != len(set(self.__renamed_column_names)):
            raise ValueError(
                "renamed_column_names_list list shouldn't have duplicates when renaming columns.\n"
                f"renamed_column_names_list - (Count: {len(self.__renamed_column_names)}) - {self.__renamed_column_names}\n"
            )

        return True
