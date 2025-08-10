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
    PyLegendList,
    PyLegendSequence,
    PyLegendCallable,
    PyLegendUnion,
    PyLegendTuple,
)
from pylegend.core.language import PyLegendColumnExpression
from pylegend.core.language.legendql_api.legendql_api_custom_expressions import LegendQLApiPrimitive
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import LegendQLApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
    SelectItem,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame
from pylegend.core.language.shared.helpers import escape_column_name


__all__: PyLegendSequence[str] = [
    "LegendQLApiRenameFunction"
]


class LegendQLApiRenameFunction(LegendQLApiAppliedFunction):
    __base_frame: LegendQLApiBaseTdsFrame
    __column_names: PyLegendList[str]
    __renamed_column_names: PyLegendList[str]

    @classmethod
    def name(cls) -> str:
        return "renameColumns"

    def __init__(
            self,
            base_frame: LegendQLApiBaseTdsFrame,
            column_renames: PyLegendUnion[
                PyLegendTuple[str, str],
                PyLegendList[PyLegendTuple[str, str]],
                PyLegendCallable[
                    [LegendQLApiTdsRow],
                    PyLegendUnion[
                        PyLegendTuple[LegendQLApiPrimitive, str],
                        PyLegendList[PyLegendTuple[LegendQLApiPrimitive, str]]
                    ]
                ]
            ]
    ) -> None:
        self.__base_frame = base_frame

        col_names: PyLegendList[str] = []
        renamed_col_names: PyLegendList[str] = []

        def rename_tuple_check(t):  # type: ignore
            return isinstance(t, tuple) and (len(t) == 2) and isinstance(t[0], str) and isinstance(t[1], str)

        if rename_tuple_check(column_renames):  # type: ignore
            col_names.append(column_renames[0])  # type: ignore
            renamed_col_names.append(column_renames[1])  # type: ignore

        elif isinstance(column_renames, list) and all([rename_tuple_check(r) for r in column_renames]):  # type: ignore
            for t in column_renames:
                col_names.append(t[0])
                renamed_col_names.append(t[1])

        elif isinstance(column_renames, type(lambda x: 0)) and (column_renames.__code__.co_argcount == 1):
            tds_row = LegendQLApiTdsRow.from_tds_frame("frame", self.__base_frame)
            try:
                result = column_renames(tds_row)
            except Exception as e:
                raise RuntimeError(
                    "rename' function column_renames argument lambda incompatible. "
                    "Error occurred while evaluating. Message: " + str(e)
                ) from e

            list_result = result if isinstance(result, list) else [result]
            for (i, r) in enumerate(list_result):
                if (isinstance(r, tuple) and (len(r) == 2) and
                        isinstance(r[0], LegendQLApiPrimitive) and isinstance(r[0].value(), PyLegendColumnExpression)
                        and isinstance(r[1], str)):
                    col_expr: PyLegendColumnExpression = r[0].value()
                    col_names.append(col_expr.get_column())
                    renamed_col_names.append(r[1])
                else:
                    raise TypeError(
                        "'rename' function column_renames argument lambda incompatible. Each element in rename list "
                        "should be a tuple with first element being a simple column expression and "
                        "second element being a string (renamed column name) "
                        f"(E.g - lambda r: [(r.c1, 'c2')]). Element at index {i} (0-indexed) is incompatible"
                    )

        else:
            raise TypeError("'rename' function column_renames argument can either be a list of renaming tuples or "
                            "a lambda function which takes one argument (LegendQLApiTdsRow)")

        self.__column_names = col_names
        self.__renamed_column_names = renamed_col_names

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

    def base_frame(self) -> LegendQLApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendQLApiBaseTdsFrame"]:
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
