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
    PyLegendTuple,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.language import PyLegendPrimitive, PyLegendColumnExpression
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.tds.legendql_api.frames.legendql_api_applied_function_tds_frame import LegendQLApiAppliedFunction
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
    SelectItem
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame
from pylegend.core.language.shared.helpers import escape_column_name


__all__: PyLegendSequence[str] = [
    "LegendQLApiSelectFunction"
]


class LegendQLApiSelectFunction(LegendQLApiAppliedFunction):
    __base_frame: LegendQLApiBaseTdsFrame
    __column_name_list: PyLegendList[str]

    @classmethod
    def name(cls) -> str:
        return "select"

    def __init__(
            self,
            base_frame: LegendQLApiBaseTdsFrame,
            columns_function: PyLegendCallable[[LegendQLApiTdsRow], PyLegendUnion[
                PyLegendPrimitive,
                PyLegendList[PyLegendPrimitive],
                str,
                PyLegendList[str]
            ]]
    ) -> None:
        self.__base_frame = base_frame
        tds_row = LegendQLApiTdsRow.from_tds_frame("frame", self.__base_frame)
        if not isinstance(columns_function, type(lambda x: 0)) or (columns_function.__code__.co_argcount != 1):
            raise TypeError("Select columns_function should be a lambda which takes one argument (TDSRow)")

        try:
            result = columns_function(tds_row)
        except Exception as e:
            raise RuntimeError(
                "Select lambda incompatible. Error occurred while evaluating. Message: " + str(e)
            ) from e

        list_result: PyLegendUnion[PyLegendList[PyLegendPrimitive], PyLegendList[str]]
        if isinstance(result, list):
            list_result = result
        elif isinstance(result, tuple):
            list_result = list(result)
        else:
            list_result = [result]

        columns_list = []
        for (i, r) in enumerate(list_result):
            if isinstance(r, str):
                columns_list.append(r)
            elif isinstance(r, PyLegendPrimitive) and isinstance(r.value(), PyLegendColumnExpression):
                col_expr: PyLegendColumnExpression = r.value()
                columns_list.append(col_expr.get_column())
            else:
                raise RuntimeError(
                    "Select lambda incompatible. Columns can either be strings (lambda r: ['c1', 'c2']) or "
                    "simple column expressions (lambda r: [r.c1, r.c2]). "
                    f"Element at index {i} in the list is incompatible."
                )

        self.__column_name_list = columns_list

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()
        columns_to_retain = [db_extension.quote_identifier(x) for x in self.__column_name_list]

        sub_query_required = (len(base_query.groupBy) > 0) or (len(base_query.orderBy) > 0) or \
                             (base_query.having is not None) or base_query.select.distinct

        if sub_query_required:
            new_query = create_sub_query(base_query, config, "root", columns_to_retain=columns_to_retain)
            return new_query
        else:
            new_cols_with_index: PyLegendList[PyLegendTuple[int, 'SelectItem']] = []
            for col in base_query.select.selectItems:
                if not isinstance(col, SingleColumn):
                    raise ValueError("Select operation not supported for queries "
                                     "with columns other than SingleColumn")  # pragma: no cover
                if col.alias is None:
                    raise ValueError("Select operation not supported for queries "
                                     "with SingleColumns with missing alias")  # pragma: no cover
                if col.alias in columns_to_retain:
                    new_cols_with_index.append((columns_to_retain.index(col.alias), col))

            new_select_items = [y[1] for y in sorted(new_cols_with_index, key=lambda x: x[0])]
            new_query = copy_query(base_query)
            new_query.select.selectItems = new_select_items
            return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        escaped_columns = []
        for col_name in self.__column_name_list:
            escaped_columns.append(escape_column_name(col_name))
        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}" +
                f"->select(~[{', '.join(escaped_columns)}])")

    def base_frame(self) -> LegendQLApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegendQLApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        base_columns = self.__base_frame.columns()
        new_columns = []
        for c in self.__column_name_list:
            for base_col in base_columns:
                if base_col.get_name() == c:
                    new_columns.append(base_col.copy())
                    break
        return new_columns

    def validate(self) -> bool:
        base_columns = self.__base_frame.columns()
        for c in self.__column_name_list:
            found_col = False
            for base_col in base_columns:
                if base_col.get_name() == c:
                    found_col = True
                    break
            if not found_col:
                raise ValueError(
                    f"Column - '{c}' in select columns list doesn't exist in the current frame. "
                    f"Current frame columns: {[x.get_name() for x in base_columns]}"
                )
        return True
