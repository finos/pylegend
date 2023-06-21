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
    PyLegendTuple
)
from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import (
    AppliedFunction,
    create_sub_query,
    copy_query
)
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
    SelectItem
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_base_tds_frame import LegendApiBaseTdsFrame


__all__: PyLegendSequence[str] = [
    "RestrictFunction"
]


class RestrictFunction(AppliedFunction):
    base_frame: LegendApiBaseTdsFrame
    column_name_list: PyLegendList[str]

    @classmethod
    def name(cls) -> str:
        return "restrict"

    def __init__(self, base_frame: LegendApiBaseTdsFrame, column_name_list: PyLegendList[str]) -> None:
        self.base_frame = base_frame
        self.column_name_list = column_name_list

    def to_sql(self, base_query: QuerySpecification, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()
        columns_to_retain = [db_extension.quote_identifier(x) for x in self.column_name_list]

        sub_query_required = (len(base_query.groupBy) > 0) or (len(base_query.orderBy) > 0) or \
                             (base_query.having is not None) or base_query.select.distinct

        if sub_query_required:
            new_query = create_sub_query(base_query, config, "root", columns_to_retain=columns_to_retain)
            return new_query
        else:
            new_cols_with_index: PyLegendList[PyLegendTuple[int, 'SelectItem']] = []
            for col in base_query.select.selectItems:
                if not isinstance(col, SingleColumn):
                    raise ValueError("Restrict operation not supported for queries "
                                     "with columns other than SingleColumn")  # pragma: no cover
                if col.alias is None:
                    raise ValueError("Restrict operation not supported for queries "
                                     "with SingleColumns with missing alias")  # pragma: no cover
                if col.alias in columns_to_retain:
                    new_cols_with_index.append((columns_to_retain.index(col.alias), col))

            new_select_items = [y[1] for y in sorted(new_cols_with_index, key=lambda x: x[0])]
            new_query = copy_query(base_query)
            new_query.select.selectItems = new_select_items
            return new_query

    def tds_frame_parameters(self) -> PyLegendList["LegendApiBaseTdsFrame"]:
        return []

    def calculate_columns(self, base_frame: "LegendApiBaseTdsFrame") -> PyLegendSequence["TdsColumn"]:
        base_columns = base_frame.columns()
        new_columns = []
        for c in self.column_name_list:
            found_col = False
            for base_col in base_columns:
                if base_col.get_name() == c:
                    new_columns.append(base_col.copy())
                    found_col = True
                    break
            if not found_col:
                raise ValueError(
                    "Column - '{col}' in restrict columns list doesn't exist in the current frame. "
                    "Current frame columns: {cols}".format(
                        col=c,
                        cols=[x.get_name() for x in base_columns]
                    )
                )
        return new_columns
