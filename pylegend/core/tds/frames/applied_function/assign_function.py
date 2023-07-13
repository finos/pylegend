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
import select

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
)
from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import (
    AppliedFunction,
    create_sub_query,
    copy_query
)
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    LogicalBinaryExpression, SingleColumn, Expression, AllColumns, SubqueryExpression
)
from pylegend.core.tds.pandas_api.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_base_tds_frame import LegendApiBaseTdsFrame

from typing import Callable, Union, Optional
from datetime import datetime, date


class AssignFunction(AppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    func: Expression
    column: Optional[str]

    @classmethod
    def name(cls) -> str:
        return "filter"

    def __init__(self, base_frame: PandasApiBaseTdsFrame, func: Expression, column: Optional[str]) -> None:
        self.__base_frame = base_frame
        self.func = func
        self.column = column

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        new_column = SingleColumn(self.column, self.func)
        if base_query.select:
            new_query = create_sub_query(base_query, config, "root")
            new_query.select.distinct = new_query.select.selectItems.append(new_column)  # type: ignore
            return new_query
        else:
            base_query.select = base_query.select.selectItems.append(new_column)  # type: ignore
            return base_query  # TODO: avoid parameter mutation

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:  # type: ignore
        pass