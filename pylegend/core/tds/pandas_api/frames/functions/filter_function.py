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
import abc
from abc import ABC

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
    LogicalBinaryExpression, Expression
)
from pylegend.core.tds.pandas_api.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.tds_api import PyLegendAbstract
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_base_tds_frame import LegendApiBaseTdsFrame

from typing import Callable, Optional, Union


class FilterFunction(AppliedFunction):
    func: Expression

    @classmethod
    def name(cls) -> str:
        return "filter"

    def __init__(self, base_frame: PandasApiBaseTdsFrame, func: Expression):
        self.__base_frame = base_frame
        self.func = func

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        if base_query.where:
            new_query = create_sub_query(base_query, config, "root")
            new_query.where = self.func
            return new_query
        else:
            base_query.where = self.func
            return base_query  # TODO: avoid parameter mutation

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:  # type: ignore
        pass