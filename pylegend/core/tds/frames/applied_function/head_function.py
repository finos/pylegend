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
)
from pylegend.core.tds.frames.applied_function.applied_function_tds_frame import (
    AppliedFunction,
    create_sub_query,
    copy_query
)
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    LongLiteral,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.frames.base_tds_frame import BaseTdsFrame


class HeadFunction(AppliedFunction):
    row_count: int

    @classmethod
    def name(cls) -> str:
        return "head"

    def __init__(self, row_count: int) -> None:
        self.row_count = row_count

    def to_sql(self, base_query: QuerySpecification, config: FrameToSqlConfig) -> QuerySpecification:
        if base_query.limit:
            new_query = create_sub_query(base_query, config, "root")
            new_query.limit = LongLiteral(value=self.row_count)
            return new_query
        else:
            new_query = copy_query(base_query)
            new_query.limit = LongLiteral(value=self.row_count)
            return new_query

    def tds_frame_parameters(self) -> PyLegendList["BaseTdsFrame"]:
        return []
