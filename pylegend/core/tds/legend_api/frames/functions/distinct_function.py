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
    PyLegendSequence
)
from pylegend.core.tds.legend_api.frames.legend_api_applied_function_tds_frame import (
    AppliedFunction,
    create_sub_query,
    copy_query
)
from pylegend.core.sql.metamodel import (
    QuerySpecification,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_base_tds_frame import LegendApiBaseTdsFrame


__all__: PyLegendSequence[str] = [
    "DistinctFunction"
]


class DistinctFunction(AppliedFunction):

    @classmethod
    def name(cls) -> str:
        return "distinct"

    def to_sql(self, base_query: QuerySpecification, config: FrameToSqlConfig) -> QuerySpecification:
        if (base_query.offset is not None) or (base_query.limit is not None):
            new_query = create_sub_query(base_query, config, "root")
            new_query.select.distinct = True
            return new_query
        else:
            new_query = copy_query(base_query)
            new_query.select.distinct = True
            return new_query

    def tds_frame_parameters(self) -> PyLegendList["LegendApiBaseTdsFrame"]:
        return []

    def calculate_columns(self, base_frame: "LegendApiBaseTdsFrame") -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in base_frame.columns()]
