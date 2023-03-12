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

from abc import ABCMeta, abstractmethod
from pylegend._typing import (
    PyLegendSequence
)
from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.databse.sql_to_string import (
    SqlToStringGenerator,
    SqlToStringConfig,
    SqlToStringFormat
)
from pylegend.core.tds.tds_frame import (
    PyLegendTdsFrame,
    FrameToSqlConfig
)


__all__: PyLegendSequence[str] = [
    "BaseTdsFrame"
]


class BaseTdsFrame(PyLegendTdsFrame, metaclass=ABCMeta):
    def head(self, row_count: int = 5) -> "PyLegendTdsFrame":
        from pylegend.core.tds.frames.applied_function_tds_frame import (
            AppliedFunctionTdsFrame,
            TakeFunction
        )
        return AppliedFunctionTdsFrame(self, TakeFunction(row_count))

    @abstractmethod
    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        pass

    def to_sql_query(self, config: FrameToSqlConfig = FrameToSqlConfig()) -> str:
        query = self.to_sql_query_object(config)
        sql_to_string_generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type(config.database_type)
        sql_to_string_config = SqlToStringConfig(
            format_=SqlToStringFormat(
                pretty=config.pretty
            )
        )
        return sql_to_string_generator.generate_sql_string(query, sql_to_string_config)
