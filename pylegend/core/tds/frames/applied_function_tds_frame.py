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
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    LongLiteral
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.frames.base_tds_frame import BaseTdsFrame


__all__: PyLegendSequence[str] = [
    "AppliedFunctionTdsFrame",
    "TakeFunction"
]


class AppliedFunction(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def name(cls) -> str:
        pass

    @abstractmethod
    def to_sql(self, base_query: QuerySpecification, config: FrameToSqlConfig) -> QuerySpecification:
        pass


class TakeFunction(AppliedFunction):
    row_count: int

    @classmethod
    def name(cls) -> str:
        return "take"

    def __init__(self, row_count: int) -> None:
        self.row_count = row_count

    def to_sql(self, base_query: QuerySpecification, config: FrameToSqlConfig) -> QuerySpecification:
        base_query.limit = LongLiteral(value=self.row_count)
        return base_query  # TODO: avoid parameter mutation


class AppliedFunctionTdsFrame(BaseTdsFrame):
    base_frame: BaseTdsFrame
    applied_function: AppliedFunction

    def __init__(self, base_frame: BaseTdsFrame, applied_function: AppliedFunction):
        self.base_frame = base_frame
        self.applied_function = applied_function

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.base_frame.to_sql_query_object(config)
        return self.applied_function.to_sql(base_query, config)
