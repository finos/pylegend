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

import importlib
from abc import ABCMeta, abstractmethod
import pandas as pd
from pylegend._typing import (
    PyLegendSequence,
    PyLegendTypeVar,
    PyLegendOptional,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.databse.sql_to_string import SqlToStringGenerator
from pylegend.core.tds.result_handler import ResultHandler
from pylegend.extensions.tds.result_handler import PandasDfReadConfig

postgres_ext = 'pylegend.extensions.database.vendors.postgres.postgres_sql_to_string'
importlib.import_module(postgres_ext)

__all__: PyLegendSequence[str] = [
    "PyLegendTdsFrame",
    "FrameToSqlConfig"
]


class FrameToSqlConfig:
    database_type: str
    pretty: bool

    __sql_to_string_generator: SqlToStringGenerator

    def __init__(
            self,
            database_type: str = "Postgres",
            pretty: bool = True
    ) -> None:
        self.database_type = database_type
        self.pretty = pretty

        self.__sql_to_string_generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type(
            self.database_type
        )

    def sql_to_string_generator(self) -> SqlToStringGenerator:
        return self.__sql_to_string_generator


R = PyLegendTypeVar('R')


class PyLegendTdsFrame(metaclass=ABCMeta):

    @abstractmethod
    def columns(self) -> PyLegendSequence[TdsColumn]:
        pass  # pragma: no cover

    @abstractmethod
    def to_sql_query(self, config: FrameToSqlConfig = FrameToSqlConfig()) -> str:
        pass  # pragma: no cover

    @abstractmethod
    def execute_frame(
            self,
            result_handler: ResultHandler[R],
            chunk_size: PyLegendOptional[int] = None
    ) -> R:
        pass  # pragma: no cover

    @abstractmethod
    def execute_frame_to_string(
            self,
            chunk_size: PyLegendOptional[int] = None
    ) -> str:
        pass  # pragma: no cover

    @abstractmethod
    def execute_frame_to_pandas_df(
            self,
            chunk_size: PyLegendOptional[int] = None,
            pandas_df_read_config: PandasDfReadConfig = PandasDfReadConfig()
    ) -> pd.DataFrame:
        pass  # pragma: no cover
