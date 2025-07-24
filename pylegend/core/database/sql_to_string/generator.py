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
    PyLegendSequence,
    PyLegendDict
)
from pylegend.utils.class_utils import find_sub_classes
from pylegend.core.database.sql_to_string.config import SqlToStringConfig
from pylegend.core.database.sql_to_string.db_extension import SqlToStringDbExtension
from pylegend.core.sql.metamodel import QuerySpecification

__all__: PyLegendSequence[str] = [
    "SqlToStringGenerator"
]


class SqlToStringGenerator(metaclass=ABCMeta):
    sql_generator_extensions: PyLegendDict[str, "SqlToStringGenerator"] = {}

    @classmethod
    @abstractmethod
    def database_type(cls) -> str:
        pass  # pragma: no cover

    @classmethod
    @abstractmethod
    def create_sql_generator(cls) -> "SqlToStringGenerator":
        pass  # pragma: no cover

    @abstractmethod
    def get_db_extension(self) -> SqlToStringDbExtension:
        pass  # pragma: no cover

    def generate_sql_string(self, query: QuerySpecification, config: "SqlToStringConfig") -> str:
        return self.get_db_extension().process_query_specification(query, config)

    @classmethod
    def find_sql_to_string_generator_for_db_type(cls, database_type: str) -> "SqlToStringGenerator":
        if database_type in SqlToStringGenerator.sql_generator_extensions:
            return SqlToStringGenerator.sql_generator_extensions[database_type]
        else:
            subclasses = find_sub_classes(SqlToStringGenerator, True)  # type: ignore
            filtered = [s for s in subclasses if s.database_type() == database_type]
            if len(filtered) != 1:
                raise RuntimeError(
                    "Found no (or multiple) sql to string generators for database type '" + database_type +
                    "'. Found generators: [" + ", ".join([str(x) for x in filtered]) + "]"
                )
            generator = filtered[0].create_sql_generator()
            SqlToStringGenerator.sql_generator_extensions[database_type] = generator
            return generator
