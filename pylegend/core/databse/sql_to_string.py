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
from pylegend._typing import PyLegendSequence
from pylegend.utils.class_utils import find_sub_classes

__all__: PyLegendSequence[str] = [
    "SqlToStringGenerator",
    "SqlToStringConfig"
]


class SqlToStringGenerator(metaclass=ABCMeta):

    @classmethod
    @abstractmethod
    def database_type(cls) -> str:
        pass  # pragma: no cover

    @classmethod
    @abstractmethod
    def create_sql_generator(cls) -> "SqlToStringGenerator":
        pass  # pragma: no cover

    @abstractmethod
    def generate_sql_string(self, sql: object, config: "SqlToStringConfig") -> str:
        pass  # pragma: no cover

    @classmethod
    def find_sql_to_string_generator_for_db_type(cls, database_type: str) -> "SqlToStringGenerator":
        subclasses = find_sub_classes(SqlToStringGenerator, True)  # type: ignore
        filtered = [s for s in subclasses if s.database_type() == database_type]
        if len(filtered) != 1:
            raise RuntimeError(
                "Found no (or multiple) sql to string generators for database type '" + database_type +
                "'. Found generators: [" + ", ".join([str(x) for x in filtered]) + "]"
            )
        return filtered[0].create_sql_generator()


class SqlToStringConfig:
    pass
