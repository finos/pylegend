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
)
from pylegend.core.tds.tds_column import TdsColumn

__all__: PyLegendSequence[str] = [
    "PyLegendTdsFrame",
    "FrameToPureConfig",
]


class FrameToPureConfig:
    __pretty: bool
    __indent: str
    __indent_level: int

    def __init__(
            self,
            pretty: bool = True,
            ident: str = "  ",
            indent_level: int = 0,
    ) -> None:
        self.__pretty = pretty
        self.__indent = ident
        self.__indent_level = indent_level

    def push_indent(self, extra_indent_level: int = 1) -> "FrameToPureConfig":
        return FrameToPureConfig(self.__pretty, self.__indent, self.__indent_level + extra_indent_level)

    def separator(self, extra_indent_level: int = 0, return_space_if_not_pretty: bool = False) -> str:
        if self.__pretty:
            return "\n" + (self.__indent * (self.__indent_level + extra_indent_level))
        else:
            return " " if return_space_if_not_pretty else ""


class PyLegendTdsFrame(metaclass=ABCMeta):

    @abstractmethod
    def columns(self) -> PyLegendSequence[TdsColumn]:
        pass  # pragma: no cover

    def schema(self) -> None:
        col_lines = [f"  {c.get_name()} ({c.get_type()})" for c in self.columns()]  # pragma: no cover
        print("Columns:\n" + "\n".join(col_lines))  # pragma: no cover

    @abstractmethod
    def to_pure_query(self, config: FrameToPureConfig = FrameToPureConfig()) -> str:
        pass  # pragma: no cover
