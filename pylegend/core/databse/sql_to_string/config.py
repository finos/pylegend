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


class SqlToStringFormat:
    multi_line: bool
    space: str
    indent_count: int

    def __init__(self, multi_line: bool = True, space: str = "  ", indent_count: int = 0) -> None:
        self.multi_line = multi_line
        self.space = space
        self.indent_count = indent_count

    def push_indent(self) -> "SqlToStringFormat":
        return SqlToStringFormat(self.multi_line, self.space, self.indent_count + 1)

    @property
    def indent(self) -> str:
        return "".join([self.space for _ in range(self.indent_count)])


class SqlToStringConfig:
    format_config: SqlToStringFormat
    quoted_identifiers: bool

    def __init__(
        self,
        format_config: SqlToStringFormat,
        quoted_identifiers: bool
    ) -> None:
        self.format_config = format_config
        self.quoted_identifiers = quoted_identifiers
