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
    pretty: bool
    indent_count: int

    def __init__(self, pretty: bool = True, indent_count: int = 0) -> None:
        self.pretty = pretty
        self.indent_count = indent_count

    def push_indent(self) -> "SqlToStringFormat":
        return SqlToStringFormat(self.pretty, self.indent_count + 1)

    @property
    def separator(self) -> str:
        if self.pretty:
            return "\n" + "".join(["    " for _ in range(self.indent_count)])
        else:
            return " "


class SqlToStringConfig:
    format: SqlToStringFormat
    quoted_identifiers: bool

    def __init__(
        self,
        format_config: SqlToStringFormat,
        quoted_identifiers: bool
    ) -> None:
        self.format = format_config
        self.quoted_identifiers = quoted_identifiers
