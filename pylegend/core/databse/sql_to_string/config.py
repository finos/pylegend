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

    def separator(self, cnt: int = 0) -> str:
        if self.pretty:
            return "\n" + "".join(["    " for _ in range(self.indent_count + cnt)])
        else:
            return " "


class SqlToStringConfig:
    format: SqlToStringFormat

    def __init__(
        self,
        format_: SqlToStringFormat
    ) -> None:
        self.format = format_

    def push_indent(self) -> "SqlToStringConfig":
        return SqlToStringConfig(self.format.push_indent())
