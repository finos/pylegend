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

from typing import Generator, Tuple, List


class PyLegendCopyrightCheckerPlugin:

    def __init__(self, tree: str, lines: List[str], filename: str) -> None:
        self.tree: str = tree
        self.lines: List[str] = lines
        self.filename: str = filename

        __license_lines: List[str] = [
            "#\n",
            "# Licensed under the Apache License, Version 2.0 (the \"License\");\n",
            "# you may not use this file except in compliance with the License.\n",
            "# You may obtain a copy of the License at\n",
            "#\n",
            "#      http://www.apache.org/licenses/LICENSE-2.0\n",
            "#\n",
            "# Unless required by applicable law or agreed to in writing, software\n",
            "# distributed under the License is distributed on an \"AS IS\" BASIS,\n",
            "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n",
            "# See the License for the specific language governing permissions and\n",
            "# limitations under the License.\n"
        ]

        __valid_copyright_first_lines: List[str] = [
            "# Copyright 2023 Goldman Sachs\n",
            "# Copyright 2024 Goldman Sachs\n",
            "# Copyright 2025 Goldman Sachs\n",
        ]

        self.__copyright_line_count: int = 1 + len(__license_lines)
        self.__valid_copyright_lines: List[List[str]] = [[c] + __license_lines for c in __valid_copyright_first_lines]

    def run(self) -> Generator[Tuple[int, int, str, type], None, None]:

        if (len(self.lines) < self.__copyright_line_count) or \
                (self.lines[0:self.__copyright_line_count] not in self.__valid_copyright_lines):
            yield (
                1,
                0,
                "L101 Invalid copyright header in " + self.filename,
                type(self)
            )
