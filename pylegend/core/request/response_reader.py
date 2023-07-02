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


from pylegend._typing import (
    PyLegendIterator,
    PyLegendSequence,
    PyLegendOptional,
)

__all__: PyLegendSequence["str"] = [
    "ResponseReader"
]


class ResponseReader:
    __iter_content: PyLegendIterator[bytes]
    __byte_array: bytearray

    def __init__(self, iter_content: PyLegendIterator[bytes]):
        self.__iter_content = iter_content
        self.__byte_array = bytearray()

    def __iter__(self) -> PyLegendIterator[bytes]:
        return self.__iter_content

    def read(self, n: PyLegendOptional[int] = None) -> bytes:
        if n == 0:
            return b""
        elif n is None:
            # Should read everything
            self.__byte_array.extend(b"".join(self.__iter_content))
            return bytes(self.__byte_array)
        else:
            while len(self.__byte_array) < n:
                try:
                    self.__byte_array.extend(next(self.__iter_content))
                except StopIteration:
                    break
            read_bytes, self.__byte_array = bytes(self.__byte_array[:n]), self.__byte_array[n:]
            return read_bytes
