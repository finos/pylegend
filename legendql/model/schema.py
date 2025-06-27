# Copyright 2025 Goldman Sachs
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

from __future__ import annotations
from dataclasses import dataclass
from typing import List
import pyarrow as pa


class Schema(list):
    name: str

    def __init__(self, *args, **kwargs):
        super(Schema, self).__init__(args)
        for arg in args:
            arg.schema = self
        self.name = kwargs.get('name', None)

    def append(self, __object):
        __object.schema = self
        return super(Schema, self).append(__object)


@dataclass
class Table:
    table: str
    columns: [pa.Field]
    schema: Schema = None

    def validate_column(self, column: str) -> bool:
        return self.column(column) is not None

    def column(self, name: str) -> pa.Field:
        return next(filter(lambda c: c.name == name, self.columns), None)

    def add_column(self, column: pa.Field):
        self.columns.append(column)

    def remove_column(self, column: str):
        col = self.column(column)
        if col:
            self.columns.remove(col)


@dataclass
class Database:
    name: str
    children: List[Table | Schema]
    pass
