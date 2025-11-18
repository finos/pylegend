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
from enum import Enum
import json

from pylegend.core.sql.metamodel import ComparisonOperator

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence
)

__all__: PyLegendSequence[str] = [
    "TdsColumn",
    "PrimitiveTdsColumn",
    "PrimitiveType",
    "EnumTdsColumn",
    "PandasApiTdsColumn",
    "tds_columns_from_json",
]


class TdsColumn(metaclass=ABCMeta):
    __name: str

    def __init__(self, name: str) -> None:
        self.__name = name

    def get_name(self) -> str:
        return self.__name

    @abstractmethod
    def get_type(self) -> str:
        pass  # pragma: no cover

    @abstractmethod
    def copy(self) -> "TdsColumn":
        pass  # pragma: no cover

    @abstractmethod
    def copy_with_changed_name(self, new_name: str) -> "TdsColumn":
        pass  # pragma: no cover

    def __str__(self) -> str:
        return f"TdsColumn(Name: {self.get_name()}, Type: {self.get_type()})"


class PrimitiveType(Enum):
    Boolean = 1
    StrictDate = 2
    Number = 3
    String = 4
    LatestDate = 5
    Float = 6
    DateTime = 7
    Date = 8
    Integer = 9
    Decimal = 10


class PrimitiveTdsColumn(TdsColumn):
    __type: "PrimitiveType"

    def __init__(self, name: str, _type: "PrimitiveType") -> None:
        super().__init__(name)
        self.__type = _type

    def get_type(self) -> "str":
        return self.__type.name

    def copy(self) -> "TdsColumn":
        return PrimitiveTdsColumn(self.get_name(), self.__type)

    def copy_with_changed_name(self, new_name: str) -> "TdsColumn":
        return PrimitiveTdsColumn(new_name, self.__type)

    @classmethod
    def integer_column(cls, name: str) -> "PrimitiveTdsColumn":
        return PrimitiveTdsColumn(name, PrimitiveType.Integer)

    @classmethod
    def float_column(cls, name: str) -> "PrimitiveTdsColumn":
        return PrimitiveTdsColumn(name, PrimitiveType.Float)

    @classmethod
    def string_column(cls, name: str) -> "PrimitiveTdsColumn":
        return PrimitiveTdsColumn(name, PrimitiveType.String)

    @classmethod
    def boolean_column(cls, name: str) -> "PrimitiveTdsColumn":
        return PrimitiveTdsColumn(name, PrimitiveType.Boolean)

    @classmethod
    def number_column(cls, name: str) -> "PrimitiveTdsColumn":
        return PrimitiveTdsColumn(name, PrimitiveType.Number)

    @classmethod
    def date_column(cls, name: str) -> "PrimitiveTdsColumn":
        return PrimitiveTdsColumn(name, PrimitiveType.Date)

    @classmethod
    def datetime_column(cls, name: str) -> "PrimitiveTdsColumn":
        return PrimitiveTdsColumn(name, PrimitiveType.DateTime)

    @classmethod
    def strictdate_column(cls, name: str) -> "PrimitiveTdsColumn":
        return PrimitiveTdsColumn(name, PrimitiveType.StrictDate)


class EnumTdsColumn(TdsColumn):
    __enum_type: str
    __enum_values: PyLegendList[str]

    def __init__(self, name: str, enum_type: str, enum_values: PyLegendList[str]) -> None:
        super().__init__(name)
        self.__enum_type = enum_type
        self.__enum_values = enum_values

    def get_type(self) -> str:
        return self.__enum_type

    def copy(self) -> "TdsColumn":
        return EnumTdsColumn(self.get_name(), self.__enum_type, self.__enum_values)

    def copy_with_changed_name(self, new_name: str) -> "TdsColumn":
        return EnumTdsColumn(new_name, self.__enum_type, self.__enum_values)

    def get_enum_values(self) -> PyLegendList[str]:
        return self.__enum_values

class PandasApiTdsColumn(TdsColumn):
    __type: "PrimitiveType"

    def __init__(self, name: str, _type: "PrimitiveType", _base: any = None) -> None:
        super().__init__(name)
        self.__type = _type
        self.__base_frame = _base

    def get_type(self) -> "str":
        return self.__type.name

    def copy(self) -> "TdsColumn":
        return PandasApiTdsColumn(self.get_name(), self.__type)

    def copy_with_changed_name(self, new_name: str) -> "TdsColumn":
        return PandasApiTdsColumn(new_name, self.__type)

    def copy_with_base_frame(self, base_frame: "PandasApiBaseTdsFrame") -> "PandasApiTdsColumn":
        return PandasApiTdsColumn(self.get_name(), self.get_type(), base_frame)

    def to_pure_expression(self) -> str:
        return f"$c.{self.get_name()}"

    def to_pure_query(self, config: "FrameToPureConfig" = None) -> str:
        if self.__base_frame is None:
            raise RuntimeError("Base frame not set for column")
        from pylegend.core.tds.tds_frame import FrameToPureConfig
        config = config if config is not None else FrameToPureConfig()
        return self.__base_frame.filter(items=[self.get_name()]).to_pure(config)

    def to_sql_query(self, config: "FrameToSqlConfig" = None) -> str:
        if self.__base_frame is None:
            raise RuntimeError("Base frame not set for column")
        from pylegend.core.tds.tds_frame import FrameToSqlConfig
        from pylegend.core.database.sql_to_string.config import SqlToStringConfig, SqlToStringFormat
        config = config if config is not None else FrameToSqlConfig()
        frame = self.__base_frame.filter(items=[self.get_name()])
        query_spec = frame.to_sql_query_object(config)
        sql_to_string_config = SqlToStringConfig(
            format_=SqlToStringFormat(pretty=config.pretty)
        )
        return config.sql_to_string_generator().generate_sql_string(query_spec, sql_to_string_config)

    @classmethod
    def integer_column(cls, name: str) -> "PandasApiTdsColumn":
        return PandasApiTdsColumn(name, PrimitiveType.Integer)

    @classmethod
    def float_column(cls, name: str) -> "PandasApiTdsColumn":
        return PandasApiTdsColumn(name, PrimitiveType.Float)

    @classmethod
    def string_column(cls, name: str) -> "PandasApiTdsColumn":
        return PandasApiTdsColumn(name, PrimitiveType.String)

    @classmethod
    def boolean_column(cls, name: str) -> "PandasApiTdsColumn":
        return PandasApiTdsColumn(name, PrimitiveType.Boolean)

    @classmethod
    def number_column(cls, name: str) -> "PandasApiTdsColumn":
        return PandasApiTdsColumn(name, PrimitiveType.Number)

    @classmethod
    def date_column(cls, name: str) -> "PandasApiTdsColumn":
        return PandasApiTdsColumn(name, PrimitiveType.Date)

    @classmethod
    def datetime_column(cls, name: str) -> "PandasApiTdsColumn":
        return PandasApiTdsColumn(name, PrimitiveType.DateTime)

    @classmethod
    def strictdate_column(cls, name: str) -> "PandasApiTdsColumn":
        return PandasApiTdsColumn(name, PrimitiveType.StrictDate)

    def __eq__(self, other):
        from pylegend.core.tds.pandas_api.frames.functions.comparator_filtering import PandasApiComparatorFiltering
        return PandasApiComparatorFiltering(self.__base_frame, column=self, operator=ComparisonOperator.EQUAL, value=other)

    def __ne__(self, other):
        from pylegend.core.tds.pandas_api.frames.functions.comparator_filtering import PandasApiComparatorFiltering
        return PandasApiComparatorFiltering(self.__base_frame, column=self, operator=ComparisonOperator.NOT_EQUAL,
                                            value=other)

    def __lt__(self, other):
        from pylegend.core.tds.pandas_api.frames.functions.comparator_filtering import PandasApiComparatorFiltering
        return PandasApiComparatorFiltering(self.__base_frame, column=self, operator=ComparisonOperator.LESS_THAN,
                                            value=other)

    def __le__(self, other):
        from pylegend.core.tds.pandas_api.frames.functions.comparator_filtering import PandasApiComparatorFiltering
        return PandasApiComparatorFiltering(self.__base_frame, column=self,
                                            operator=ComparisonOperator.LESS_THAN_OR_EQUAL, value=other)

    def __gt__(self, other):
        from pylegend.core.tds.pandas_api.frames.functions.comparator_filtering import PandasApiComparatorFiltering
        return PandasApiComparatorFiltering(self.__base_frame, column=self, operator=ComparisonOperator.GREATER_THAN,
                                            value=other)

    def __ge__(self, other):
        from pylegend.core.tds.pandas_api.frames.functions.comparator_filtering import PandasApiComparatorFiltering
        return PandasApiComparatorFiltering(self.__base_frame, column=self,
                                            operator=ComparisonOperator.GREATER_THAN_OR_EQUAL, value=other)


def tds_columns_from_json(s: str) -> PyLegendSequence[TdsColumn]:
    try:
        parsed = json.loads(s)
        print("Parsed: ", parsed)

        enums = parsed["enums"] if "enums" in parsed else []
        enums = [enums] if isinstance(enums, dict) else enums

        columns = parsed["columns"]
        columns = [columns] if isinstance(columns, dict) else columns

        print("Columns: ", columns)

        result_columns: PyLegendList[TdsColumn] = []
        for col in columns:
            if col["_type"] == "primitiveSchemaColumn":
                result_columns.append(PrimitiveTdsColumn(col["name"], PrimitiveType[col["type"]]))
            else:
                result_columns.append(
                    EnumTdsColumn(col["name"], col["type"], _enum_values_for_type(col["type"], enums))
                )
        print("Result: ", result_columns)
        return result_columns

    except Exception as e:
        raise RuntimeError("Unable to parse tds columns from schema: \n" + s, e)


def _enum_values_for_type(enum_type: str, all_enums: PyLegendList[dict]) -> PyLegendList[str]:  # type: ignore
    for e in all_enums:
        if e["type"] == enum_type:
            values = e["values"]
            return values if isinstance(values, list) else [values]

    raise RuntimeError("Unknown enum type: " + enum_type)
