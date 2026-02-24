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

import importlib
from abc import ABCMeta, abstractmethod
import pandas as pd
from pylegend._typing import (
    PyLegendSequence,
    PyLegendTypeVar,
    PyLegendOptional,
    PyLegendDict,
    PyLegendList,
    PyLegendSet,
)
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn, PrimitiveType
from pylegend.core.database.sql_to_string import SqlToStringGenerator
from pylegend.core.tds.result_handler import ResultHandler
from pylegend.extensions.tds.result_handler import PandasDfReadConfig

postgres_ext = 'pylegend.extensions.database.vendors.postgres.postgres_sql_to_string'
importlib.import_module(postgres_ext)

__all__: PyLegendSequence[str] = [
    "PyLegendTdsFrame",
    "FrameToSqlConfig",
    "FrameToPureConfig",
]


class FrameToSqlConfig:
    database_type: str
    pretty: bool

    __sql_to_string_generator: SqlToStringGenerator

    def __init__(
            self,
            database_type: str = "Postgres",
            pretty: bool = True
    ) -> None:
        self.database_type = database_type
        self.pretty = pretty

        self.__sql_to_string_generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type(
            self.database_type
        )

    def sql_to_string_generator(self) -> SqlToStringGenerator:
        return self.__sql_to_string_generator


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


R = PyLegendTypeVar('R')

_NUMERIC_TYPES: PyLegendSet[PrimitiveType] = {
    PrimitiveType.Number, PrimitiveType.Integer, PrimitiveType.Float,
    PrimitiveType.Decimal, PrimitiveType.TinyInt, PrimitiveType.UTinyInt,
    PrimitiveType.SmallInt, PrimitiveType.USmallInt, PrimitiveType.Int,
    PrimitiveType.UInt, PrimitiveType.BigInt, PrimitiveType.UBigInt,
    PrimitiveType.Float4, PrimitiveType.Double, PrimitiveType.Numeric,
}

_STRING_TYPES: PyLegendSet[PrimitiveType] = {
    PrimitiveType.String, PrimitiveType.Varchar,
}

_DATE_TYPES: PyLegendSet[PrimitiveType] = {
    PrimitiveType.Date, PrimitiveType.DateTime,
    PrimitiveType.StrictDate, PrimitiveType.Timestamp,
}

_BOOLEAN_TYPES: PyLegendSet[PrimitiveType] = {
    PrimitiveType.Boolean,
}

# Allowed cast: source family -> set of target families it can cast into.
# Key = frozenset id of the source family, Value = list of allowed target family sets.
_ALLOWED_CAST_TARGETS: PyLegendDict[str, PyLegendList[PyLegendSet[PrimitiveType]]] = {
    "numeric": [_NUMERIC_TYPES, _STRING_TYPES],   # numeric -> numeric or string
    "string":  [_STRING_TYPES],                     # string -> string only
    "date":    [_DATE_TYPES],                       # date -> date only
    "boolean": [_BOOLEAN_TYPES],                    # boolean -> boolean only
}


def _type_family(t: PrimitiveType) -> str:
    if t in _NUMERIC_TYPES:
        return "numeric"
    if t in _STRING_TYPES:
        return "string"
    if t in _DATE_TYPES:
        return "date"
    if t in _BOOLEAN_TYPES:
        return "boolean"
    return "unknown"  # pragma: no cover


def _is_cast_allowed(source: PrimitiveType, target: PrimitiveType) -> bool:
    family = _type_family(source)
    allowed_target_sets = _ALLOWED_CAST_TARGETS.get(family, [])
    return any(target in s for s in allowed_target_sets)


class PyLegendTdsFrame(metaclass=ABCMeta):

    @abstractmethod
    def columns(self) -> PyLegendSequence[TdsColumn]:
        pass  # pragma: no cover

    def schema(self) -> None:
        col_lines = [f"  {c.get_name()} ({c.get_type()})" for c in self.columns()]  # pragma: no cover
        print("Columns:\n" + "\n".join(col_lines))  # pragma: no cover

    @abstractmethod
    def to_sql_query(self, config: FrameToSqlConfig = FrameToSqlConfig()) -> str:
        pass  # pragma: no cover

    @abstractmethod
    def to_pure_query(self, config: FrameToPureConfig = FrameToPureConfig()) -> str:
        pass  # pragma: no cover

    @abstractmethod
    def execute_frame(
            self,
            result_handler: ResultHandler[R],
            chunk_size: PyLegendOptional[int] = None
    ) -> R:
        pass  # pragma: no cover

    @abstractmethod
    def execute_frame_to_string(
            self,
            chunk_size: PyLegendOptional[int] = None
    ) -> str:
        pass  # pragma: no cover

    @abstractmethod
    def execute_frame_to_pandas_df(
            self,
            chunk_size: PyLegendOptional[int] = None,
            pandas_df_read_config: PandasDfReadConfig = PandasDfReadConfig()
    ) -> pd.DataFrame:
        pass  # pragma: no cover

    def cast(
            self,
            column_type_map: PyLegendDict[str, PrimitiveType]
    ) -> PyLegendList[TdsColumn]:
        """Cast columns to new types.

        Takes a mapping of column name -> PrimitiveType and returns a new list of
        TdsColumn objects with the specified columns changed to the target types.

        Allowed casts:
            - Numeric -> Numeric (any numeric family type to any other)
            - Numeric -> String  (number to text representation)
            - String  -> String  (String <-> Varchar)
            - Date    -> Date    (any date family type to any other)
            - Boolean -> Boolean (identity only)

        Args:
            column_type_map: A dictionary mapping column names to their target PrimitiveType.

        Returns:
            A new list of TdsColumn with the updated types applied.

        Raises:
            ValueError: If a column name in the map does not exist in the frame,
                        or if the cast is not allowed between the source and target types.
            TypeError: If a column in the map is not a PrimitiveTdsColumn (e.g. EnumTdsColumn).
        """
        current_columns = self.columns()
        current_col_names = {c.get_name() for c in current_columns}

        unknown_cols = set(column_type_map.keys()) - current_col_names
        if unknown_cols:
            raise ValueError(
                f"Column(s) not found in frame: {sorted(unknown_cols)}. "
                f"Available columns: {sorted(current_col_names)}"
            )

        new_columns: PyLegendList[TdsColumn] = []
        for col in current_columns:
            if col.get_name() in column_type_map:
                if not isinstance(col, PrimitiveTdsColumn):
                    raise TypeError(
                        f"Cannot cast non-primitive column '{col.get_name()}' "
                        f"(type: {col.get_type()}). Only PrimitiveTdsColumn can be cast."
                    )
                source_type = PrimitiveType[col.get_type()]
                target_type = column_type_map[col.get_name()]
                if not _is_cast_allowed(source_type, target_type):
                    source_family = _type_family(source_type)
                    allowed_families = sorted({
                        _type_family(t)
                        for sets in _ALLOWED_CAST_TARGETS.get(source_family, [])
                        for t in sets
                    })
                    raise ValueError(
                        f"Cannot cast column '{col.get_name()}' from {source_type.name} to {target_type.name}. "
                        f"{source_family.capitalize()} types can only be cast to: {allowed_families}"
                    )
                new_columns.append(
                    PrimitiveTdsColumn(col.get_name(), target_type)
                )
            else:
                new_columns.append(col.copy())
        return new_columns

    def to_pandas_df(
            self,
            chunk_size: PyLegendOptional[int] = None,
            pandas_df_read_config: PandasDfReadConfig = PandasDfReadConfig()
    ) -> pd.DataFrame:
        return self.execute_frame_to_pandas_df(chunk_size, pandas_df_read_config)  # pragma: no cover

    def to_pandas(
            self,
            chunk_size: PyLegendOptional[int] = None,
            pandas_df_read_config: PandasDfReadConfig = PandasDfReadConfig()
    ) -> pd.DataFrame:
        return self.execute_frame_to_pandas_df(chunk_size, pandas_df_read_config)  # pragma: no cover
