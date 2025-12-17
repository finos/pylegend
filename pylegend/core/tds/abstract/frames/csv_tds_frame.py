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
from abc import ABCMeta
from pylegend._typing import (
    PyLegendSequence,
    PyLegendList,
)
from io import StringIO
from pylegend.core.tds.tds_column import (
    PrimitiveType,
    PrimitiveTdsColumn)
from pylegend.core.tds.abstract.frames.base_tds_frame import BaseTdsFrame
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.core.sql.metamodel import (
    QuerySpecification,
)
import pandas as pd

__all__: PyLegendSequence[str] = [
    "CsvTdsFrame",
    "tds_columns_from_csv_string"
]


class CsvTdsFrame(BaseTdsFrame, metaclass=ABCMeta):
    __csv_string: str

    def __init__(
            self,
            csv_string: str,
    ) -> None:
        super().__init__(columns=tds_columns_from_csv_string(csv_string))
        self.__csv_string = csv_string

    def get_all_tds_frames(self) -> PyLegendSequence["BaseTdsFrame"]:
        return [self]

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        raise RuntimeError("SQL generation for csv tds frames is not supported yet.")

    def to_pure(self, config: FrameToPureConfig) -> str:
        return f"#TDS\n{self.__csv_string}#"


def tds_columns_from_csv_string(
        csv_string: str
) -> PyLegendList[PrimitiveTdsColumn]:
    df = pd.read_csv(StringIO(csv_string))
    tds_columns = []
    dt = pd.api.types

    for col in df.columns:
        dtype = df[col].dtype

        if dt.is_bool_dtype(dtype):
            primitive_type = PrimitiveType.Boolean

        elif dt.is_integer_dtype(dtype):
            primitive_type = PrimitiveType.Integer

        elif dt.is_float_dtype(dtype):
            primitive_type = PrimitiveType.Float

        elif is_strict_date_or_datetime(df[col]):
            primitive_type = PrimitiveType.Date

        else:
            primitive_type = PrimitiveType.String

        tds_columns.append(
            PrimitiveTdsColumn(name=col, _type=primitive_type)
        )

    return tds_columns


def is_strict_date_or_datetime(col: pd.Series) -> bool:
    s = col.dropna()
    if s.empty:
        return False

    try:
        pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", exact=True, errors="raise")
        return True
    except (ValueError, TypeError):
        pass

    try:
        pd.to_datetime(s, format="%Y-%m-%d", exact=True, errors="raise")
        return True
    except (ValueError, TypeError):
        return False
