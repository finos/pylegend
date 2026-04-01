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
import re
from pylegend._typing import (
    PyLegendSequence,
    PyLegendList,
)
from io import StringIO
from pylegend.core.tds.tds_column import (
    PrimitiveType,
    PrimitiveTdsColumn)
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig, PyLegendTdsFrame
from pylegend.core.sql.metamodel import (
    QuerySpecification,
)
import pandas as pd

__all__: PyLegendSequence[str] = [
    "CsvInputFrameAbstract",
    "tds_columns_from_csv_string"
]

# Matches a Pure decimal literal: digits with optional decimal point, ending in 'd' or 'D'
# e.g. "21d", "31.0d", "101.0D", "3.14d"
_PURE_DECIMAL_SUFFIX_RE = re.compile(r'^(\d+(?:\.\d+)?)[dD]$')


def _strip_decimal_suffix(csv_string: str) -> tuple[str, set[str]]:
    """Strip Pure decimal suffix (d/D) from values and track which columns had it."""
    lines = csv_string.strip().split('\n')
    if len(lines) < 2:
        return csv_string, set()

    header_line = lines[0].strip()
    headers = [h.strip() for h in header_line.split(',')]
    decimal_columns: set[str] = set()
    new_lines = [header_line]

    for line in lines[1:]:
        stripped = line.strip()
        if not stripped:
            continue
        values = [v.strip() for v in stripped.split(',')]
        new_values = []
        for i, val in enumerate(values):
            m = _PURE_DECIMAL_SUFFIX_RE.match(val)
            if m:
                new_values.append(m.group(1))
                if i < len(headers):
                    decimal_columns.add(headers[i])
            else:
                new_values.append(val)
        new_lines.append(','.join(new_values))

    return '\n'.join(new_lines) + '\n', decimal_columns


class CsvInputFrameAbstract(PyLegendTdsFrame, metaclass=ABCMeta):
    __csv_string: str

    def __init__(
            self,
            csv_string: str,
    ) -> None:
        super().__init__(columns=tds_columns_from_csv_string(csv_string))  # type: ignore[call-arg]
        self.__csv_string = csv_string

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        raise RuntimeError("SQL generation for csv tds frames is not supported yet.")

    def to_pure(self, config: FrameToPureConfig) -> str:
        return f"#TDS\n{self.__csv_string}#"


def tds_columns_from_csv_string(
        csv_string: str
) -> PyLegendList[PrimitiveTdsColumn]:
    cleaned_csv, decimal_columns = _strip_decimal_suffix(csv_string)
    df = pd.read_csv(StringIO(cleaned_csv))
    tds_columns = []
    dt = pd.api.types

    for col in df.columns:
        col_name = str(col).strip()
        dtype = df[col].dtype

        if col_name in decimal_columns:
            primitive_type = PrimitiveType.Decimal

        elif dt.is_bool_dtype(dtype):
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
            PrimitiveTdsColumn(name=_remove_quotes_if_present(col), _type=primitive_type)
        )

    return tds_columns


def _remove_quotes_if_present(col_name: str) -> str:
    if len(col_name) >= 2 and col_name[0] == col_name[-1] and col_name[0] in ("'", '"'):
        return col_name[1:-1]
    return col_name


def is_strict_date_or_datetime(col: pd.Series) -> bool:  # type: ignore[explicit-any]
    try:
        pd.to_datetime(col, format="%Y-%m-%d %H:%M:%S", exact=True, errors="raise")
        return True
    except (ValueError, TypeError):
        pass

    try:
        pd.to_datetime(col, format="%Y-%m-%dT%H:%M:%S.%f%z", exact=True, errors="raise")
        return True
    except (ValueError, TypeError):
        pass

    try:
        pd.to_datetime(col, format="%Y-%m-%d", exact=True, errors="raise")
        return True
    except (ValueError, TypeError):
        return False