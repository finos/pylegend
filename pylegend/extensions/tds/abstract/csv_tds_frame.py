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
import csv
import re
from datetime import datetime
from pylegend._typing import (
    PyLegendSequence,
    PyLegendList,
)
from io import StringIO
from pylegend.core.tds.tds_column import (
    PrimitiveType,
    PrimitiveTdsColumn)
from pylegend.core.tds.tds_frame import FrameToPureConfig, PyLegendTdsFrame

__all__: PyLegendSequence[str] = [
    "CsvInputFrameAbstract",
    "tds_columns_from_csv_string"
]

# Matches a Pure decimal literal: digits with optional decimal point, ending in 'd' or 'D'
# e.g. "21d", "31.0d", "101.0D", "3.14d"
_PURE_DECIMAL_SUFFIX_RE = re.compile(r'^(\d+(?:\.\d+)?)[dD]$')

_DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%d",
]


def _strip_decimal_suffix(csv_string: str) -> tuple[str, set[str]]:
    """Strip Pure decimal suffix (d/D) from values and track which columns had it.

    Uses Python's csv module to correctly handle quoted fields containing
    commas, escaped quotes, and other edge-cases that naive split(',')
    would break on.
    """
    reader = csv.reader(StringIO(csv_string.strip()))
    rows = list(reader)
    if len(rows) < 2:
        return csv_string, set()

    headers = [h.strip() for h in rows[0]]
    decimal_columns: set[str] = set()
    new_rows = [headers]

    for row in rows[1:]:
        if not any(cell.strip() for cell in row):
            continue  # skip blank rows  # pragma: no cover
        new_values = []
        for i, val in enumerate(row):
            val = val.strip()
            m = _PURE_DECIMAL_SUFFIX_RE.match(val)
            if m:
                new_values.append(m.group(1))  # pragma: no cover
                if i < len(headers):  # pragma: no cover
                    decimal_columns.add(headers[i])  # pragma: no cover
            else:
                new_values.append(val)
        new_rows.append(new_values)

    out = StringIO()
    writer = csv.writer(out, lineterminator='\n')
    writer.writerows(new_rows)
    return out.getvalue(), decimal_columns


def _infer_column_type(values: PyLegendList[str], col_name: str, decimal_columns: set[str]) -> PrimitiveType:
    """Infer a PrimitiveType from a list of raw string values (may include empty strings for nulls)."""
    if col_name in decimal_columns:
        return PrimitiveType.Decimal  # pragma: no cover

    non_null = [v for v in values if v.strip() != ""]
    if not non_null:
        return PrimitiveType.String

    # Boolean check: all non-null values are True/False (case-insensitive)
    if all(v.strip().lower() in ("true", "false") for v in non_null):
        return PrimitiveType.Boolean

    # Integer check
    if all(_is_integer(v) for v in non_null):
        return PrimitiveType.Integer

    # Float check
    if all(_is_float(v) for v in non_null):
        return PrimitiveType.Float

    # Date/Datetime check
    if all(_is_date_or_datetime(v) for v in non_null):
        return PrimitiveType.Date

    return PrimitiveType.String


def _is_integer(val: str) -> bool:
    try:
        int(val.strip())
        return True
    except ValueError:
        return False


def _is_float(val: str) -> bool:
    try:
        float(val.strip())
        return True
    except ValueError:
        return False


def _is_date_or_datetime(val: str) -> bool:
    v = val.strip()
    for fmt in _DATE_FORMATS:
        try:
            datetime.strptime(v, fmt)
            return True
        except ValueError:
            continue
    return False


class CsvInputFrameAbstract(PyLegendTdsFrame, metaclass=ABCMeta):
    __csv_string: str

    def __init__(
            self,
            csv_string: str,
    ) -> None:
        super().__init__(columns=tds_columns_from_csv_string(csv_string))  # type: ignore[call-arg]
        self.__csv_string = csv_string

    def to_pure(self, config: FrameToPureConfig) -> str:
        return f"#TDS\n{self.__csv_string}#"


def tds_columns_from_csv_string(
        csv_string: str
) -> PyLegendList[PrimitiveTdsColumn]:
    cleaned_csv, decimal_columns = _strip_decimal_suffix(csv_string)
    reader = csv.reader(StringIO(cleaned_csv.strip()))
    rows = list(reader)

    if not rows or not rows[0]:
        raise ValueError("No columns to parse from file")

    headers = [h.strip() for h in rows[0]]
    data_rows = [
        [cell.strip() for cell in row]
        for row in rows[1:]
        if any(cell.strip() for cell in row)
    ]

    tds_columns = []
    for col_idx, col_name in enumerate(headers):
        col_values = [row[col_idx] if col_idx < len(row) else "" for row in data_rows]
        primitive_type = _infer_column_type(col_values, col_name, decimal_columns)
        tds_columns.append(
            PrimitiveTdsColumn(name=_remove_quotes_if_present(col_name), _type=primitive_type)
        )

    return tds_columns


def _remove_quotes_if_present(col_name: str) -> str:
    if len(col_name) >= 2 and col_name[0] == col_name[-1] and col_name[0] in ("'", '"'):
        return col_name[1:-1]
    return col_name
