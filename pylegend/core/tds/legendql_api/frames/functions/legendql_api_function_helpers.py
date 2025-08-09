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

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.language import PyLegendColumnExpression
from pylegend.core.language.legendql_api.legendql_api_custom_expressions import LegendQLApiPrimitive
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame


__all__: PyLegendSequence[str] = [
    "infer_columns_from_frame"
]


def infer_columns_from_frame(
    base_frame: LegendQLApiBaseTdsFrame,
    columns: PyLegendUnion[
        str,
        PyLegendList[str],
        PyLegendCallable[
            [LegendQLApiTdsRow],
            PyLegendUnion[LegendQLApiPrimitive, PyLegendList[LegendQLApiPrimitive]]
        ]
    ],
    context: str
) -> PyLegendList[str]:
    if isinstance(columns, str):
        return [columns]
    if isinstance(columns, list) and all([isinstance(c, str) for c in columns]):
        return columns

    if isinstance(columns, type(lambda x: 0)) and (columns.__code__.co_argcount == 1):
        tds_row = LegendQLApiTdsRow.from_tds_frame("frame", base_frame)
        try:
            result = columns(tds_row)
        except Exception as e:
            raise RuntimeError(
                f"{context} argument lambda incompatible. Error occurred while evaluating. Message: " + str(e)
            ) from e

        list_result = result if isinstance(result, list) else [result]

        columns_list = []
        for (i, r) in enumerate(list_result):
            if isinstance(r, LegendQLApiPrimitive) and isinstance(r.value(), PyLegendColumnExpression):
                col_expr: PyLegendColumnExpression = r.value()
                columns_list.append(col_expr.get_column())
            else:
                raise TypeError(
                    f"{context} argument lambda incompatible. Columns can be simple column expressions "
                    f"(E.g - lambda r: [r.column1, r.column2, r['column with spaces']). "
                    f"Element at index {i} (0-indexed) is incompatible"
                )
        return columns_list

    raise TypeError(f"{context} argument can either be a list of strings (column names) or "
                    f"a lambda function which takes one argument (LegendQLApiTdsRow)")
