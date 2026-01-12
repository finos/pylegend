# Copyright 2026 Goldman Sachs
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

from typing import TYPE_CHECKING

import pandas as pd

from pylegend._typing import (
    PyLegendUnion,
    PyLegendSequence,
    PyLegendTuple,
    PyLegendCallable
)
from pylegend.core.language import PyLegendBoolean
from pylegend.core.tds.pandas_api.frames.functions.filtering import PandasApiFilteringFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunctionTdsFrame

if TYPE_CHECKING:
    from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
    from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame

__all__: PyLegendSequence[str] = [
    "PandasApiLocIndexer"
]


class PandasApiLocIndexer:
    _frame: "PandasApiBaseTdsFrame"

    def __init__(self, frame: "PandasApiBaseTdsFrame") -> None:
        self._frame = frame

    def __getitem__(  # type: ignore
            self,
            key: PyLegendUnion[
                slice,
                PyLegendBoolean,
                PyLegendCallable[["PandasApiBaseTdsFrame"], PyLegendBoolean],
                PyLegendTuple[
                    PyLegendUnion[slice, PyLegendBoolean, PyLegendCallable[["PandasApiBaseTdsFrame"], PyLegendBoolean]],
                    PyLegendUnion[str, slice, PyLegendSequence[str], PyLegendSequence[bool]]
                ]
            ]
    ) -> "PandasApiTdsFrame":
        rows: PyLegendUnion[  # type: ignore
            slice,
            PyLegendBoolean,
            PyLegendCallable[["PandasApiBaseTdsFrame"], PyLegendBoolean]
        ]
        cols: PyLegendUnion[str, slice, PyLegendSequence[str], PyLegendSequence[bool]]  # type: ignore

        if isinstance(key, tuple):
            if len(key) == 1:
                rows, cols = key[0], slice(None, None, None)
            elif len(key) == 2:
                rows, cols = key[0], key[1]
            else:
                raise IndexError("Too many indexers")
        else:
            rows, cols = key, slice(None, None, None)

        row_frame = self._handle_row_selection(rows)
        return self._handle_column_selection(row_frame, cols)

    def _handle_row_selection(  # type: ignore
            self,
            rows: PyLegendUnion[slice, PyLegendBoolean, PyLegendCallable[["PandasApiBaseTdsFrame"], PyLegendBoolean]]
    ) -> "PandasApiTdsFrame":
        if isinstance(rows, slice):
            if rows.start is None and rows.stop is None and rows.step is None:
                return self._frame
            else:
                raise TypeError(
                    "loc supports only ':' for row slicing. "
                    "Label-based slicing for rows is not supported."
                )

        if isinstance(rows, PyLegendBoolean):
            return PandasApiAppliedFunctionTdsFrame(
                PandasApiFilteringFunction(self._frame, filter_expr=rows)
            )

        if callable(rows):
            new_key = rows(self._frame)
            return self._handle_row_selection(new_key)

        raise TypeError(f"Unsupported key type for .loc row selection: {type(rows)}")

    def _handle_column_selection(  # type: ignore
            self,
            frame: "PandasApiTdsFrame",
            cols: PyLegendUnion[str, slice, PyLegendSequence[str], PyLegendSequence[bool]]
    ) -> "PandasApiTdsFrame":
        if isinstance(cols, slice) and cols.start is None and cols.stop is None and cols.step is None:
            return frame

        if isinstance(cols, str):
            return frame.filter(items=[cols])

        if isinstance(cols, (list, tuple)):
            all_columns = [c.get_name() for c in frame.columns()]
            is_boolean_list = all(isinstance(k, bool) for k in cols)

            if is_boolean_list:
                if len(cols) != len(all_columns):
                    raise IndexError(f"Boolean index has wrong length: {len(cols)} instead of {len(all_columns)}")
                selected_columns = [col for col, select in zip(all_columns, cols) if select]
                return frame.filter(items=selected_columns)
            else:
                missing_cols = [c for c in cols if c not in all_columns]
                if missing_cols:
                    raise KeyError(f"{missing_cols} not in index")
                return frame.filter(items=cols)  # type: ignore

        if isinstance(cols, slice):
            all_columns = [c.get_name() for c in frame.columns()]
            pd_index = pd.Index(all_columns)

            slicer = pd_index.slice_indexer(start=cols.start, end=cols.stop, step=cols.step)
            selected_columns = pd_index[slicer].tolist()
            if not selected_columns:
                return frame.head(0)
            return frame.filter(items=selected_columns)

        raise TypeError(f"Unsupported key type for .loc column selection: {type(cols)}")
