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

from typing import TYPE_CHECKING, Callable, Any
from pylegend._typing import (
    PyLegendUnion,
    PyLegendSequence,
    PyLegendList,
    PyLegendTuple,
)
from pylegend.core.language import PyLegendBoolean
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunctionTdsFrame
from pylegend.core.tds.pandas_api.frames.functions.filtering import PandasApiFilteringFunction


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

    def __getitem__(
            self,
            key: PyLegendUnion[
                slice,
                PyLegendBoolean,
                Callable[["PandasApiBaseTdsFrame"], PyLegendBoolean],
                PyLegendTuple[
                    PyLegendUnion[slice, PyLegendBoolean, Callable[["PandasApiBaseTdsFrame"], PyLegendBoolean]],
                    PyLegendUnion[str, slice, PyLegendList[str], PyLegendList[bool]]
                ]
            ]
    ) -> "PandasApiTdsFrame":
        rows: PyLegendUnion[slice, PyLegendBoolean, Callable[["PandasApiBaseTdsFrame"], PyLegendBoolean]]
        cols: PyLegendUnion[str, slice, PyLegendList[str], PyLegendList[bool]]

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

    def _handle_row_selection(
            self,
            rows: PyLegendUnion[slice, PyLegendBoolean, Callable[["PandasApiBaseTdsFrame"], PyLegendBoolean]]
    ) -> "PandasApiTdsFrame":
        if isinstance(rows, slice):
            if rows.start is None and rows.stop is None and rows.step is None:
                return self._frame  # type: ignore
            else:
                raise TypeError(
                    "loc supports only ':' for row slicing. "
                    "Label-based or integer-based slicing for rows is not supported."
                )

        if isinstance(rows, PyLegendBoolean):
            return PandasApiAppliedFunctionTdsFrame(
                PandasApiFilteringFunction(self._frame, filter_expr=rows)  # type: ignore
            )

        if callable(rows):
            new_key = rows(self._frame)
            return self._handle_row_selection(new_key)

        raise TypeError(f"Unsupported key type for .loc row selection: {type(rows)}")

    def _handle_column_selection(
            self,
            frame: "PandasApiTdsFrame",
            cols: PyLegendUnion[str, slice, PyLegendList[str], PyLegendList[bool]]
    ) -> "PandasApiTdsFrame":
        if isinstance(cols, slice) and cols.start is None and cols.stop is None and cols.step is None:
            return frame

        if isinstance(cols, str):
            return frame.filter(items=[cols])

        if isinstance(cols, list):
            if all(isinstance(k, bool) for k in cols):
                all_columns = [c.get_name() for c in frame.columns()]
                if len(cols) != len(all_columns):
                    raise IndexError(f"Boolean index has wrong length: "
                                     f"{len(cols)} instead of {len(all_columns)}")
                selected_columns = [col for col, select in zip(all_columns, cols) if select]
                return frame.filter(items=selected_columns)
            elif all(isinstance(k, str) for k in cols):
                return frame.filter(items=cols)
            else:
                raise TypeError("List for loc must contain either all strings or all booleans")

        if isinstance(cols, slice):
            all_columns = [c.get_name() for c in frame.columns()]
            start = cols.start
            stop = cols.stop

            try:
                start_index = all_columns.index(start) if start is not None else 0
            except ValueError as e:
                raise KeyError(f"Start label '{start}' not found in columns") from e

            try:
                stop_index = all_columns.index(stop) if stop is not None else len(all_columns) - 1
            except ValueError as e:
                raise KeyError(f"Stop label '{stop}' not found in columns") from e

            if start_index > stop_index:
                return frame.filter(items=[])

            selected_columns = all_columns[start_index:stop_index + 1]
            return frame.filter(items=selected_columns)

        raise TypeError(f"Unsupported key type for .loc column selection: {type(cols)}")
