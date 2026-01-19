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
from pylegend._typing import (
    PyLegendUnion,
    PyLegendTuple,
    PyLegendSequence,
)

if TYPE_CHECKING:
    from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
    from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame


__all__: PyLegendSequence[str] = [
    "PandasApiIlocIndexer"
]


class PandasApiIlocIndexer:
    _frame: "PandasApiBaseTdsFrame"

    def __init__(self, frame: "PandasApiBaseTdsFrame") -> None:
        self._frame = frame

    def __getitem__(  # type: ignore
            self,
            key: PyLegendUnion[int, slice, PyLegendTuple[PyLegendUnion[int, slice], ...]]
    ) -> "PandasApiTdsFrame":
        if isinstance(key, tuple):
            if len(key) > 2:
                raise IndexError("Too many indexers")
            elif len(key) == 1:
                rows, cols = key[0], slice(None, None, None)
            else:
                rows, cols = key  # type: ignore
        else:
            rows, cols = key, slice(None, None, None)

        # Row selection
        row_frame = self._handle_row_selection(rows)

        # Column selection
        return self._handle_column_selection(row_frame, cols)

    def _handle_row_selection(self, rows: PyLegendUnion[int, slice]) -> "PandasApiTdsFrame":  # type: ignore
        if isinstance(rows, slice):
            if rows.step is not None and rows.step != 1:
                raise NotImplementedError("iloc with slice step other than 1 is not supported yet in Pandas Api")

            start = rows.start
            stop = rows.stop
            after = stop - 1 if stop is not None else None
            return self._frame.truncate(before=start, after=after)

        elif isinstance(rows, int):
            return self._frame.truncate(before=rows, after=rows)

        else:
            raise NotImplementedError(
                f"iloc supports integer, slice, or tuple of these, but got indexer of type: {type(rows)}"
            )

    def _handle_column_selection(  # type: ignore
            self,
            frame: "PandasApiTdsFrame",
            cols: PyLegendUnion[int, slice]
    ) -> "PandasApiTdsFrame":
        if isinstance(cols, slice):
            if cols.step is not None and cols.step != 1:
                raise NotImplementedError("iloc with slice step other than 1 is not supported yet in Pandas Api")

            all_columns = [c.get_name() for c in frame.columns()]
            selected_columns = all_columns[cols]
            return frame.filter(items=selected_columns)

        elif isinstance(cols, int):
            all_columns = [c.get_name() for c in frame.columns()]
            if not -len(all_columns) <= cols < len(all_columns):
                raise IndexError("single positional indexer is out-of-bounds")
            selected_column = all_columns[cols]
            return frame.filter(items=[selected_column])

        else:
            raise NotImplementedError(
                f"iloc supports integer, slice, or tuple of these, but got indexer of type: {type(cols)}"
            )
