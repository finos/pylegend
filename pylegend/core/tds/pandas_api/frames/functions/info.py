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

import sys
from io import StringIO
from typing import TYPE_CHECKING

from pylegend._typing import (
    PyLegendList,
    PyLegendOptional,
    PyLegendSequence,
    PyLegendTypeVar,
    PyLegendUnion
)
from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.tds.result_handler import ResultHandler
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.pandas_api.frames.pandas_api_input_tds_frame import (
    PandasApiInputTdsFrame,
    PandasApiExecutableInputTdsFrame,
)
if TYPE_CHECKING:
    from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame

R = PyLegendTypeVar('R')

class PandasApiInfoFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame

    @classmethod
    def name(cls) -> str:
        return "info"  # pragma: no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            verbose: PyLegendOptional[bool],
            buf: PyLegendOptional[StringIO],
            max_cols: PyLegendOptional[int],
            memory_usage: PyLegendOptional[PyLegendUnion[bool, str]],
            show_counts: PyLegendOptional[bool],
    ) -> None:
        self.__base_frame = base_frame
        self.__verbose = verbose
        self.__buf = buf
        self.__max_cols = max_cols
        self.__memory_usage = memory_usage
        self.__show_counts = show_counts

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        return self.__base_frame.to_sql_query_object(config)

    def to_pure(self, config: FrameToPureConfig) -> str:
        return self.__base_frame.to_pure(config)

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:

        return True

    def execute_frame(
            self,
            result_handler: ResultHandler[R],
            chunk_size: PyLegendOptional[int] = None
    ) -> R:
        tds_frames = self.get_all_tds_frames()
        input_frames = [x for x in tds_frames if isinstance(x, PandasApiInputTdsFrame)]
        non_exec_frames = [x for x in input_frames if not isinstance(x, PandasApiExecutableInputTdsFrame)]
        if non_exec_frames:
            raise ValueError(
                "Cannot execute frame as its built on top of non-executable input frames: [" +
                (", ".join([str(f) for f in non_exec_frames]) + "]")
            )

        exec_frames = [x for x in input_frames if isinstance(x, PandasApiExecutableInputTdsFrame)]
        all_legend_clients = [e.get_legend_client() for e in exec_frames if e.get_legend_client()]
        if not all_legend_clients:
            raise ValueError("No legend client found to execute query.")
        if len(set(all_legend_clients)) > 1:
            raise ValueError(
                "Found tds frames with multiple legend_clients (which is not supported): [" +
                (", ".join([str(c) for c in set(all_legend_clients)]) + "]")
            )
        legend_client = all_legend_clients[0]

        sql_query = self.to_sql_query(FrameToSqlConfig())
        response_reader = legend_client.execute_sql_string(sql_query, chunk_size=chunk_size)
        result_bytes = b"".join(response_reader)
        result_text = result_bytes.decode("utf-8")

        result_rows = result_text.strip().splitlines()
        columns = self.__base_frame.columns()
        col_names = [c.get_name() for c in columns]

        if not result_rows or len(result_rows) <= 1:
            total_rows = 0
            non_null_counts = {name: 0 for name in col_names}
        else:
            header = result_rows[0].split(',')
            data_rows = result_rows[1:]
            total_rows = len(data_rows)
            header_indices = {h.strip(): i for i, h in enumerate(header)}
            non_null_counts = {name: 0 for name in col_names}

            for row_str in data_rows:
                row_values = row_str.split(',')
                for col_name in col_names:
                    col_index = header_indices.get(col_name)
                    if col_index is not None and col_index < len(row_values):
                        value = row_values[col_index]
                        if value is not None and value != '':
                            non_null_counts[col_name] += 1

        show_counts = self.__show_counts if self.__show_counts is not None else True

        output = StringIO()
        output.write(f"<class 'pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame'>\n")
        output.write(f"RangeIndex: {total_rows} entries\n")
        output.write(f"Data columns (total {len(columns)} columns):\n")

        headers = ["#", "Column", "Non-Null Count", "Dtype"]
        col_data = []
        for i, col in enumerate(columns):
            non_null_count_str = ""
            if show_counts:
                non_null_count = non_null_counts[col.get_name()]
                non_null_count_str = f"{non_null_count} non-null"
            col_data.append([str(i), col.get_name(), non_null_count_str, col.get_type()])

        widths = [max(len(str(item)) for item in col) for col in zip(*([headers] + col_data))]
        header_line = "  ".join(f"{h:<{w}}" for h, w in zip(headers, widths))
        separator_line = "  ".join("-" * w for w in widths)
        output.write(header_line + "\n")
        output.write(separator_line + "\n")

        for row in col_data:
            output.write("  ".join(f"{item:<{w}}" for item, w in zip(row, widths)) + "\n")

        dtypes = [c.get_type() for c in columns]
        dtype_summary = ", ".join(f"{d}({dtypes.count(d)})" for d in sorted(set(dtypes)))
        output.write(f"dtypes: {dtype_summary}\n")

        # memory
        memory_usage = self.__memory_usage if self.__memory_usage is not None else True
        if memory_usage:
            mem_bytes = sys.getsizeof(result_bytes)
            if mem_bytes < 1024:
                mem_str = f"{mem_bytes} bytes"
            elif mem_bytes < 1024 * 1024:
                mem_str = f"{mem_bytes / 1024:.2f} KB"
            else:
                mem_str = f"{mem_bytes / (1024 * 1024):.2f} MB"
            output.write(f"memory usage: {mem_str}\n")
        else:
            output.write("memory usage: ? (disabled)\n")

        print(f"Output:\n{output.getvalue()}\n")
        return result_handler.handle_result(self.__base_frame, output.getvalue())