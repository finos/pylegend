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
from datetime import date, datetime
import pandas as pd
from pylegend.core.language.shared.expression import PyLegendExpressionBooleanReturn

from pylegend._typing import (
    PyLegendSequence,
    PyLegendTypeVar,
    PyLegendList,
    PyLegendOptional,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.sql.metamodel import QuerySpecification, ComparisonOperator
from pylegend.core.database.sql_to_string import (
    SqlToStringConfig,
    SqlToStringFormat
)
from pylegend.core.language import PyLegendPrimitive, LegacyApiTdsRow, PyLegendInteger
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import TdsColumn, PandasApiTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.result_handler import (
    ResultHandler,
    ToStringResultHandler,
)
from pylegend.extensions.tds.result_handler import (
    ToPandasDfResultHandler,
    PandasDfReadConfig,
)

__all__: PyLegendSequence[str] = [
    "PandasApiBaseTdsFrame"
]

R = PyLegendTypeVar('R')


class PandasApiBaseTdsFrame(PandasApiTdsFrame, metaclass=ABCMeta):
    __columns: PyLegendSequence[TdsColumn]

    def __init__(self, columns: PyLegendSequence[TdsColumn]) -> None:
        col_names = [c.get_name() for c in columns]
        if len(col_names) != len(set(col_names)):
            cols = "[" + ", ".join([str(c) for c in columns]) + "]"
            raise ValueError(f"TdsFrame cannot have duplicated column names. Passed columns: {cols}")
        self.__columns = [c.copy() for c in columns]

    def columns(self) -> PyLegendSequence[TdsColumn]:
        return [c.copy() for c in self.__columns]

    def __getitem__(self, key):
        if isinstance(key, PyLegendExpressionBooleanReturn):
            from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import \
                PandasApiAppliedFunctionTdsFrame
            from pylegend.core.tds.pandas_api.frames.functions.boolean_filtering import \
                PandasApiBooleanFilteringFunction
            return PandasApiAppliedFunctionTdsFrame(
                PandasApiBooleanFilteringFunction(self, filter_expr=key)
            )
        elif isinstance(key, str):
            # Use .filter for single column access
            return self.filter(items=[key])
        elif isinstance(key, list):
            # Use .filter for multiple columns access
            return self.filter(items=key)
        else:
            raise TypeError(f"Invalid key type: {type(key)}. Expected str, list, or boolean expression.")

    def assign(
            self,
            **kwargs: PyLegendCallable[
                [LegacyApiTdsRow],
                PyLegendUnion[int, float, bool, str, date, datetime, PyLegendPrimitive]
            ],
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.assign_function import AssignFunction
        return PandasApiAppliedFunctionTdsFrame(AssignFunction(self, col_definitions=kwargs))

    def shape(
            self
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.shape import PandasApiShapeFunction
        return PandasApiAppliedFunctionTdsFrame(PandasApiShapeFunction(self))

    def filter(
            self,
            items: PyLegendOptional[PyLegendUnion[list, PyLegendList]] = None,
            like: PyLegendOptional[str] = None,
            regex: PyLegendOptional[str] = None,
            axis: PyLegendUnion[str, int, PyLegendInteger] = None
    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.filter import PandasApiFilterFunction
        return PandasApiAppliedFunctionTdsFrame(
            PandasApiFilterFunction(
                self,
                items=items,
                like=like,
                regex=regex,
                axis=axis
            )
        )

    def comparator_filtering(
            self,
            column: PandasApiTdsColumn,
            operator: ComparisonOperator,
            value: PyLegendPrimitive

    ) -> "PandasApiTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.comparator_filtering import \
            PandasApiComparatorFiltering
        return PandasApiAppliedFunctionTdsFrame(
            PandasApiComparatorFiltering(
                self,
                column=column,
                operator=operator,
                value=value
            )
        )

    @abstractmethod
    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        pass  # pragma: no cover

    @abstractmethod
    def to_pure(self, config: FrameToPureConfig) -> str:
        pass  # pragma: no cover

    def to_pure_query(self, config: FrameToPureConfig = FrameToPureConfig()) -> str:
        return self.to_pure(config)

    @abstractmethod
    def get_all_tds_frames(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        pass  # pragma: no cover

    def to_sql_query(self, config: FrameToSqlConfig = FrameToSqlConfig()) -> str:
        query = self.to_sql_query_object(config)
        sql_to_string_config = SqlToStringConfig(
            format_=SqlToStringFormat(
                pretty=config.pretty
            )
        )
        return config.sql_to_string_generator().generate_sql_string(query, sql_to_string_config)

    def execute_frame(
            self,
            result_handler: ResultHandler[R],
            chunk_size: PyLegendOptional[int] = None
    ) -> R:
        from pylegend.core.tds.pandas_api.frames.pandas_api_input_tds_frame import (
            PandasApiInputTdsFrame,
            PandasApiExecutableInputTdsFrame
        )
        tds_frames = self.get_all_tds_frames()
        input_frames = [x for x in tds_frames if isinstance(x, PandasApiInputTdsFrame)]

        non_exec_frames = [x for x in input_frames if not isinstance(x, PandasApiExecutableInputTdsFrame)]
        if non_exec_frames:
            raise ValueError(
                "Cannot execute frame as its built on top of non-executable input frames: [" +
                (", ".join([str(f) for f in non_exec_frames]) + "]")
            )

        exec_frames = [x for x in input_frames if isinstance(x, PandasApiExecutableInputTdsFrame)]

        all_legend_clients = []
        for e in exec_frames:
            c = e.get_legend_client()
            if c not in all_legend_clients:
                all_legend_clients.append(c)
        if len(all_legend_clients) > 1:
            raise ValueError(
                "Found tds frames with multiple legend_clients (which is not supported): [" +
                (", ".join([str(f) for f in all_legend_clients]) + "]")
            )
        legend_client = all_legend_clients[0]
        result = legend_client.execute_sql_string(self.to_sql_query(), chunk_size=chunk_size)
        return result_handler.handle_result(self, result)

    def execute_frame_to_string(
            self,
            chunk_size: PyLegendOptional[int] = None
    ) -> str:
        return self.execute_frame(ToStringResultHandler(), chunk_size)

    def execute_frame_to_pandas_df(
            self,
            chunk_size: PyLegendOptional[int] = None,
            pandas_df_read_config: PandasDfReadConfig = PandasDfReadConfig()
    ) -> pd.DataFrame:
        return self.execute_frame(
            ToPandasDfResultHandler(pandas_df_read_config),
            chunk_size
        )
