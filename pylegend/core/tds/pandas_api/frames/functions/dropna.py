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

from functools import reduce

from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
    PyLegendList,
    PyLegendUnion,
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    LogicalBinaryType,
    LogicalBinaryExpression, BooleanLiteral,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
    PandasApiAppliedFunction,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import (
    PandasApiBaseTdsFrame,
)
from pylegend.core.tds.sql_query_helpers import copy_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig

__all__: PyLegendSequence[str] = ["PandasApiDropnaFunction"]


class PandasApiDropnaFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __axis: PyLegendUnion[int, str]
    __how: str
    __thresh: PyLegendOptional[int]
    __subset: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]]
    __inplace: bool
    __ignore_index: bool

    @classmethod
    def name(cls) -> str:
        return "dropna"  # pragma: no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            axis: PyLegendUnion[int, str],
            how: str,
            thresh: PyLegendOptional[int],
            subset: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]],
            inplace: bool,
            ignore_index: bool
    ) -> None:
        self.__base_frame = base_frame
        self.__axis = axis
        self.__how = how
        self.__thresh = thresh
        self.__subset = subset
        self.__inplace = inplace
        self.__ignore_index = ignore_index

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.__base_frame.to_sql_query_object(config)
        new_query = copy_query(base_query)

        if self.__subset is not None:
            cols_to_check = self.__subset
        else:
            cols_to_check = [c.get_name() for c in self.__base_frame.columns()]

        if not cols_to_check:
            if self.__how == 'all':
                new_query.where = BooleanLiteral(value=False)
            return new_query

        tds_row = PandasApiTdsRow.from_tds_frame("c", self.__base_frame)

        filter_expr = None

        conditions = [tds_row[col].is_not_null() for col in cols_to_check]
        if self.__how == "any":
            filter_expr = reduce(lambda x, y: x & y, conditions)
        else:  # "all"
            filter_expr = reduce(lambda x, y: x | y, conditions)

        sql_expr = filter_expr.to_sql_expression({"c": new_query}, config)
        if new_query.where is None:
            new_query.where = sql_expr
        else:
            new_query.where = LogicalBinaryExpression(
                type_=LogicalBinaryType.AND,
                left=new_query.where,
                right=sql_expr
            )

        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        base_pure = self.__base_frame.to_pure(config)
        if self.__subset is not None:
            cols_to_check = self.__subset
        else:
            cols_to_check = [c.get_name() for c in self.__base_frame.columns()]

        if not cols_to_check:
            if self.__how == 'all':
                return f"{base_pure}{config.separator(1)}->filter(c|1!=1)"
            return base_pure

        tds_row = PandasApiTdsRow.from_tds_frame("c", self.__base_frame)
        conditions = [tds_row[col].is_not_null() for col in cols_to_check]

        if self.__how == "any":
            filter_expr = reduce(lambda x, y: x & y, conditions)
        else:  # "all"
            filter_expr = reduce(lambda x, y: x | y, conditions)

        pure_expr = filter_expr.to_pure_expression(config)
        return f"{base_pure}{config.separator(1)}->filter(c|{pure_expr})"

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        if self.__axis not in (0, 1, "index", "columns"):
            raise ValueError(f"No axis named {self.__axis} for object type TdsFrame")
        if self.__axis in (1, "columns"):
            raise NotImplementedError("axis=1 is not supported yet in Pandas API dropna")

        if self.__thresh is not None:
            raise NotImplementedError("thresh parameter is not supported yet in Pandas API dropna")

        if self.__how not in ("any", "all"):
            raise ValueError(f"invalid how option: {self.__how}")

        if self.__subset is not None:
            if not isinstance(self.__subset, (list, tuple, set)):
                raise TypeError(f"subset must be a list, tuple or set of column names. Got {type(self.__subset)}")
            valid_cols = {c.get_name() for c in self.__base_frame.columns()}
            invalid_cols = [s for s in self.__subset if s not in valid_cols]
            if invalid_cols:
                raise KeyError(f"{invalid_cols}")

        if self.__inplace:
            raise NotImplementedError("inplace=True is not supported yet in Pandas API dropna")

        if self.__ignore_index:
            raise NotImplementedError("ignore_index=True is not supported yet in Pandas API dropna")

        return True
