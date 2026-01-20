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

import numpy as np
import collections.abc
from pylegend._typing import (
    PyLegendCallable,
    PyLegendSequence,
    PyLegendTuple,
    PyLegendUnion,
    PyLegendList,
    PyLegendMapping,
)
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import (
    PyLegendAggFunc,
    PyLegendAggInput,
    PyLegendAggList,
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.helpers import escape_column_name, generate_pure_lambda
from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.language.shared.primitive_collection import PyLegendPrimitiveCollection, create_primitive_collection
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
from pylegend.core.language.shared.primitives.date import PyLegendDate
from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
from pylegend.core.language.shared.primitives.float import PyLegendFloat
from pylegend.core.language.shared.primitives.integer import PyLegendInteger
from pylegend.core.language.shared.primitives.number import PyLegendNumber
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive, PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.language.shared.primitives.strictdate import PyLegendStrictDate
from pylegend.core.language.shared.primitives.string import PyLegendString
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SelectItem,
    SingleColumn,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.tds.tds_column import PrimitiveTdsColumn, TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig


class AggregateFunction(PandasApiAppliedFunction):
    __base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame]
    __func: PyLegendAggInput
    __axis: PyLegendUnion[int, str]
    __args: PyLegendSequence[PyLegendPrimitiveOrPythonPrimitive]
    __kwargs: PyLegendMapping[str, PyLegendPrimitiveOrPythonPrimitive]

    @classmethod
    def name(cls) -> str:
        return "aggregate"  # pragma: no cover

    def __init__(
        self,
        base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame],
        func: PyLegendAggInput,
        axis: PyLegendUnion[int, str],
        *args: PyLegendPrimitiveOrPythonPrimitive,
        **kwargs: PyLegendPrimitiveOrPythonPrimitive,
    ) -> None:
        self.__base_frame = base_frame
        self.__func = func
        self.__axis = axis
        self.__args = args
        self.__kwargs = kwargs

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()

        base_query: QuerySpecification = self.base_frame().to_sql_query_object(config)

        should_create_sub_query = (
            len(base_query.groupBy) > 0
            or base_query.select.distinct
            or base_query.offset is not None
            or base_query.limit is not None
        )

        new_query: QuerySpecification
        if should_create_sub_query:
            new_query = create_sub_query(base_query, config, "root")
        else:
            new_query = copy_query(base_query)

        new_select_items: PyLegendList[SelectItem] = []

        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            columns_to_retain: PyLegendList[str] = [
                db_extension.quote_identifier(x.get_name()) for x in self.__base_frame.get_grouping_columns()
            ]
            new_cols_with_index: PyLegendList[PyLegendTuple[int, "SelectItem"]] = []
            for col in new_query.select.selectItems:
                if not isinstance(col, SingleColumn):
                    raise ValueError(
                        "Group By operation not supported for queries " "with columns other than SingleColumn"
                    )  # pragma: no cover
                if col.alias is None:
                    raise ValueError(
                        "Group By operation not supported for queries " "with SingleColumns with missing alias"
                    )  # pragma: no cover
                if col.alias in columns_to_retain:
                    new_cols_with_index.append((columns_to_retain.index(col.alias), col))

            new_select_items = [y[1] for y in sorted(new_cols_with_index, key=lambda x: x[0])]

        for agg in self.__aggregates_list:
            agg_sql_expr = agg[2].to_sql_expression({"r": new_query}, config)

            new_select_items.append(SingleColumn(alias=db_extension.quote_identifier(agg[0]), expression=agg_sql_expr))

        new_query.select.selectItems = new_select_items

        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            tds_row = PandasApiTdsRow.from_tds_frame("r", self.base_frame())
            new_query.groupBy = [
                (lambda x: x[c.get_name()])(tds_row).to_sql_expression({"r": new_query}, config)
                for c in self.__base_frame.get_grouping_columns()
            ]

        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        agg_strings = []
        for agg in self.__aggregates_list:
            map_expr_string = (
                agg[1].to_pure_expression(config)
                if isinstance(agg[1], PyLegendPrimitive)
                else convert_literal_to_literal_expression(agg[1]).to_pure_expression(config)
            )
            agg_expr_string = agg[2].to_pure_expression(config).replace(map_expr_string, "$c")
            agg_strings.append(
                f"{escape_column_name(agg[0])}:{generate_pure_lambda('r', map_expr_string)}:"
                f"{generate_pure_lambda('c', agg_expr_string)}"
            )

        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            group_strings = []
            for col in self.__base_frame.get_grouping_columns():
                group_strings.append(escape_column_name(col.get_name()))

            pure_expression = (
                f"{self.base_frame().to_pure(config)}{config.separator(1)}" + f"->groupBy({config.separator(2)}"
                f"~[{', '.join(group_strings)}],{config.separator(2, True)}"
                f"~[{', '.join(agg_strings)}]{config.separator(1)}"
                f")"
            )

            return pure_expression
        else:
            return (
                f"{self.__base_frame.to_pure(config)}{config.separator(1)}"
                f"->aggregate({config.separator(2)}"
                f"~[{', '.join(agg_strings)}]{config.separator(1)}"
                f")"
            )

    def base_frame(self) -> PandasApiBaseTdsFrame:
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            return self.__base_frame.base_frame()
        else:
            return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        new_columns = []

        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            base_cols_map = {c.get_name(): c for c in self.base_frame().columns()}
            for group_col in self.__base_frame.get_grouping_columns():
                group_col_name = group_col.get_name()
                if group_col_name in base_cols_map:
                    new_columns.append(base_cols_map[group_col_name].copy())

        for alias, _, agg_expr in self.__aggregates_list:
            new_columns.append(self.__infer_column_from_expression(alias, agg_expr))

        return new_columns

    def __infer_column_from_expression(self, name: str, expr: PyLegendPrimitive) -> TdsColumn:
        if isinstance(expr, PyLegendInteger):
            return PrimitiveTdsColumn.integer_column(name)
        elif isinstance(expr, PyLegendFloat):
            return PrimitiveTdsColumn.float_column(name)
        elif isinstance(expr, PyLegendNumber):
            return PrimitiveTdsColumn.number_column(name)
        elif isinstance(expr, PyLegendString):
            return PrimitiveTdsColumn.string_column(name)
        elif isinstance(expr, PyLegendBoolean):
            return PrimitiveTdsColumn.boolean_column(name)  # pragma: no cover
        elif isinstance(expr, PyLegendDate):
            return PrimitiveTdsColumn.date_column(name)
        elif isinstance(expr, PyLegendDateTime):
            return PrimitiveTdsColumn.datetime_column(name)
        elif isinstance(expr, PyLegendStrictDate):
            return PrimitiveTdsColumn.strictdate_column(name)
        else:
            raise TypeError(f"Could not infer TdsColumn type for aggregation result type: {type(expr)}")  # pragma: no cover

    def validate(self) -> bool:
        if self.__axis not in [0, "index"]:
            raise NotImplementedError(
                f"The 'axis' parameter of the aggregate function must be 0 or 'index', but got: {self.__axis}"
            )

        if len(self.__args) > 0 or len(self.__kwargs) > 0:
            raise NotImplementedError(
                "AggregateFunction currently does not support additional positional "
                "or keyword arguments. Please remove extra *args/**kwargs."
            )

        self.__aggregates_list: PyLegendList[PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]] = []

        normalized_func: dict[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]] = (
            self.__normalize_input_func_to_standard_dict(self.__func)
        )

        tds_row = PandasApiTdsRow.from_tds_frame("r", self.base_frame())

        group_cols: set[str] = set()
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            group_cols = set([col.get_name() for col in self.__base_frame.get_grouping_columns()])

        for column_name, agg_input in normalized_func.items():
            mapper_function: PyLegendCallable[[PandasApiTdsRow], PyLegendPrimitiveOrPythonPrimitive] = eval(
                f'lambda r: r["{column_name}"]'
            )
            map_result: PyLegendPrimitiveOrPythonPrimitive = mapper_function(tds_row)
            collection: PyLegendPrimitiveCollection = create_primitive_collection(map_result)

            if isinstance(agg_input, list):
                lambda_counter = 0
                for func in agg_input:
                    is_anonymous_lambda = False
                    if not isinstance(func, str):
                        if getattr(func, "__name__", "<lambda>") == "<lambda>":
                            is_anonymous_lambda = True

                    if is_anonymous_lambda:
                        lambda_counter += 1

                    normalized_agg_func = self.__normalize_agg_func_to_lambda_function(func)
                    agg_result = normalized_agg_func(collection)

                    alias = self._generate_column_alias(column_name, func, lambda_counter)
                    self.__aggregates_list.append((alias, map_result, agg_result))

            else:
                normalized_agg_func = self.__normalize_agg_func_to_lambda_function(agg_input)
                agg_result = normalized_agg_func(collection)

                if column_name in group_cols:
                    alias = self._generate_column_alias(column_name, agg_input, 0)
                else:
                    alias = column_name

                self.__aggregates_list.append((alias, map_result, agg_result))

        return True

    def __normalize_input_func_to_standard_dict(
        self, func_input: PyLegendAggInput
    ) -> dict[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]]:

        validation_columns: PyLegendList[str]
        default_broadcast_columns: PyLegendList[str]
        group_cols: set[str] = set()

        all_cols = [col.get_name() for col in self.base_frame().columns()]

        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            group_cols = set([col.get_name() for col in self.__base_frame.get_grouping_columns()])

            selected_cols = self.__base_frame.get_selected_columns()

            if selected_cols is not None:
                validation_columns = [col.get_name() for col in selected_cols]
                default_broadcast_columns = [col.get_name() for col in selected_cols]
            else:
                validation_columns = all_cols
                default_broadcast_columns = [c for c in all_cols if c not in group_cols]
        else:
            validation_columns = all_cols
            default_broadcast_columns = all_cols

        if isinstance(func_input, collections.abc.Mapping):
            normalized: dict[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]] = {}

            for key, value in func_input.items():
                if not isinstance(key, str):
                    raise TypeError(
                        f"Invalid `func` argument for the aggregate function.\n"
                        f"When a dictionary is provided, all keys must be strings.\n"
                        f"But got key: {key!r} (type: {type(key).__name__})\n"
                    )

                if key not in validation_columns:
                    raise ValueError(
                        f"Invalid `func` argument for the aggregate function.\n"
                        f"When a dictionary is provided, all keys must be column names.\n"
                        f"Available columns are: {sorted(validation_columns)}\n"
                        f"But got key: {key!r} (type: {type(key).__name__})\n"
                    )

                if isinstance(value, collections.abc.Sequence) and not isinstance(value, str):
                    for i, f in enumerate(value):
                        if not (callable(f) or isinstance(f, str) or isinstance(f, np.ufunc)):
                            raise TypeError(
                                f"Invalid `func` argument for the aggregate function.\n"
                                f"When a list is provided for a column, all elements must be callable, str, or np.ufunc.\n"
                                f"But got element at index {i}: {f!r} (type: {type(f).__name__})\n"
                            )
                    normalized[key] = value

                else:
                    if not (callable(value) or isinstance(value, str) or isinstance(value, np.ufunc)):
                        raise TypeError(
                            f"Invalid `func` argument for the aggregate function.\n"
                            f"When a dictionary is provided, the value must be a callable, str, or np.ufunc "
                            f"(or a list containing these).\n"
                            f"But got value for key '{key}': {value} (type: {type(value).__name__})\n"
                        )

                    if key in group_cols:
                        normalized[key] = [value]
                    else:
                        normalized[key] = value

            return normalized

        elif isinstance(func_input, collections.abc.Sequence) and not isinstance(func_input, str):
            for i, f in enumerate(func_input):
                if not (callable(f) or isinstance(f, str) or isinstance(f, np.ufunc)):
                    raise TypeError(
                        f"Invalid `func` argument for the aggregate function.\n"
                        f"When a list is provided as the main argument, all elements must be callable, str, or np.ufunc.\n"
                        f"But got element at index {i}: {f!r} (type: {type(f).__name__})\n"
                    )

            return {col: func_input for col in default_broadcast_columns}

        elif callable(func_input) or isinstance(func_input, str) or isinstance(func_input, np.ufunc):
            return {col: func_input for col in default_broadcast_columns}

        else:
            raise TypeError(
                "Invalid `func` argument for aggregate function. "
                "Expected a callable, str, np.ufunc, a list containing exactly one of these, "
                "or a mapping[str -> callable/str/ufunc/a list containing exactly one of these]. "
                f"But got: {func_input!r} (type: {type(func_input).__name__})"
            )

    def __normalize_agg_func_to_lambda_function(
        self, func: PyLegendAggFunc
    ) -> PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]:

        PYTHON_FUNCTION_TO_LEGEND_FUNCTION_MAPPING: PyLegendMapping[str, PyLegendList[str]] = {
            "average": ["mean", "average", "nanmean"],
            "sum": ["sum", "nansum"],
            "min": ["min", "amin", "minimum", "nanmin"],
            "max": ["max", "amax", "maximum", "nanmax"],
            "std_dev_sample": ["std", "std_dev", "nanstd"],
            "variance_sample": ["var", "variance", "nanvar"],
            "count": ["count", "size", "len", "length"],
        }

        FLATTENED_FUNCTION_MAPPING: dict[str, str] = {}
        for target_method, source_list in PYTHON_FUNCTION_TO_LEGEND_FUNCTION_MAPPING.items():
            for alias in source_list:
                FLATTENED_FUNCTION_MAPPING[alias] = target_method

        lambda_source: str
        final_lambda: PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]

        if isinstance(func, str):
            func_lower = func.lower()
            if func_lower in FLATTENED_FUNCTION_MAPPING:
                internal_method_name = FLATTENED_FUNCTION_MAPPING[func_lower]
            else:
                raise NotImplementedError(
                    f"Invalid `func` argument for the aggregate function.\n"
                    f"The string {func!r} does not correspond to any supported aggregation.\n"
                    f"Available string functions are: {sorted(FLATTENED_FUNCTION_MAPPING.keys())}"
                )  # pragma: no cover
            lambda_source = self._generate_lambda_source(internal_method_name)
            final_lambda = eval(lambda_source)
            return final_lambda

        elif isinstance(func, np.ufunc):
            func_name = func.__name__
            if func_name in FLATTENED_FUNCTION_MAPPING:
                internal_method_name = FLATTENED_FUNCTION_MAPPING[func_name]
            else:
                raise NotImplementedError(
                    f"Invalid `func` argument for the aggregate function.\n"
                    f"The NumPy function {func_name!r} is not supported.\n"
                    f"Supported aggregate functions are: {sorted(FLATTENED_FUNCTION_MAPPING.keys())}"
                )  # pragma: no cover
            lambda_source = self._generate_lambda_source(internal_method_name)
            final_lambda = eval(lambda_source)
            return final_lambda

        else:
            func_name = getattr(func, "__name__", "").lower()
            if func_name in FLATTENED_FUNCTION_MAPPING and func_name != "<lambda>":
                internal_method_name = FLATTENED_FUNCTION_MAPPING[func_name]
                lambda_source = self._generate_lambda_source(internal_method_name)
                final_lambda = eval(lambda_source)
                return final_lambda
            else:

                def validation_wrapper(x: PyLegendPrimitiveCollection) -> PyLegendPrimitive:
                    result = func(x)
                    if not isinstance(result, PyLegendPrimitive):
                        raise TypeError(
                            f"Custom aggregation function must return a PyLegendPrimitive (Expression).\n"
                            f"But got type: {type(result).__name__}\n"
                            f"Value: {result!r}"
                        )  # pragma: no cover
                    return result

                return validation_wrapper

    def _generate_lambda_source(self, internal_method_name: str) -> str:
        return f"lambda x: x.{internal_method_name}()"

    def _generate_column_alias(self, col_name: str, func: PyLegendAggFunc, lambda_counter: int) -> str:
        if isinstance(func, str):
            return f"{func}({col_name})"

        func_name = getattr(func, "__name__", "<lambda>")

        if func_name != "<lambda>":
            return f"{func_name}({col_name})"
        else:
            return f"lambda_{lambda_counter}({col_name})"
