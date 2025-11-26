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
    PyLegendAggInput
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.helpers import escape_column_name, generate_pure_lambda
from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.language.shared.primitive_collection import PyLegendPrimitiveCollection, create_primitive_collection
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive, PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.sql.metamodel import QuerySpecification, SelectItem, SingleColumn
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig


class AggregateFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __func: PyLegendAggInput
    __axis: PyLegendUnion[int, str]
    __args: PyLegendSequence[PyLegendPrimitiveOrPythonPrimitive]
    __kwargs: PyLegendMapping[str, PyLegendPrimitiveOrPythonPrimitive]

    @classmethod
    def name(cls) -> str:
        return "aggregate"  # pragma: no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str],
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> None:
        self.__base_frame = base_frame
        self.__func = func
        self.__axis = axis
        self.__args = args
        self.__kwargs = kwargs

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()
        base_query: QuerySpecification = self.__base_frame.to_sql_query_object(config)

        should_create_sub_query = (
            len(base_query.groupBy) > 0 or
            base_query.select.distinct or
            base_query.offset is not None or
            base_query.limit is not None
        )

        new_query: QuerySpecification
        if should_create_sub_query:
            new_query = create_sub_query(base_query, config, "root")
        else:
            new_query = copy_query(base_query)

        new_select_items: PyLegendList[SelectItem] = []

        for agg in self.__aggregates_list:
            agg_sql_expr = agg[2].to_sql_expression({"r": new_query}, config)
            new_select_items.append(
                SingleColumn(alias=db_extension.quote_identifier(agg[0]), expression=agg_sql_expr)
            )

        new_query.select.selectItems = new_select_items
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        agg_strings = []
        for agg in self.__aggregates_list:
            map_expr_string = (agg[1].to_pure_expression(config) if isinstance(agg[1], PyLegendPrimitive)
                               else convert_literal_to_literal_expression(agg[1]).to_pure_expression(config))
            agg_expr_string = agg[2].to_pure_expression(config).replace(map_expr_string, "$c")
            agg_strings.append(f"{escape_column_name(agg[0])}:{generate_pure_lambda('r', map_expr_string)}:"
                               f"{generate_pure_lambda('c', agg_expr_string)}")

        return (f"{self.__base_frame.to_pure(config)}{config.separator(1)}"
                f"->aggregate({config.separator(2)}"
                f"~[{', '.join(agg_strings)}]{config.separator(1)}"
                f")")

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

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

        self.__aggregates_list: PyLegendList[
            PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]
        ] = []

        normalized_func: dict[str, PyLegendAggFunc] = self.__normalize_input_func_to_standard_dict(self.__func)
        tds_row = PandasApiTdsRow.from_tds_frame("r", self.__base_frame)

        for column_name, aggregate_function in normalized_func.items():
            mapper_function: PyLegendCallable[[PandasApiTdsRow], PyLegendPrimitiveOrPythonPrimitive] = eval(
                f'lambda r: r["{column_name}"]')
            map_result: PyLegendPrimitiveOrPythonPrimitive = mapper_function(tds_row)
            collection: PyLegendPrimitiveCollection = create_primitive_collection(map_result)

            normalized_aggregate_function = self.__normalize_agg_func_to_lambda_function(aggregate_function)
            agg_result: PyLegendPrimitive = normalized_aggregate_function(collection)

            self.__aggregates_list.append((column_name, map_result, agg_result))

        return True

    def __normalize_input_func_to_standard_dict(
            self,
            func_input: PyLegendAggInput
    ) -> dict[str, PyLegendAggFunc]:

        column_names = [col.get_name() for col in self.calculate_columns()]

        if isinstance(func_input, collections.abc.Mapping):
            normalized: dict[str, PyLegendAggFunc] = {}

            for key, value in func_input.items():
                if not isinstance(key, str):
                    raise TypeError(
                        f"Invalid `func` argument for the aggregate function.\n"
                        f"When a dictionary is provided, all keys must be strings.\n"
                        f"But got key: {key!r} (type: {type(key).__name__})\n"
                    )
                if key not in column_names:
                    raise ValueError(
                        f"Invalid `func` argument for the aggregate function.\n"
                        f"When a dictionary is provided, all keys must be column names.\n"
                        f"Available columns are: {sorted(column_names)}\n"
                        f"But got key: {key!r} (type: {type(key).__name__})\n"
                    )

                if isinstance(value, collections.abc.Sequence) and not isinstance(value, str):
                    if len(value) != 1:
                        raise ValueError(
                            f"Invalid `func` argument for the aggregate function.\n"
                            f"When providing a list of functions for a specific column, "
                            f"the list must contain exactly one element (single aggregation only).\n"
                            f"Column: {key!r}\n"
                            f"List Length: {len(value)}\n"
                            f"Value: {value!r}\n"
                        )

                    single_func = value[0]

                    if not (callable(single_func) or isinstance(single_func, str) or isinstance(single_func, np.ufunc)):
                        raise TypeError(
                            f"Invalid `func` argument for the aggregate function.\n"
                            f"The single element in the list for key {key!r} must be a callable, str, or np.ufunc.\n"
                            f"But got element: {single_func!r} (type: {type(single_func).__name__})\n"
                        )

                    normalized[key] = single_func

                else:
                    if not (callable(value) or isinstance(value, str) or isinstance(value, np.ufunc)):
                        raise TypeError(
                            f"Invalid `func` argument for the aggregate function.\n"
                            f"When a dictionary is provided, the value must be a callable, str, or np.ufunc "
                            f"(or a list containing exactly one of these).\n"
                            f"But got value for key {key!r}: {value!r} (type: {type(value).__name__})\n"
                        )
                    normalized[key] = value

            return normalized

        elif isinstance(func_input, collections.abc.Sequence) and not isinstance(func_input, str):

            if len(func_input) != 1:
                raise ValueError(
                    f"Invalid `func` argument for the aggregate function.\n"
                    f"When providing a list as the func argument, it must contain exactly one element "
                    f"(which will be applied to all columns).\n"
                    f"Multiple functions are not supported.\n"
                    f"List Length: {len(func_input)}\n"
                    f"Input: {func_input!r}\n"
                )

            single_func = func_input[0]

            if not (callable(single_func) or isinstance(single_func, str) or isinstance(single_func, np.ufunc)):
                raise TypeError(
                    f"Invalid `func` argument for the aggregate function.\n"
                    f"The single element in the top-level list must be a callable, str, or np.ufunc.\n"
                    f"But got element: {single_func!r} (type: {type(single_func).__name__})\n"
                )

            return {col: single_func for col in column_names}

        elif callable(func_input) or isinstance(func_input, str) or isinstance(func_input, np.ufunc):
            return {col: func_input for col in column_names}

        else:
            raise TypeError(
                "Invalid `func` argument for aggregate function. "
                "Expected a callable, str, np.ufunc, a list containing exactly one of these, "
                "or a mapping[str -> callable/str/ufunc/a list containing exactly one of these]. "
                f"But got: {func_input!r} (type: {type(func_input).__name__})"
            )

    def __normalize_agg_func_to_lambda_function(
            self,
            func: PyLegendAggFunc
    ) -> PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]:

        PYTHON_FUNCTION_TO_LEGEND_FUNCTION_MAPPING: PyLegendMapping[str, PyLegendList[str]] = {
            "average":         ["mean", "average", "nanmean"],
            "sum":             ["sum", "nansum"],
            "min":             ["min", "amin", "minimum", "nanmin"],
            "max":             ["max", "amax", "maximum", "nanmax"],
            "std_dev_sample":  ["std", "std_dev", "nanstd"],
            "variance_sample": ["var", "variance", "nanvar"],
            "median":          ["median", "nanmedian"],
            "count":           ["count", "size", "len", "length"],
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
