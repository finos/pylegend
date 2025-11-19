# Copyright 2025 Goldman Sachs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import ast
import inspect
import textwrap
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
    PyLegendAggList,
    PyLegendAggInput
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.primitive_collection import PyLegendPrimitiveCollection, create_primitive_collection
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive, PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.language.shared.primitives.string import PyLegendString
from pylegend.core.language.shared.tds_row import AbstractTdsRow
from pylegend.core.sql.metamodel import QuerySpecification, SingleColumn
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig


class AggregateFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __func: dict[
        str,
        PyLegendList[
            PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]
        ]
    ]
    __axis: PyLegendUnion[int, str]
    __args: PyLegendSequence[PyLegendPrimitive]
    __kwargs: PyLegendMapping[str, PyLegendPrimitive]

    @classmethod
    def name(cls) -> str:
        return "aggregate_function"  # pragma: no cover
    
    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str],
            *args: PyLegendPrimitive,
            **kwargs: PyLegendPrimitive
    ) -> None:
        self.__base_frame = base_frame
        self.__func_input = func
        self.__axis = axis
        self.__args = args
        self.__kwargs = kwargs

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
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

        tds_row = PandasApiTdsRow.from_tds_frame("r", self.__base_frame)
        aggregates_list: PyLegendList[PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]] = []

        for column_name, func_list in self.__func.items():
            mapper_function: PyLegendCallable[[AbstractTdsRow], PyLegendPrimitiveOrPythonPrimitive] = eval(
                f"lambda r: r.{column_name}")
            map_result: PyLegendPrimitiveOrPythonPrimitive = mapper_function(tds_row)
            collection: PyLegendPrimitiveCollection = create_primitive_collection(map_result)

            for func in func_list:
                agg_result: PyLegendPrimitive = func(collection)
                aggregates_list.append(("lambda_applied", map_result, agg_result))

        new_select_items: PyLegendList[str] = []

        for agg in aggregates_list:
            agg_sql_expr = agg[2].to_sql_expression({"r": new_query}, config)

            new_select_items.append(
                SingleColumn(alias=agg[0], expression=agg_sql_expr)
            )

        new_query.select.selectItems = new_select_items

        return new_query

        

    def to_pure(self, config: FrameToPureConfig) -> str:
        pass

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

        normalized_func: dict[str, PyLegendAggFunc] = self.__normalize_input_func_to_standard_dict(self.__func_input)
        self.__func = dict()
        for col, func_list in normalized_func.items():
            self.__func[col] = [self.__normalize_agg_func_to_lambda_function(func) for func in func_list]

        return True

    def __normalize_input_func_to_standard_dict(
            self,
            func_input: PyLegendAggInput
    ) -> dict[str, PyLegendAggList]:

        column_names = {col.get_name() for col in self.calculate_columns()}

        if isinstance(func_input, collections.abc.Mapping):
            normalized: dict[str, PyLegendAggList] = {}

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
                    for i, f in enumerate(value):
                        if not (callable(f) or isinstance(f, str) or isinstance(f, np.ufunc)):
                            raise TypeError(
                                f"Invalid `func` argument for the aggregate function.\n"
                                f"When a dictionary is provided with a key whose value is a list, "
                                f"all elements of the list must be of type callable, str, or np.ufunc.\n"
                                f"But got element at index {i} (0-indexed) of key "
                                f"{key!r}: {f!r} (type: {type(f).__name__})\n"
                            )
                    normalized[key] = value
                else:
                    if not (callable(value) or isinstance(value, str) or isinstance(f, np.ufunc)):
                        raise TypeError(
                            f"Invalid `func` argument for the aggregate function.\n"
                            f"When a dictionary is provided, a key can have its value as a "
                            f"callable, str, np.ufunc, or a list of those.\n"
                            f"But got value of the key {key!r}: {value!r} (type: {type(value).__name__})\n"
                        )
                    normalized[key] = PyLegendAggList([value])

            return normalized

        elif isinstance(func_input, collections.abc.Sequence) and not isinstance(func_input, str):
            for i, f in enumerate(func_input):
                if not (callable(f) or isinstance(f, str) or isinstance(f, np.ufunc)):
                    raise TypeError(
                        f"Invalid `func` argument for the aggregate function.\n"
                        f"When a list is provided, all elements must be of type callable, str, or np.ufunc.\n"
                        f"But got element at index {i} (0-indexed): {f!r} (type: {type(f).__name__})\n"
                    )
            return {col: PyLegendAggList(func_input.copy())
                    for col in column_names}

        elif callable(func_input) or isinstance(func_input, str) or isinstance(func_input, np.ufunc):
            return {col: PyLegendAggList([func_input])
                    for col in column_names}

        else:
            raise TypeError(
                "Invalid `func` argument for aggregate function. "
                "Expected a callable, str, np.ufunc, or a list of those, "
                "or a mapping[str -> those or a list of those]. "
                f"But got: {func_input!r} (type: {type(func_input).__name__})"
            )

    def __normalize_agg_func_to_lambda_function(
            self,
            func: PyLegendAggFunc
    ) -> PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]:
        
        if isinstance(func, str):
            raise NotImplementedError("WIP")
        
        if isinstance(func, np.ufunc):
            raise NotImplementedError("WIP")
        
        return func
        
        # ALLOWED_METHODS = {
        #     'sum', 'count',
        #     'min', 'max',
        #     'mean', 'median',
        #     'std', 'var'
        # }

        # ALLOWED_NODE_TYPES = {
        #     ast.Lambda, ast.Name, ast.Attribute, ast.Call, 
        #     ast.BinOp, ast.UnaryOp, ast.Constant, 
        #     ast.Load, ast.Store, ast.arguments, ast.arg
        # }

        # ALLOWED_BINARY_OPERATORS = {
        #     ast.Add, ast.Sub, ast.Mult, ast.Div, 
        #     ast.FloorDiv, ast.Mod, ast.Pow,
        # }

        # ALLOWED_UNARY_OPERATORS = {
        #     ast.UAdd, ast.USub,
        # }

        # try:
        #     source = inspect.getsource(lambda_func)
        # except OSError:
        #     raise ValueError("Could not retrieve source code for the provided lambda function.")

        # source = textwrap.dedent(source).strip()
        
        # try:
        #     tree = ast.parse(source)
        # except SyntaxError:
        #     raise ValueError("Could not parse the provided lambda function.")

        # lambda_node = None
        # for node in ast.walk(tree):
        #     if isinstance(node, ast.Lambda):
        #         lambda_node = node
        #         break
        
        # if lambda_node is None:
        #     raise ValueError("No lambda function found in the provided source code.")

        # for node in ast.walk(lambda_node):
        #     if type(node) not in ALLOWED_NODE_TYPES:
        #         raise ValueError(f"Security violation: AST node '{type(node).__name__}' is not allowed.")

        #     # B. Validate Binary Operators (if applicable)
        #     if isinstance(node, ast.BinOp):
        #         if type(node.op) not in ALLOWED_BINARY_OPERATORS:
        #             raise ValueError(f"Security violation: Binary operator '{type(node.op).__name__}' is not allowed.")

        #     # C. Validate Unary Operators (if applicable)
        #     if isinstance(node, ast.UnaryOp):
        #         if type(node.op) not in ALLOWED_UNARY_OPERATORS:
        #             raise ValueError(f"Security violation: Unary operator '{type(node.op).__name__}' is not allowed.")

        #     # D. Validate Function/Method Calls
        #     if isinstance(node, ast.Call):
        #         # Case 1: Method call (e.g., x.sum())
        #         if isinstance(node.func, ast.Attribute):
        #             if node.func.attr not in ALLOWED_METHODS:
        #                 raise ValueError(f"Security violation: Method '{node.func.attr}' is not allowed.")
                
        #         # Case 2: Direct function call (e.g., sum(x)) - Optional, depending on your needs.
        #         # If you only want to allow x.sum(), you might reject ast.Name here.
        #         elif isinstance(node.func, ast.Name):
        #             if node.func.id not in ALLOWED_METHODS:
        #                 raise ValueError(f"Security violation: Function '{node.func.id}' is not allowed.")

        # return lambda_func
