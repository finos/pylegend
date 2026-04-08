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

import numpy as np
import collections.abc
from pylegend._typing import (
    PyLegendCallable,
    PyLegendList,
    PyLegendMapping,
    PyLegendSequence,
    PyLegendTuple,
    PyLegendUnion,
)
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import (
    PyLegendAggFunc,
    PyLegendAggInput,
    PyLegendAggList,
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
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
from pylegend.core.tds.tds_column import PrimitiveTdsColumn, TdsColumn
from pylegend.core.tds.tds_frame import PyLegendTdsFrame


__all__: PyLegendSequence[str] = [
    "AggregateEntry",
    "build_aggregates_list",
    "normalize_func_to_dict",
    "normalize_agg_func_to_callable",
    "generate_column_alias",
    "infer_column_from_primitive",
]


# (alias, map_result, agg_result)
AggregateEntry = PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def build_aggregates_list(
    frame_name: str,
    base_frame: PyLegendTdsFrame,
    func: PyLegendAggInput,
    axis: PyLegendUnion[int, str],
    args: PyLegendSequence[PyLegendPrimitiveOrPythonPrimitive],
    kwargs: PyLegendMapping[str, PyLegendPrimitiveOrPythonPrimitive],
    group_col_names: PyLegendList[str],
    validation_columns: PyLegendList[str],
    default_broadcast_columns: PyLegendList[str],
) -> PyLegendList[AggregateEntry]:
    """
    Build the list of (alias, map_result, agg_result) tuples that describe
    each aggregate column.

    Parameters
    ----------
    frame_name:
        The name to use when constructing a TdsRow (e.g. "r" for standalone
        aggregate queries, or any other name for reuse in other contexts).
    base_frame:
        The base frame (unwrapped from groupby if applicable) used to
        construct a TdsRow for column access.
    func:
        The user-supplied aggregation specification.
    axis:
        Must be 0 or "index".
    args / kwargs:
        Extra positional / keyword arguments (currently must be empty).
    group_col_names:
        Names of grouping columns (empty list if not a groupby aggregate).
    validation_columns:
        Column names that are valid keys in a dict-style func input.
    default_broadcast_columns:
        Column names to broadcast a scalar/list func input across.
    """
    _validate_axis(axis)
    _validate_no_extra_args(args, kwargs)

    normalized_func = normalize_func_to_dict(
        func,
        validation_columns=validation_columns,
        default_broadcast_columns=default_broadcast_columns,
        group_col_names=set(group_col_names),
    )

    tds_row = PandasApiTdsRow.from_tds_frame(frame_name, base_frame)

    group_cols_set = set(group_col_names)
    aggregates: PyLegendList[AggregateEntry] = []

    for column_name, agg_input in normalized_func.items():
        map_result = tds_row[column_name]
        collection = create_primitive_collection(map_result)

        if isinstance(agg_input, list):
            _process_list_agg_input(
                column_name, agg_input, collection, map_result, aggregates
            )
        else:
            _process_scalar_agg_input(
                column_name, agg_input, collection, map_result, group_cols_set, aggregates
            )

    return aggregates


def normalize_func_to_dict(
    func_input: PyLegendAggInput,
    validation_columns: PyLegendList[str],
    default_broadcast_columns: PyLegendList[str],
    group_col_names: set[str],
) -> dict[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]]:
    """
    Normalize any form of user-supplied aggregation input (str, callable,
    list, dict) into a canonical ``{column_name: func_or_list}`` dictionary.
    """
    if isinstance(func_input, collections.abc.Mapping):
        return _normalize_mapping_input(
            func_input,  # type: ignore[arg-type]  # keys validated as str inside _normalize_mapping_input
            validation_columns,
            group_col_names,
        )

    if isinstance(func_input, collections.abc.Sequence) and not isinstance(func_input, str):
        _validate_list_elements(func_input)
        return {col: func_input for col in default_broadcast_columns}

    if callable(func_input) or isinstance(func_input, str) or isinstance(func_input, np.ufunc):
        return {col: func_input for col in default_broadcast_columns}

    raise TypeError(
        "Invalid `func` argument for aggregate function. "
        "Expected a callable, str, np.ufunc, a list containing exactly one of these, "
        "or a mapping[str -> callable/str/ufunc/a list containing exactly one of these]. "
        f"But got: {func_input!r} (type: {type(func_input).__name__})"
    )


def normalize_agg_func_to_callable(
    func: PyLegendAggFunc,
) -> PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]:
    """
    Convert a single aggregation specifier (str, np.ufunc, or callable) into a
    callable that takes a ``PyLegendPrimitiveCollection`` and returns a
    ``PyLegendPrimitive``.
    """
    if isinstance(func, str):
        return _resolve_string_func(func)

    if isinstance(func, np.ufunc):
        return _resolve_numpy_func(func)

    # Named callable (e.g. ``len``, ``sum`` built-in)
    func_name = getattr(func, "__name__", "").lower()
    if func_name in _FLATTENED_FUNCTION_MAPPING and func_name != "<lambda>":
        internal = _FLATTENED_FUNCTION_MAPPING[func_name]
        resolved: PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive] = eval(
            f"lambda x: x.{internal}()"
        )
        return resolved

    # Custom / anonymous callable — wrap with a type check
    def _validation_wrapper(x: PyLegendPrimitiveCollection) -> PyLegendPrimitive:
        result = func(x)
        if not isinstance(result, PyLegendPrimitive):
            raise TypeError(
                f"Custom aggregation function must return a PyLegendPrimitive (Expression).\n"
                f"But got type: {type(result).__name__}\n"
                f"Value: {result!r}"
            )  # pragma: no cover
        return result

    return _validation_wrapper


def generate_column_alias(col_name: str, func: PyLegendAggFunc, lambda_counter: int) -> str:
    """Derive the output column alias for a given aggregation function."""
    if isinstance(func, str):
        return f"{func}({col_name})"

    func_name = getattr(func, "__name__", "<lambda>")
    if func_name != "<lambda>":
        return f"{func_name}({col_name})"
    else:
        return f"lambda_{lambda_counter}({col_name})"


def infer_column_from_primitive(name: str, expr: PyLegendPrimitive) -> TdsColumn:
    """Infer the ``TdsColumn`` type for an aggregate result expression."""
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
    elif isinstance(expr, PyLegendDateTime):  # pragma: no cover
        return PrimitiveTdsColumn.datetime_column(name)  # pragma: no cover
    elif isinstance(expr, PyLegendStrictDate):  # pragma: no cover
        return PrimitiveTdsColumn.strictdate_column(name)  # pragma: no cover
    else:
        raise TypeError(
            f"Could not infer TdsColumn type for aggregation result type: {type(expr)}"
        )  # pragma: no cover


# ──────────────────────────────────────────────────────────────────────────────
# Internal constants
# ──────────────────────────────────────────────────────────────────────────────

_PYTHON_TO_LEGEND_FUNCTION_MAPPING: PyLegendMapping[str, PyLegendList[str]] = {
    "average": ["mean", "average", "nanmean"],
    "sum": ["sum", "nansum"],
    "min": ["min", "amin", "minimum", "nanmin"],
    "max": ["max", "amax", "maximum", "nanmax"],
    "std_dev_sample": ["std", "std_dev", "nanstd", "std_dev_sample"],
    "std_dev_population": ["std_dev_population"],
    "variance_sample": ["var", "variance", "nanvar", "variance_sample"],
    "variance_population": ["variance_population", "var_population"],
    "median": ["median"],
    "mode": ["mode"],
    "count": ["count", "size", "len", "length"],
}

_FLATTENED_FUNCTION_MAPPING: dict[str, str] = {}
for _target, _aliases in _PYTHON_TO_LEGEND_FUNCTION_MAPPING.items():
    for _alias in _aliases:
        _FLATTENED_FUNCTION_MAPPING[_alias] = _target


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers — validation
# ──────────────────────────────────────────────────────────────────────────────

def _validate_axis(axis: PyLegendUnion[int, str]) -> None:
    if axis not in [0, "index"]:
        raise NotImplementedError(
            f"The 'axis' parameter of the aggregate function must be 0 or 'index', but got: {axis}"
        )


def _validate_no_extra_args(
    args: PyLegendSequence[PyLegendPrimitiveOrPythonPrimitive],
    kwargs: PyLegendMapping[str, PyLegendPrimitiveOrPythonPrimitive],
) -> None:
    if len(args) > 0 or len(kwargs) > 0:
        raise NotImplementedError(
            "AggregateFunction currently does not support additional positional "
            "or keyword arguments. Please remove extra *args/**kwargs."
        )


def _validate_list_elements(items: PyLegendSequence[PyLegendAggFunc]) -> None:
    for i, f in enumerate(items):
        if not (callable(f) or isinstance(f, str) or isinstance(f, np.ufunc)):
            raise TypeError(
                f"Invalid `func` argument for the aggregate function.\n"
                f"When a list is provided as the main argument, all elements must be callable, str, or np.ufunc.\n"
                f"But got element at index {i}: {f!r} (type: {type(f).__name__})\n"
            )


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers — normalize_func_to_dict sub-routines
# ──────────────────────────────────────────────────────────────────────────────

def _normalize_mapping_input(
    func_input: PyLegendMapping[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]],
    validation_columns: PyLegendList[str],
    group_col_names: set[str],
) -> dict[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]]:
    normalized: dict[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]] = {}

    for key, value in func_input.items():
        _validate_mapping_key(key, validation_columns)

        if isinstance(value, collections.abc.Sequence) and not isinstance(value, str):
            _validate_mapping_list_value(key, value)
            normalized[key] = value
        else:
            _validate_mapping_scalar_value(key, value)
            if key in group_col_names:
                normalized[key] = [value]
            else:
                normalized[key] = value

    return normalized


def _validate_mapping_key(key: object, validation_columns: PyLegendList[str]) -> None:
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


def _validate_mapping_list_value(key: str, value: PyLegendSequence[PyLegendAggFunc]) -> None:
    for i, f in enumerate(value):
        if not (callable(f) or isinstance(f, str) or isinstance(f, np.ufunc)):
            raise TypeError(
                f"Invalid `func` argument for the aggregate function.\n"
                f"When a list is provided for a column, all elements must be callable, str, or np.ufunc.\n"
                f"But got element at index {i}: {f!r} (type: {type(f).__name__})\n"
            )


def _validate_mapping_scalar_value(key: str, value: object) -> None:
    if not (callable(value) or isinstance(value, str) or isinstance(value, np.ufunc)):
        raise TypeError(
            f"Invalid `func` argument for the aggregate function.\n"
            f"When a dictionary is provided, the value must be a callable, str, or np.ufunc "
            f"(or a list containing these).\n"
            f"But got value for key '{key}': {value} (type: {type(value).__name__})\n"
        )


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers — normalize_agg_func_to_callable sub-routines
# ──────────────────────────────────────────────────────────────────────────────

def _resolve_string_func(
    func: str,
) -> PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]:
    func_lower = func.lower()
    if func_lower not in _FLATTENED_FUNCTION_MAPPING:
        raise NotImplementedError(
            f"Invalid `func` argument for the aggregate function.\n"
            f"The string {func!r} does not correspond to any supported aggregation.\n"
            f"Available string functions are: {sorted(_FLATTENED_FUNCTION_MAPPING.keys())}"
        )  # pragma: no cover
    internal = _FLATTENED_FUNCTION_MAPPING[func_lower]
    resolved: PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive] = eval(
        f"lambda x: x.{internal}()"
    )
    return resolved


def _resolve_numpy_func(
    func: np.ufunc,
) -> PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]:
    func_name = func.__name__
    if func_name not in _FLATTENED_FUNCTION_MAPPING:
        raise NotImplementedError(
            f"Invalid `func` argument for the aggregate function.\n"
            f"The NumPy function {func_name!r} is not supported.\n"
            f"Supported aggregate functions are: {sorted(_FLATTENED_FUNCTION_MAPPING.keys())}"
        )  # pragma: no cover
    internal = _FLATTENED_FUNCTION_MAPPING[func_name]
    resolved: PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive] = eval(
        f"lambda x: x.{internal}()"
    )
    return resolved


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers — build_aggregates_list sub-routines
# ──────────────────────────────────────────────────────────────────────────────

def _process_list_agg_input(
    column_name: str,
    agg_input: PyLegendList[PyLegendAggFunc],
    collection: PyLegendPrimitiveCollection,
    map_result: PyLegendPrimitiveOrPythonPrimitive,
    aggregates: PyLegendList[AggregateEntry],
) -> None:
    """Process a list-style aggregation input for a single column."""
    lambda_counter = 0
    for func in agg_input:
        is_anonymous_lambda = (
            not isinstance(func, str)
            and getattr(func, "__name__", "<lambda>") == "<lambda>"
        )
        if is_anonymous_lambda:
            lambda_counter += 1

        agg_callable = normalize_agg_func_to_callable(func)
        agg_result = agg_callable(collection)

        alias = generate_column_alias(column_name, func, lambda_counter)
        aggregates.append((alias, map_result, agg_result))


def _process_scalar_agg_input(
    column_name: str,
    agg_input: PyLegendAggFunc,
    collection: PyLegendPrimitiveCollection,
    map_result: PyLegendPrimitiveOrPythonPrimitive,
    group_cols: set[str],
    aggregates: PyLegendList[AggregateEntry],
) -> None:
    """Process a single (non-list) aggregation input for a single column."""
    agg_callable = normalize_agg_func_to_callable(agg_input)
    agg_result = agg_callable(collection)

    if column_name in group_cols:
        alias = generate_column_alias(column_name, agg_input, 0)
    else:
        alias = column_name

    aggregates.append((alias, map_result, agg_result))
