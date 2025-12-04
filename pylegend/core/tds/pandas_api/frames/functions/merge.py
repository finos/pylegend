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

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendUnion,
    PyLegendOptional,
    PyLegendTuple,
)
from pylegend.core.language import (
    PyLegendBoolean,
    PyLegendBooleanLiteralExpression,
    PyLegendPrimitive,
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.helpers import generate_pure_lambda
from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    Select,
    SelectItem,
    SingleColumn,
    AliasedRelation,
    TableSubquery,
    Query,
    Join,
    JoinType,
    JoinOn,
    QualifiedNameReference,
    QualifiedName,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query, extract_columns_for_subquery
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig

__all__: PyLegendSequence[str] = [
    "PandasApiMergeFunction"
]


class PandasApiMergeFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __other_frame: PandasApiBaseTdsFrame
    __how: PyLegendOptional[str]
    __on: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]]
    __left_on: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]]
    __right_on: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]]
    __left_index: PyLegendOptional[bool]
    __right_index: PyLegendOptional[bool]
    __sort: PyLegendOptional[bool]
    __suffixes: PyLegendOptional[
        PyLegendUnion[
            PyLegendTuple[PyLegendUnion[str, None], PyLegendUnion[str, None]],
            PyLegendList[PyLegendUnion[str, None]],
        ]
    ]
    __indicator: PyLegendOptional[PyLegendUnion[bool, str]]
    __validate: PyLegendOptional[str]

    @classmethod
    def name(cls) -> str:
        return "merge"  # pragma: no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            other_frame: PandasApiBaseTdsFrame,
            how: PyLegendOptional[str],
            on: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]],
            left_on: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]],
            right_on: PyLegendOptional[PyLegendUnion[str, PyLegendSequence[str]]],
            left_index: PyLegendOptional[bool],
            right_index: PyLegendOptional[bool],
            sort: PyLegendOptional[bool],
            suffixes: PyLegendOptional[
                PyLegendUnion[
                    PyLegendTuple[PyLegendUnion[str, None], PyLegendUnion[str, None]],
                    PyLegendList[PyLegendUnion[str, None]],
                ]
            ],
            indicator: PyLegendOptional[PyLegendUnion[bool, str]],
            validate: PyLegendOptional[str]
    ) -> None:
        self.__base_frame = base_frame
        self.__other_frame = other_frame
        self.__on = on
        self.__left_on = left_on
        self.__right_on = right_on
        self.__left_index = left_index
        self.__right_index = right_index
        self.__sort = sort
        self.__how = how
        self.__suffixes = suffixes
        self.__indicator = indicator
        self.__validate = validate
        self.__sortkeys = []  # type: PyLegendList[str]

    def get_sort_keys(self) -> PyLegendList[str]:
        return self.__sortkeys

    # Key resolution helpers
    def __normalize_keys(
            self,
            candidate: PyLegendUnion[str, PyLegendSequence[str], None]
    ) -> PyLegendList[str]:
        if candidate is None:
            return []
        return [candidate] if isinstance(candidate, str) else list(candidate)

    def __derive_key_pairs(self) -> PyLegendList[PyLegendTuple[str, str]]:

        if self.__how.lower() == "cross":  # type: ignore
            return []

        left_cols = [c.get_name() for c in self.__base_frame.columns()]
        right_cols = [c.get_name() for c in self.__other_frame.columns()]

        if self.__on is not None and (self.__left_on is not None or self.__right_on is not None):
            raise ValueError('Can only pass argument "on" OR "left_on" and "right_on", not a combination of both.')
        if self.__on is not None:
            on_keys = self.__normalize_keys(self.__on)
            for k in on_keys:
                if k not in left_cols or k not in right_cols:
                    raise KeyError(f"'{k}' not found")
            return [(k, k) for k in on_keys]

        left_keys = self.__normalize_keys(self.__left_on)
        right_keys = self.__normalize_keys(self.__right_on)

        if left_keys or right_keys:
            if len(left_keys) != len(right_keys):
                print("came here")
                raise ValueError("len(right_on) must equal len(left_on)")
            for lk in left_keys:
                if lk not in left_cols:
                    raise KeyError(f"'{lk}' not found")
            for rk in right_keys:
                if rk not in right_cols:
                    raise KeyError(f"'{rk}' not found")

            return list(zip(left_keys, right_keys))

        # Infer intersection by default
        inferred = [c for c in left_cols if c in right_cols]
        return [(k, k) for k in inferred]

    def __normalize_suffixes(self) -> None:
        # Convert None to empty string and coerce to list[str]
        left = self.__suffixes[0] or ""  # type: ignore
        right = self.__suffixes[1] or ""  # type: ignore

        self.__suffixes = [left, right]

    # Internal auto join condition builder (returns PyLegendBoolean expression)
    def __build_condition(self) -> PyLegendBoolean:
        key_pairs = self.__derive_key_pairs()
        left_row = PandasApiTdsRow.from_tds_frame("left", self.__base_frame)
        right_row = PandasApiTdsRow.from_tds_frame("right", self.__other_frame)

        expr = None
        for left_key, right_key in key_pairs:
            part = (left_row[left_key] == right_row[right_key])
            expr = part if expr is None else (expr & part)
        return expr  # type: ignore

    def __join_type(self) -> JoinType:
        how_lower = self.__how.lower()  # type: ignore
        if how_lower == "inner":
            return JoinType.INNER
        if how_lower == "left":
            return JoinType.LEFT
        if how_lower == "right":
            return JoinType.RIGHT
        if how_lower == "outer":
            return JoinType.FULL
        if how_lower == "cross":
            return JoinType.CROSS
        raise ValueError("do not recognize join method " + self.__how)  # type: ignore

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()
        left_query = copy_query(self.__base_frame.to_sql_query_object(config))
        right_query = copy_query(self.__other_frame.to_sql_query_object(config))

        join_criteria = None
        join_type = self.__join_type()
        if join_type != JoinType.CROSS:
            join_condition_expr = self.__build_condition()
            if isinstance(join_condition_expr, bool):
                join_condition_expr = PyLegendBoolean(PyLegendBooleanLiteralExpression(join_condition_expr))  # pragma: no cover  # noqa: E501
            join_sql_expr = join_condition_expr.to_sql_expression(
                {
                    "left": create_sub_query(left_query, config, "left"),
                    "right": create_sub_query(right_query, config, "right"),
                },
                config
            )
            join_criteria = JoinOn(expression=join_sql_expr)

        left_alias = db_extension.quote_identifier("left")
        right_alias = db_extension.quote_identifier("right")

        left_original = {c.get_name(): c for c in self.__base_frame.columns()}
        right_original = {c.get_name(): c for c in self.__other_frame.columns()}
        key_pairs = self.__derive_key_pairs()
        same_name_keys = {left_key for left_key, right_key in key_pairs if left_key == right_key}

        select_items: PyLegendList[SelectItem] = []

        left_cols_set = set(left_original.keys())
        right_cols_set = set(right_original.keys())
        overlapping = (left_cols_set & right_cols_set) - same_name_keys

        # Left select items
        for c in self.__base_frame.columns():
            orig = c.get_name()
            out_name = orig + self.__suffixes[0] if orig in overlapping else orig  # type: ignore
            q_out = db_extension.quote_identifier(out_name)
            q_in = db_extension.quote_identifier(orig)
            select_items.append(
                SingleColumn(q_out, QualifiedNameReference(QualifiedName(parts=[left_alias, q_in])))
            )

        # Right select items
        for c in self.__other_frame.columns():
            orig = c.get_name()
            if orig in same_name_keys:
                continue

            out_name = orig + self.__suffixes[1] if orig in overlapping else orig  # type: ignore
            q_out = db_extension.quote_identifier(out_name)
            q_in = db_extension.quote_identifier(orig)
            select_items.append(
                SingleColumn(q_out, QualifiedNameReference(QualifiedName(parts=[right_alias, q_in])))
            )

        join_spec = QuerySpecification(
            select=Select(selectItems=select_items, distinct=False),
            from_=[
                Join(
                    type_=self.__join_type(),
                    left=AliasedRelation(
                        relation=TableSubquery(Query(queryBody=left_query, limit=None, offset=None, orderBy=[])),
                        alias=left_alias,
                        columnNames=extract_columns_for_subquery(left_query)
                    ),
                    right=AliasedRelation(
                        relation=TableSubquery(Query(queryBody=right_query, limit=None, offset=None, orderBy=[])),
                        alias=right_alias,
                        columnNames=extract_columns_for_subquery(right_query)
                    ),
                    criteria=join_criteria
                )
            ],
            where=None,
            groupBy=[],
            having=None,
            orderBy=[],
            limit=None,
            offset=None
        )

        return create_sub_query(join_spec, config, "root")

    def to_pure(self, config: FrameToPureConfig) -> str:
        how_lower = self.__how.lower()  # type: ignore
        if how_lower == "inner":
            join_kind = "INNER"
        elif how_lower == "left":
            join_kind = "LEFT"
        elif how_lower == "right":
            join_kind = "RIGHT"
        elif how_lower == "outer":
            join_kind = "FULL"
        elif how_lower == "cross":
            join_kind = "INNER"

        # Resolve key pairs
        key_pairs = self.__derive_key_pairs()

        left_cols = [c.get_name() for c in self.__base_frame.columns()]
        right_cols = [c.get_name() for c in self.__other_frame.columns()]

        # Suffix handling for overlapping non-key columns
        s = list(self.__suffixes)  # type: ignore
        left_suf, right_suf = s[0], s[1]

        left_col_set = set(left_cols)
        right_col_set = set(right_cols)
        overlapping = left_col_set & right_col_set

        # Overlapping join keys (same name) need temporary rename on right to allow join
        identical_key_names = {
            lk for (lk, rk) in key_pairs if lk == rk
        }

        left_rename_map = {}
        right_rename_map = {}

        # Non-key overlapping columns get suffixes
        for col in overlapping:
            if col in identical_key_names:
                continue

            left_rename_map[col] = col + left_suf  # type: ignore
            right_rename_map[col] = col + right_suf  # type: ignore

        # Temporary rename for identical key names on right
        temp_right_key_map = {}
        for k in identical_key_names:
            temp_name = k + "__right_key_tmp"
            temp_right_key_map[k] = temp_name

        # Rename
        left_frame = (self.__base_frame.rename(columns=left_rename_map, errors="raise")
                      if left_rename_map else self.__base_frame)

        right_map = {**right_rename_map, **temp_right_key_map} if (right_rename_map or temp_right_key_map) else None
        right_frame = (self.__other_frame.rename(columns=right_map, errors="raise")
                       if right_map else self.__other_frame)

        # Build join condition expression
        if how_lower != "cross":
            left_row = PandasApiTdsRow.from_tds_frame("l", left_frame)
            right_row = PandasApiTdsRow.from_tds_frame("r", right_frame)

            expr = None
            for l_key, r_key in key_pairs:
                l_eff = left_rename_map.get(l_key, l_key)
                r_eff = right_map.get(r_key, r_key)  # type: ignore
                part = (left_row[l_eff] == right_row[r_eff])
                expr = part if expr is None else (expr & part)

            if not isinstance(expr, PyLegendPrimitive):
                expr = convert_literal_to_literal_expression(expr)  # type: ignore  # pragma: no cover
            cond_str = expr.to_pure_expression(config.push_indent(2))  # type: ignore
        else:
            cond_str = "1==1"

        left_pure = left_frame.to_pure(config)  # type: ignore
        right_pure = right_frame.to_pure(config.push_indent(2))  # type: ignore

        join_expr = (
            f"{left_pure}{config.separator(1)}"
            f"->join({config.separator(2)}"
            f"{right_pure},{config.separator(2, True)}"
            f"JoinKind.{join_kind},{config.separator(2, True)}"
            f"{generate_pure_lambda('l, r', cond_str)}{config.separator(1)})"
        )

        # Only project if temporary right key renames exist
        if temp_right_key_map:
            final_cols = []

            for c in left_cols:
                if (c in overlapping and c not in identical_key_names):
                    final_cols.append(c + left_suf)  # type: ignore
                else:
                    final_cols.append(c)

            for c in right_cols:
                if c in temp_right_key_map:
                    continue
                if (c in overlapping and c not in identical_key_names):
                    final_cols.append(c + right_suf)  # type: ignore
                else:
                    final_cols.append(c)

            project_items = [f"{col}:x|$x.{col}" for col in final_cols]
            project_body = ", ".join(project_items)
            join_expr = (
                f"{join_expr}{config.separator(1)}"
                f"->project({config.separator(2)}~[{project_body}]{config.separator(1)})"
            )

        return join_expr

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return [self.__other_frame]

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        key_pairs = self.__derive_key_pairs()
        left_keys_same_name = {left_key for left_key, right_key in key_pairs if left_key == right_key}
        left_cols = [c.get_name() for c in self.__base_frame.columns()]
        right_cols = [c.get_name() for c in self.__other_frame.columns()]

        overlapping = (set(left_cols) & set(right_cols)) - left_keys_same_name

        # Build left columns (apply suffix to overlapping non-key)
        result_cols: PyLegendSequence["TdsColumn"] = []
        for c in self.__base_frame.columns():
            name = c.get_name()
            if name in overlapping:
                result_cols.append(c.copy_with_changed_name(name + self.__suffixes[0]))  # type: ignore
            else:
                result_cols.append(c.copy())  # type: ignore

        # Build right columns (skip same-name keys; apply suffix to overlapping non-key)
        for c in self.__other_frame.columns():
            name = c.get_name()
            if any(name == left_key and left_key == right_key for left_key, right_key in key_pairs):
                continue

            if name in overlapping:
                result_cols.append(c.copy_with_changed_name(name + self.__suffixes[1]))  # type: ignore
            else:
                result_cols.append(c.copy())  # type: ignore

        # Validate no duplicates
        names = [c.get_name() for c in result_cols]
        if len(names) != len(set(names)):
            raise ValueError("Resulting merged columns contain duplicates after suffix application")

        if self.__sort:
            for lk, rk in key_pairs:
                left_out = (lk + self.__suffixes[0]) if (lk in overlapping) else lk  # type: ignore
                self.__sortkeys.append(left_out)

                if rk != lk:
                    right_out = (rk + self.__suffixes[1]) if (rk in overlapping) else rk  # type: ignore
                    # right identical-name keys are skipped
                    self.__sortkeys.append(right_out)

        return result_cols

    def validate(self) -> bool:
        # Frame type validation
        if not isinstance(self.__other_frame, PandasApiTdsFrame):
            raise TypeError(f"Can only merge TdsFrame objects, a {type(self.__other_frame)} was passed")

        # Same frame not supported
        if self.__base_frame is self.__other_frame:
            raise NotImplementedError("Merging the same TdsFrame is not supported yet")

        # how
        if not isinstance(self.__how, str):
            raise TypeError(f"'how' must be str, got {type(self.__how)}")

        if self.__how.lower() == "cross":
            if any(v is not None for v in (self.__on, self.__left_on, self.__right_on)):
                raise ValueError("Can not pass on, right_on, left_on for how='cross'")

        # key parameters: on / left_on / right_on
        def _validate_keys_param(param_name: str, value: PyLegendUnion[str, PyLegendSequence[str], None]) -> None:
            if value is None:
                return
            if isinstance(value, str):
                return
            if isinstance(value, (list, tuple)):
                if not all(isinstance(v, str) for v in value):
                    raise TypeError(f"'{param_name}' must contain only str elements")
                return
            raise TypeError(
                f"Passing '{param_name}' as a {type(value)} is not supported. "
                f"Provide '{param_name}' as a tuple instead."
            )

        _validate_keys_param("on", self.__on)
        _validate_keys_param("left_on", self.__left_on)
        _validate_keys_param("right_on", self.__right_on)

        # Suffix validation
        if not isinstance(self.__suffixes, (tuple, list)):
            raise TypeError(
                f"Passing 'suffixes' as {type(self.__suffixes)}, is not supported. "
                "Provide 'suffixes' as a tuple instead."
            )
        for s in self.__suffixes:
            if s is not None and not isinstance(s, str):
                raise TypeError("'suffixes' elements must be str or None")
        if len(self.__suffixes) != 2:
            raise ValueError("too many values to unpack (expected 2)")

        # Sort
        if self.__sort is not None and not isinstance(self.__sort, (bool, PyLegendBoolean)):
            raise TypeError(f"Sort parameter must be bool, got {type(self.__sort)}")

        # Unsupported parameters
        if self.__left_index or self.__right_index:
            raise NotImplementedError("Merging on index is not supported yet in PandasApi merge function")

        if self.__indicator:
            raise NotImplementedError("Indicator parameter is not supported yet in PandasApi merge function")

        if self.__validate:
            raise NotImplementedError("Validate parameter is not supported yet in PandasApi merge function")

        self.__join_type()  # runs how validation
        self.__normalize_suffixes()  # runs suffixes validation

        key_pairs = self.__derive_key_pairs()  # runs key validations
        if not key_pairs and self.__how.lower() != "cross":
            raise ValueError("No merge keys resolved. Specify 'on' or 'left_on'/'right_on', or ensure common columns.")

        return True
