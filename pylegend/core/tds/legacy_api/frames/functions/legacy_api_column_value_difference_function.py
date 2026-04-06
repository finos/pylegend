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

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendCallable,
    PyLegendUnion,
    PyLegendOptional,
)
from pylegend.core.tds.legacy_api.frames.legacy_api_applied_function_tds_frame import LegacyApiAppliedFunction
from pylegend.core.sql.metamodel import (
    QuerySpecification,
)
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legacy_api.frames.legacy_api_base_tds_frame import LegacyApiBaseTdsFrame
from pylegend.core.tds.legacy_api.frames.legacy_api_tds_frame import LegacyApiTdsFrame
from pylegend.core.language import (
    LegacyApiTdsRow,
    PyLegendBoolean,
    PyLegendPrimitiveOrPythonPrimitive,
)

__all__: PyLegendSequence[str] = [
    "LegacyApiColumnValueDifferenceFunction"
]


class LegacyApiColumnValueDifferenceFunction(LegacyApiAppliedFunction):
    __base_frame: LegacyApiBaseTdsFrame
    __other_frame: LegacyApiBaseTdsFrame
    __self_join_columns: PyLegendList[str]
    __other_join_columns: PyLegendList[str]
    __columns_to_check: PyLegendList[str]
    __composed_frame: PyLegendOptional[LegacyApiBaseTdsFrame]

    @classmethod
    def name(cls) -> str:
        return "column_value_difference"

    def __init__(
            self,
            base_frame: LegacyApiBaseTdsFrame,
            other_frame: LegacyApiTdsFrame,
            self_join_columns: PyLegendList[str],
            other_join_columns: PyLegendList[str],
            columns_to_check: PyLegendList[str]
    ) -> None:
        self.__base_frame = base_frame
        if not isinstance(other_frame, LegacyApiBaseTdsFrame):
            raise ValueError("Expected LegacyApiBaseTdsFrame")  # pragma: no cover
        self.__other_frame = other_frame
        self.__self_join_columns = self_join_columns
        self.__other_join_columns = other_join_columns
        self.__columns_to_check = columns_to_check
        self.__composed_frame = None

    def __get_composed_frame(self) -> LegacyApiBaseTdsFrame:
        if self.__composed_frame is None:
            self.__composed_frame = self.__build_composed_frame()
        return self.__composed_frame

    def __build_composed_frame(self) -> LegacyApiBaseTdsFrame:
        tds1 = self.__base_frame
        tds2 = self.__other_frame
        columns_to_check = self.__columns_to_check
        self_join_columns = self.__self_join_columns
        other_join_columns = self.__other_join_columns

        cols_1 = [vc + '_1' for vc in columns_to_check]
        cols_2 = [vc + '_2' for vc in columns_to_check]

        self_restrict_cols = list(dict.fromkeys(self_join_columns + columns_to_check))
        other_restrict_cols = list(dict.fromkeys(other_join_columns + columns_to_check))
        tds1_renamed = tds1.restrict(self_restrict_cols).rename_columns(columns_to_check, cols_1)
        tds2_renamed = tds2.restrict(other_restrict_cols).rename_columns(columns_to_check, cols_2)

        diff_col_names = [vc + '_valueDifference' for vc in columns_to_check]

        def _make_value_diff_func(
                col_name: str
        ) -> PyLegendCallable[[LegacyApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]:
            return lambda r: r[col_name + '_1'].is_null().case(
                -r[col_name + '_2'],  # type: ignore[operator]
                r[col_name + '_2'].is_null().case(
                    r[col_name + '_1'],
                    r[col_name + '_1'] - r[col_name + '_2']  # type: ignore[operator]
                )
            )

        def _build_extend_functions(
        ) -> PyLegendList[PyLegendCallable[[LegacyApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]]:
            funcs: PyLegendList[PyLegendCallable[[LegacyApiTdsRow], PyLegendPrimitiveOrPythonPrimitive]] = []
            for vc in columns_to_check:
                funcs.append(_make_value_diff_func(vc))
            return funcs

        def _all_not_null(r: LegacyApiTdsRow) -> PyLegendUnion[bool, PyLegendBoolean]:
            result: PyLegendUnion[bool, PyLegendBoolean] = r[cols_1[0]].is_not_null()
            for c in cols_1[1:]:
                result = result & r[c].is_not_null()
            return result

        def _all_null(r: LegacyApiTdsRow) -> PyLegendUnion[bool, PyLegendBoolean]:
            result: PyLegendUnion[bool, PyLegendBoolean] = r[cols_1[0]].is_null()
            for c in cols_1[1:]:
                result = result & r[c].is_null()
            return result

        all_join_cols = list(dict.fromkeys(self_join_columns + other_join_columns))
        check_col_triples: PyLegendList[str] = []
        for vc in columns_to_check:
            check_col_triples.extend([vc + '_1', vc + '_2', vc + '_valueDifference'])
        final_cols = all_join_cols + check_col_triples

        left_part = (
            tds1_renamed
            .join_by_columns(tds2_renamed, self_join_columns, other_join_columns, 'LEFT_OUTER')
            .filter(_all_not_null)
            .extend(_build_extend_functions(), diff_col_names)
            .restrict(final_cols)
        )

        right_part = (
            tds1_renamed
            .join_by_columns(tds2_renamed, self_join_columns, other_join_columns, 'RIGHT_OUTER')
            .filter(_all_null)
            .extend(_build_extend_functions(), diff_col_names)
            .restrict(final_cols)
        )

        result = left_part.concatenate(right_part)
        if not isinstance(result, LegacyApiBaseTdsFrame):
            raise ValueError("Expected LegacyApiBaseTdsFrame")  # pragma: no cover
        return result

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        return self.__get_composed_frame().to_sql_query_object(config)

    def to_pure(self, config: FrameToPureConfig) -> str:
        return self.__get_composed_frame().to_pure(config)

    def base_frame(self) -> LegacyApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["LegacyApiBaseTdsFrame"]:
        return [self.__other_frame]

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return self.__get_composed_frame().columns()

    def validate(self) -> bool:
        self_join_columns = self.__self_join_columns
        other_join_columns = self.__other_join_columns
        columns_to_check = self.__columns_to_check

        # Validate other is a proper TdsFrame
        if not isinstance(self.__other_frame, LegacyApiBaseTdsFrame):
            raise TypeError('"other" parameter can only be another TdsFrame')

        # Validate list-of-strings params
        self.__validate_list_of_strings(self_join_columns, "self_join_columns")
        self.__validate_list_of_strings(other_join_columns, "other_join_columns")
        self.__validate_list_of_strings(columns_to_check, "columns_to_check", allow_empty=False)

        # Validate join column list lengths match
        if len(self_join_columns) != len(other_join_columns):
            raise ValueError(
                "self_join_columns and other_join_columns should be of the same size"
            )

        tds1_column_names = [c.get_name() for c in self.__base_frame.columns()]
        tds2_column_names = [c.get_name() for c in self.__other_frame.columns()]

        # Validate join columns exist in respective frames
        for col in self_join_columns:
            if col not in tds1_column_names:
                raise RuntimeError(
                    f"Join column: '{col}' not found in self. "
                    f"Available columns: {', '.join(tds1_column_names)}"
                )
        for col in other_join_columns:
            if col not in tds2_column_names:
                raise RuntimeError(
                    f"Join column: '{col}' not found in other. "
                    f"Available columns: {', '.join(tds2_column_names)}"
                )

        # Validate difference columns exist in both frames
        for col in columns_to_check:
            if col not in tds1_column_names:
                raise RuntimeError(
                    f"Difference column: '{col}' not found in self. "
                    f"Available columns: {', '.join(tds1_column_names)}"
                )
            if col not in tds2_column_names:
                raise RuntimeError(
                    f"Difference column: '{col}' not found in other. "
                    f"Available columns: {', '.join(tds2_column_names)}"
                )

        # Check for duplicate final column names
        all_join_cols = list(dict.fromkeys(self_join_columns + other_join_columns))
        check_col_triples: PyLegendList[str] = []
        for vc in columns_to_check:
            check_col_triples.extend([vc + '_1', vc + '_2', vc + '_valueDifference'])
        final_col_names = all_join_cols + check_col_triples
        if len(final_col_names) != len(set(final_col_names)):
            raise RuntimeError(
                "Duplicate column names in column difference not supported.\n"
                f"Self columns: {', '.join(tds1_column_names)}\n"
                f"Other columns: {', '.join(tds2_column_names)}\n"
                f"Difference columns: {', '.join(columns_to_check)}"
            )

        return True

    @staticmethod
    def __validate_list_of_strings(param: object, param_name: str, allow_empty: bool = True) -> None:
        if not isinstance(param, list) or not all(isinstance(x, str) for x in param):
            raise TypeError(
                f"{param_name} parameter must be a list of strings. Got: {type(param).__name__}"
            )
        if not allow_empty and len(param) == 0:
            raise ValueError(f"{param_name} parameter should be a non-empty list.")
