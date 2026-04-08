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
    PyLegendCallable,
    PyLegendDict,
    PyLegendList,
    PyLegendOptional,
    PyLegendSequence,
)
import inspect
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiPartialFrame,
    PandasApiWindow,
    PandasApiWindowReference,
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.primitive_collection import PyLegendPrimitiveCollection
from pylegend.core.language.shared.primitives.primitive import (
    PyLegendPrimitive,
    PyLegendPrimitiveOrPythonPrimitive,
)
from pylegend.core.language.shared.column_expressions import PyLegendColumnExpression
from pylegend.core.language.shared.helpers import escape_column_name, generate_pure_lambda
from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.sql.metamodel import (
    Expression,
    IntegerLiteral,
    QualifiedName,
    QualifiedNameReference,
    QuerySpecification,
    SelectItem,
    SingleColumn,
)
from pylegend.core.sql.metamodel_extension import WindowExpression
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
    PandasApiAppliedFunction,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame, ZERO_COLUMN_NAME
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.abstract.function_helpers import tds_column_for_primitive

# Type alias for the p,w,r-mapper lambda: (p, w, r) -> primitive
PwrFunc = PyLegendCallable[
    [PandasApiPartialFrame, PandasApiWindowReference, PandasApiTdsRow],
    PyLegendPrimitiveOrPythonPrimitive,
]

# Type alias for the optional aggregation lambda: (collection) -> primitive
AggFunc = PyLegendCallable[
    [PyLegendPrimitiveCollection],
    PyLegendPrimitive,
]


class SingleColumnWindowFunction(PandasApiAppliedFunction):

    __base_window_frame: PandasApiWindowTdsFrame
    __pwr_func: PwrFunc
    __agg_func: PyLegendOptional[AggFunc]
    __window: PandasApiWindow

    @classmethod
    def name(cls) -> str:
        return "single_column_window"  # pragma: no cover

    def __init__(
            self,
            base_window_frame: PandasApiWindowTdsFrame,
            pwr_func: PwrFunc,
            agg_func: PyLegendOptional[AggFunc] = None,
    ) -> None:
        self.__base_window_frame = base_window_frame
        self.__pwr_func = pwr_func
        self.__agg_func = agg_func

        self.__window = self.__base_window_frame.construct_window()

    # ──────────────────────────────────────────────────────────────────────
    # PandasApiAppliedFunction interface
    # ──────────────────────────────────────────────────────────────────────

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_window_frame.base_frame()

    def tds_frame_parameters(self) -> PyLegendList[PandasApiBaseTdsFrame]:
        return []

    def calculate_columns(self) -> PyLegendSequence[TdsColumn]:
        tds_row = PandasApiTdsRow.from_tds_frame("r", self.base_frame())
        partial_frame = PandasApiPartialFrame(base_frame=self.base_frame(), var_name="p")
        window_ref = PandasApiWindowReference(window=self.__window, var_name="w")

        result = self.__pwr_func(partial_frame, window_ref, tds_row)

        def _apply_agg(
                primitive: PyLegendPrimitiveOrPythonPrimitive,
        ) -> PyLegendPrimitiveOrPythonPrimitive:
            if self.__agg_func is None:
                return primitive
            from pylegend.core.language.shared.primitive_collection import create_primitive_collection
            return self.__agg_func(create_primitive_collection(primitive))

        # If pwr_func returned a TdsRow (e.g. p.first(w, r)), expand to all columns
        if isinstance(result, PandasApiTdsRow):
            columns: PyLegendList[TdsColumn] = []
            for col in self.base_frame().columns():
                col_result = _apply_agg(result[col.get_name()])
                columns.append(tds_column_for_primitive(col.get_name(), col_result))
            return columns

        # pwr_func returned a single primitive (e.g. p.first(w, r)["col"])
        result = _apply_agg(result)

        # Derive the column name from the underlying column expression when possible.
        col_name = self.__infer_column_name(result)
        return [tds_column_for_primitive(col_name, result)]

    @staticmethod
    def __infer_column_name(result: PyLegendPrimitiveOrPythonPrimitive) -> str:
        """Try to extract the column name from a primitive's underlying expression."""
        if isinstance(result, PyLegendPrimitive):
            expr = result.value()
            if isinstance(expr, PyLegendColumnExpression):
                return expr.get_column()
        return "__result__"  # pragma: no cover

    def validate(self) -> bool:
        # 1. base_window_frame must be a PandasApiWindowTdsFrame
        if not isinstance(self.__base_window_frame, PandasApiWindowTdsFrame):
            raise TypeError(
                f"base_window_frame must be a PandasApiWindowTdsFrame, "
                f"got: {type(self.__base_window_frame).__name__}"
            )

        # 2. pwr_func must be callable with exactly 3 parameters
        if not callable(self.__pwr_func):
            raise TypeError(
                f"pwr_func must be callable, got: {type(self.__pwr_func).__name__}"
            )
        pwr_sig = inspect.signature(self.__pwr_func)
        pwr_params = [
            p for p in pwr_sig.parameters.values()
            if p.default is inspect.Parameter.empty
            and p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        ]
        if len(pwr_params) != 3:
            raise TypeError(
                f"pwr_func must accept exactly 3 positional parameters "
                f"(PandasApiPartialFrame, PandasApiWindowReference, PandasApiTdsRow), "
                f"got {len(pwr_params)} required parameter(s)"
            )

        # 3. agg_func, if provided, must be callable with exactly 1 parameter
        if self.__agg_func is not None:
            if not callable(self.__agg_func):
                raise TypeError(
                    f"agg_func must be callable or None, got: {type(self.__agg_func).__name__}"
                )
            agg_sig = inspect.signature(self.__agg_func)
            agg_params = [
                p for p in agg_sig.parameters.values()
                if p.default is inspect.Parameter.empty
                and p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
            ]
            if len(agg_params) != 1:
                raise TypeError(
                    f"agg_func must accept exactly 1 positional parameter "
                    f"(PyLegendPrimitiveCollection), "
                    f"got {len(agg_params)} required parameter(s)"
                )

        return True

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        temp_column_name_suffix = "__pylegend_olap_column__"

        base_query = self.base_frame().to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()

        # 1. Add the zero column to the base query
        base_query.select.selectItems.append(
            SingleColumn(
                alias=db_extension.quote_identifier(ZERO_COLUMN_NAME),
                expression=IntegerLiteral(0),
            )
        )

        # 2. Wrap in a sub-query so the zero column is accessible
        new_query: QuerySpecification = create_sub_query(base_query, config, "root")

        # 3. Build the window (with zero column in partition)
        window = self.__base_window_frame.construct_window(include_zero_column=True)

        # 4. Evaluate pwr_func to get the column expression(s)
        tds_row = PandasApiTdsRow.from_tds_frame("root", self.base_frame())
        partial_frame = PandasApiPartialFrame(base_frame=self.base_frame(), var_name="p")
        window_ref = PandasApiWindowReference(window=self.__window, var_name="w")

        pwr_result = self.__pwr_func(partial_frame, window_ref, tds_row)

        # Collect (col_name, sql_expression) pairs
        col_entries: PyLegendList[tuple] = []

        if isinstance(pwr_result, PandasApiTdsRow):
            # pwr_func returned a full row — expand to all base columns
            for col in self.base_frame().columns():
                col_primitive = pwr_result[col.get_name()]
                col_entries.append((col.get_name(), col_primitive))
        else:
            col_name = self.__infer_column_name(pwr_result)
            col_entries.append((col_name, pwr_result))

        # 5. For each column, resolve the SQL expression (with optional agg),
        #    wrap in WindowExpression, and add with a temp alias
        new_select_items: PyLegendList[SelectItem] = []
        for col_name, primitive in col_entries:
            if isinstance(primitive, PyLegendPrimitive):
                col_sql_expr = primitive.to_sql_expression({"root": new_query}, config)
            else:
                col_sql_expr = (
                    convert_literal_to_literal_expression(primitive)
                    .to_sql_expression({"root": new_query}, config)
                )

            if self.__agg_func is not None:
                from pylegend.core.language.shared.primitive_collection import create_primitive_collection
                collection = create_primitive_collection(primitive)
                agg_result = self.__agg_func(collection)
                col_sql_expr = agg_result.to_sql_expression({"root": new_query}, config)

            window_expr = WindowExpression(
                nested=col_sql_expr,
                window=window.to_sql_node(new_query, config),
            )
            new_select_items.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(col_name + temp_column_name_suffix),
                    expression=window_expr,
                )
            )

        new_query.select.selectItems = new_select_items

        # 6. Wrap in an outer query that renames from temp suffix to final alias
        new_query = create_sub_query(new_query, config, "root")
        final_select_items: PyLegendList[SelectItem] = []
        for col_name, _ in col_entries:
            col_expr = QualifiedNameReference(QualifiedName([
                db_extension.quote_identifier("root"),
                db_extension.quote_identifier(col_name + temp_column_name_suffix),
            ]))
            final_select_items.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(col_name),
                    expression=col_expr,
                )
            )
        new_query.select.selectItems = final_select_items

        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix = "__pylegend_olap_column__"

        # 1. Evaluate pwr_func to derive the column entries (same logic as to_sql / calculate_columns)
        tds_row = PandasApiTdsRow.from_tds_frame("r", self.base_frame())
        partial_frame = PandasApiPartialFrame(base_frame=self.base_frame(), var_name="p")
        window_ref = PandasApiWindowReference(window=self.__window, var_name="w")

        pwr_result = self.__pwr_func(partial_frame, window_ref, tds_row)

        # Collect (col_name, mapper_primitive) pairs
        col_entries: PyLegendList[tuple] = []

        if isinstance(pwr_result, PandasApiTdsRow):
            for col in self.base_frame().columns():
                col_entries.append((col.get_name(), pwr_result[col.get_name()]))
        else:
            col_name = self.__infer_column_name(pwr_result)
            col_entries.append((col_name, pwr_result))

        # 2. Build the window expression (with zero column)
        window_with_zero = self.__base_window_frame.construct_window(include_zero_column=True)
        window_expr = window_with_zero.to_pure_expression(config)

        # 3. Build the extend column expressions
        extend_col_expressions: PyLegendList[str] = []
        for col_name, primitive in col_entries:
            if isinstance(primitive, PyLegendPrimitive):
                mapper_expr = primitive.to_pure_expression(config)
            else:
                mapper_expr = (
                    convert_literal_to_literal_expression(primitive)
                    .to_pure_expression(config)
                )

            agg_part = ""
            if self.__agg_func is not None:
                from pylegend.core.language.shared.primitive_collection import create_primitive_collection
                collection = create_primitive_collection(primitive)
                agg_result = self.__agg_func(collection)
                agg_expr = agg_result.to_pure_expression(config).replace(mapper_expr, "$c")
                agg_part = f":{generate_pure_lambda('c', agg_expr)}"

            escaped_col = escape_column_name(col_name + temp_column_name_suffix)
            extend_col_expressions.append(
                f"{escaped_col}:{generate_pure_lambda('p,w,r', mapper_expr)}{agg_part}"
            )

        # 4. Build the extend string
        if len(extend_col_expressions) == 1:
            extend_str = f"->extend({window_expr}, ~{extend_col_expressions[0]})"
        else:
            extend_str = (
                f"->extend({window_expr}, ~[{config.separator(2)}"
                + ("," + config.separator(2, True)).join(extend_col_expressions)
                + f"{config.separator(1)}])"
            )

        # 5. Build the project string that renames from temp suffix to final name
        project_col_expressions = [
            f"{escape_column_name(col_name)}:p|$p.{escape_column_name(col_name + temp_column_name_suffix)}"
            for col_name, _ in col_entries
        ]
        if len(project_col_expressions) == 1:
            project_str = f"->project(~{project_col_expressions[0]})"
        else:
            project_str = (
                f"->project(~[{config.separator(2)}"
                + ("," + config.separator(2, True)).join(project_col_expressions)
                + f"{config.separator(1)}])"
            )

        # 6. Assemble: base_frame -> zero column extend -> window extend -> project
        return (
            self.base_frame().to_pure(config)
            + config.separator(1) + f"->extend(~{escape_column_name(ZERO_COLUMN_NAME)}:{{r|0}})"
            + config.separator(1) + extend_str
            + config.separator(1) + project_str
        )

    # ──────────────────────────────────────────────────────────────────────
    # Series interface
    # ──────────────────────────────────────────────────────────────────────

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig,
    ) -> Expression:
        columns = self.calculate_columns()
        assert len(columns) == 1, (
            "to_sql_expression is only supported for single-column window functions"
        )

        frame_name = list(frame_name_to_base_query_map.keys())[0]
        base_query = frame_name_to_base_query_map[frame_name]

        # Evaluate the pwr_func to get the SQL expression
        tds_row = PandasApiTdsRow.from_tds_frame(frame_name, self.base_frame())
        partial_frame = PandasApiPartialFrame(base_frame=self.base_frame(), var_name="p")
        window_ref = PandasApiWindowReference(window=self.__window, var_name="w")

        result = self.__pwr_func(partial_frame, window_ref, tds_row)

        if isinstance(result, PyLegendPrimitive):
            col_sql_expr = result.to_sql_expression(frame_name_to_base_query_map, config)
        else:
            col_sql_expr = (
                convert_literal_to_literal_expression(result)
                .to_sql_expression(frame_name_to_base_query_map, config)
            )

        if self.__agg_func is not None:
            from pylegend.core.language.shared.primitive_collection import create_primitive_collection
            collection = create_primitive_collection(result)
            agg_result = self.__agg_func(collection)
            col_sql_expr = agg_result.to_sql_expression(frame_name_to_base_query_map, config)

        # Build a local window with the zero column added to partition_by
        # (the base query already has the zero column)
        db_ext = config.sql_to_string_generator().get_db_extension()
        zero_col_alias = db_ext.quote_identifier(ZERO_COLUMN_NAME)
        has_zero_col = any(
            isinstance(si, SingleColumn) and si.alias == zero_col_alias
            for si in base_query.select.selectItems
        )
        if not has_zero_col:
            raise RuntimeError(
                "SingleColumnWindowFunction requires the zero column "
                f"({ZERO_COLUMN_NAME!r}) in the base query, but it was not found."
            )
        window = self.__base_window_frame.construct_window(include_zero_column=True)

        window_node = window.to_sql_node(base_query, config)
        return WindowExpression(nested=col_sql_expr, window=window_node)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix = "__pylegend_olap_column__"
        columns = self.calculate_columns()
        assert len(columns) == 1, (
            "to_pure_expression is only supported for single-column window functions"
        )
        return f"$c.{escape_column_name(columns[0].get_name() + temp_column_name_suffix)}"

    def build_pure_extend_strs(self, temp_column_name_suffix: str, config: FrameToPureConfig) -> PyLegendList[str]:
        result: PyLegendList[str] = []

        # 1. Always prepend the zero column extend
        result.append(f"->extend(~{escape_column_name(ZERO_COLUMN_NAME)}:{{r|0}})")

        # 2. Evaluate the pwr_func to get the mapper Pure expression
        tds_row = PandasApiTdsRow.from_tds_frame("r", self.base_frame())
        partial_frame = PandasApiPartialFrame(base_frame=self.base_frame(), var_name="p")
        window_ref = PandasApiWindowReference(window=self.__window, var_name="w")

        pwr_result = self.__pwr_func(partial_frame, window_ref, tds_row)

        if isinstance(pwr_result, PyLegendPrimitive):
            mapper_expr = pwr_result.to_pure_expression(config)
        else:
            mapper_expr = (
                convert_literal_to_literal_expression(pwr_result)
                .to_pure_expression(config)
            )

        # 3. Build the agg part if present
        agg_part = ""
        if self.__agg_func is not None:
            from pylegend.core.language.shared.primitive_collection import create_primitive_collection
            collection = create_primitive_collection(pwr_result)
            agg_result = self.__agg_func(collection)
            agg_expr = agg_result.to_pure_expression(config).replace(mapper_expr, "$c")
            agg_part = f":{generate_pure_lambda('c', agg_expr)}"

        # 4. Derive the column name
        columns = self.calculate_columns()
        assert len(columns) == 1
        col_name = columns[0].get_name()

        # 5. Build the window expression
        window_with_zero = self.__base_window_frame.construct_window(include_zero_column=True)
        window_expr = window_with_zero.to_pure_expression(config)

        escaped_col = escape_column_name(col_name + temp_column_name_suffix)
        extend = (
            f"->extend({window_expr}, "
            f"~{escaped_col}:{generate_pure_lambda('p,w,r', mapper_expr)}{agg_part})"
        )
        result.append(extend)
        return result
