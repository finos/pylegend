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
    PyLegendSequence,
    PyLegendOptional,
    PyLegendList,
    PyLegendUnion,
)
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiPartialFrame,
    PandasApiWindow,
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.helpers import escape_column_name, generate_pure_lambda
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SelectItem,
    SingleColumn,
    QualifiedName,
    QualifiedNameReference,
    ComparisonExpression,
    ComparisonOperator,
    IntegerLiteral,
)
from pylegend.core.sql.metamodel_extension import WindowExpression
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
    PandasApiAppliedFunction,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import (
    PandasApiBaseTdsFrame,
)
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig

__all__: PyLegendSequence[str] = ["DropDuplicatesFunction"]


class DropDuplicatesFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __subset: PyLegendOptional[PyLegendUnion[str, PyLegendList[str]]]
    __keep: str
    __inplace: bool
    __ignore_index: bool

    __subset_list: PyLegendList[str]

    @classmethod
    def name(cls) -> str:
        return "drop_duplicates"  # pragma: no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            subset: PyLegendOptional[PyLegendUnion[str, PyLegendList[str]]],
            keep: str,
            inplace: bool,
            ignore_index: bool
    ) -> None:
        self.__base_frame = base_frame
        self.__subset = subset
        self.__keep = keep
        self.__inplace = inplace
        self.__ignore_index = ignore_index
        self.__subset_list = []

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        temp_col = "__INTERNAL_PYLEGEND_ROW_NUM__"
        db_extension = config.sql_to_string_generator().get_db_extension()

        base_query = self.__base_frame.to_sql_query_object(config)

        inner_query = create_sub_query(base_query, config, "root")

        tds_row = PandasApiTdsRow.from_tds_frame("r", self.__base_frame)
        partial_frame = PandasApiPartialFrame(base_frame=self.__base_frame, var_name="p")

        partition_by = self.__subset_list
        window = PandasApiWindow(partition_by=partition_by, order_by=None, frame=None)

        row_number_expr = partial_frame.row_number(tds_row)
        row_number_sql = row_number_expr.to_sql_expression({"r": inner_query}, config)

        window_expr = WindowExpression(
            nested=row_number_sql,
            window=window.to_sql_node(inner_query, config),
        )

        inner_query.select.selectItems.append(
            SingleColumn(
                alias=db_extension.quote_identifier(temp_col),
                expression=window_expr
            )
        )

        outer_query = create_sub_query(inner_query, config, "root")

        temp_col_ref = QualifiedNameReference(
            name=QualifiedName(parts=[
                db_extension.quote_identifier("root"),
                db_extension.quote_identifier(temp_col)
            ])
        )
        outer_query.where = ComparisonExpression(
            left=temp_col_ref,
            right=IntegerLiteral(value=1),
            operator=ComparisonOperator.EQUAL
        )

        final_query = create_sub_query(outer_query, config, "root")
        final_select_items: PyLegendList[SelectItem] = []
        for col in self.__base_frame.columns():
            col_name = col.get_name()
            col_expr = QualifiedNameReference(QualifiedName([
                db_extension.quote_identifier("root"),
                db_extension.quote_identifier(col_name)
            ]))
            final_select_items.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(col_name),
                    expression=col_expr
                )
            )
        final_query.select.selectItems = final_select_items

        return final_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        temp_col = "__INTERNAL_PYLEGEND_ROW_NUM__"
        base_pure = self.__base_frame.to_pure(config)

        # Window expression
        partition_str = (
            "" if not self.__subset_list
            else "~[" + (", ".join(map(escape_column_name, self.__subset_list))) + "], "
        )
        window_str = f"over({partition_str}[])"

        # Extend with row_number
        extend_str = (
            f"->extend({window_str}, "
            f"~{escape_column_name(temp_col)}:"
            f"{generate_pure_lambda('p,w,r', '$p->rowNumber($r)')})"
        )

        # Filter rn == 1
        filter_str = f"->filter(c|$c.{escape_column_name(temp_col)} == 1)"

        # Project back to original columns
        project_cols = [
            f"{escape_column_name(c.get_name())}:p|$p.{escape_column_name(c.get_name())}"
            for c in self.__base_frame.columns()
        ]
        project_str = f"->project(~[{', '.join(project_cols)}])"

        return (
            f"{base_pure}{config.separator(1)}"
            f"{extend_str}{config.separator(1)}"
            f"{filter_str}{config.separator(1)}"
            f"{project_str}"
        )

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        return [c.copy() for c in self.__base_frame.columns()]

    def validate(self) -> bool:
        if self.__keep != 'first':
            raise NotImplementedError(
                f"keep='{self.__keep}' is not supported yet in Pandas API drop_duplicates. "
                f"Only keep='first' is supported."
            )

        if self.__inplace:
            raise NotImplementedError(
                "inplace=True is not supported yet in Pandas API drop_duplicates"
            )

        if self.__ignore_index:
            raise NotImplementedError(
                "ignore_index=True is not supported yet in Pandas API drop_duplicates"
            )

        # Normalize subset
        if self.__subset is None:
            self.__subset_list = [c.get_name() for c in self.__base_frame.columns()]
        elif isinstance(self.__subset, str):
            self.__subset_list = [self.__subset]
        elif isinstance(self.__subset, (list, tuple, set)):
            self.__subset_list = list(self.__subset)
        else:
            raise TypeError(
                f"subset must be a column label or list of column labels, "
                f"but got {type(self.__subset)}"
            )

        # Validate subset columns exist
        valid_cols = {c.get_name() for c in self.__base_frame.columns()}
        invalid_cols = [s for s in self.__subset_list if s not in valid_cols]
        if invalid_cols:
            raise KeyError(f"{invalid_cols}")

        return True
