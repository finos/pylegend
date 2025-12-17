
from datetime import date, datetime
from pylegend._typing import (
    PyLegendUnion,
    PyLegendList,
    PyLegendSequence,
    PyLegendTuple,
    PyLegendOptional,
    PyLegendCallable,
)
from pylegend.core.language import PyLegendColumnExpression
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import PandasApiPartialFrame, PandasApiSortDirection, PandasApiSortInfo, PandasApiWindow, PandasApiWindowReference
from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.language.shared.primitive_collection import PyLegendPrimitiveCollection, create_primitive_collection
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive, PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.sql.metamodel import (
    IsNullPredicate,
    NullLiteral,
    QualifiedName,
    QuerySpecification,
    SearchedCaseExpression,
    SingleColumn,
    SortItem,
    SortItemNullOrdering,
    SortItemOrdering,
    WhenClause,
)
from pylegend.core.sql.metamodel_extension import WindowExpression
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
    PandasApiAppliedFunction,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import (
    PandasApiBaseTdsFrame,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import (
    PandasApiGroupbyTdsFrame,
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query


def get_column_expressions_and_windows_tuple() -> PyLegendList[
    PyLegendTuple[
        PyLegendUnion[
            PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive],
            PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]
        ],
        PandasApiWindow
    ]
]:
    column_expressions_and_windows_tuple: PyLegendList[
        PyLegendTuple[
            PyLegendUnion[
                PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive],
                PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]
            ],
            PandasApiWindow
        ]
    ] = []


    