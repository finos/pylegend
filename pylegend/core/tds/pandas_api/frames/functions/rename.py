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
    PyLegendCallable,
    PyLegendDict
)
from pylegend.core.language import (
    PyLegendInteger,
    PyLegendBoolean
)
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SelectItem,
    SingleColumn
)
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.sql_query_helpers import copy_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig


class PandasApiRenameFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __mapper: PyLegendOptional[PyLegendUnion[PyLegendDict[str, str], PyLegendCallable[[str], str]]]
    __axis: PyLegendUnion[str, int, PyLegendInteger]
    __index: PyLegendOptional[PyLegendUnion[PyLegendDict[str, str], PyLegendCallable[[str], str]]]
    __columns: PyLegendOptional[PyLegendUnion[PyLegendDict[str, str], PyLegendCallable[[str], str]]]
    __level: PyLegendOptional[PyLegendUnion[int, PyLegendInteger, str]]
    __inplace: PyLegendUnion[bool, PyLegendBoolean]
    __copy: PyLegendUnion[bool, PyLegendBoolean]
    __errors: str

    @classmethod
    def name(cls) -> str:
        return "rename"  # pragma: no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            mapper: PyLegendOptional[PyLegendUnion[PyLegendDict[str, str], PyLegendCallable[[str], str]]],
            axis: PyLegendUnion[str, int, PyLegendInteger],
            index: PyLegendOptional[PyLegendUnion[PyLegendDict[str, str], PyLegendCallable[[str], str]]],
            columns: PyLegendOptional[PyLegendUnion[PyLegendDict[str, str], PyLegendCallable[[str], str]]],
            level: PyLegendOptional[PyLegendUnion[int, PyLegendInteger, str]],
            inplace: PyLegendUnion[bool, PyLegendBoolean],
            errors: str,
            copy: PyLegendUnion[bool, PyLegendBoolean]
    ) -> None:
        self.__base_frame = base_frame
        self.__mapper = mapper
        self.__axis = axis
        self.__index = index
        self.__columns = columns
        self.__level = level
        self.__inplace = inplace
        self.__errors = errors
        self.__copy = copy

    def __resolve_columns_mapping(self) -> PyLegendDict[str, str]:
        base_cols = [c.get_name() for c in self.__base_frame.columns()]
        mapping_source = None

        axis_is_columns = (self.__axis == 1 or self.__axis == "columns")
        axis_is_index = (self.__axis == 0 or self.__axis == "index")

        if axis_is_index:
            raise NotImplementedError("Renaming index is not supported yet in Pandas API")

        # Priority: explicit columns=, else mapper when axis targets columns
        if self.__columns is not None:
            mapping_source = self.__columns
        elif self.__mapper is not None and axis_is_columns:
            mapping_source = self.__mapper
        elif self.__index is not None:
            raise NotImplementedError("Index mapper is not supported yet in Pandas API")

        if mapping_source is None:
            return {}

        if not callable(mapping_source) and not isinstance(mapping_source, dict):
            raise TypeError(
                f"Rename mapping must be a dict or a callable, got {type(mapping_source)}"
            )

        out: PyLegendDict[str, str] = {}
        if callable(mapping_source):
            func = mapping_source
            for col in base_cols:
                new = func(col)
                if not isinstance(new, str):
                    raise TypeError(
                        f"Rename function must return str, got {type(new)} for column {col}")  # pragma: no cover
                if new != col:
                    out[col] = new
        else:
            # dict-like
            dict_map: PyLegendDict[str, str] = mapping_source
            if self.__errors == "raise":
                missing = [k for k in dict_map.keys() if k not in base_cols]
                if missing:
                    raise KeyError(f"{missing} not found in axis")

            for k, v in dict_map.items():
                if k in base_cols and k != v:
                    out[k] = v

        return out

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        rename_map = self.__resolve_columns_mapping()
        base_query = self.__base_frame.to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()

        # Prepare quoted lookup for aliases
        quoted_from = [db_extension.quote_identifier(s) for s in rename_map.keys()]
        quoted_to = [db_extension.quote_identifier(rename_map[s]) for s in rename_map.keys()]

        new_select_items: PyLegendList[SelectItem] = []
        for col in base_query.select.selectItems:
            if not isinstance(col, SingleColumn):
                raise ValueError("Rename operation not supported for non-SingleColumn select items")
            if col.alias is None:
                raise ValueError("Rename operation requires SingleColumn items with aliases")
            if col.alias in quoted_from:
                new_alias = quoted_to[quoted_from.index(col.alias)]
                new_select_items.append(SingleColumn(alias=new_alias, expression=col.expression))
            else:
                new_select_items.append(col)

        new_query = copy_query(base_query)
        new_query.select.selectItems = new_select_items
        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        rename_map = self.__resolve_columns_mapping()
        base_pure = self.__base_frame.to_pure(config)

        # Build a single project that aliases columns to new names
        project_items: PyLegendList[str] = []
        for c in self.__base_frame.columns():
            orig = c.get_name()
            new = rename_map.get(orig, orig)
            project_items.append(f"{new}:x|$x.{orig}")

        project_body = ", ".join(project_items)
        return (
            f"{base_pure}{config.separator(1)}"
            f"->project({config.separator(2)}~[{project_body}]{config.separator(1)})"
        )

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        rename_map = self.__resolve_columns_mapping()
        new_cols = []
        for c in self.__base_frame.columns():
            name = c.get_name()
            if name in rename_map:
                new_cols.append(c.copy_with_changed_name(rename_map[name]))
            else:
                new_cols.append(c.copy())
        names = [c.get_name() for c in new_cols]
        if len(names) != len(set(names)):
            raise ValueError("Resulting columns contain duplicates after rename")
        return new_cols

    def validate(self) -> bool:
        if self.__level is not None:
            raise NotImplementedError("level parameter not supported yet in Pandas API")

        if not isinstance(self.__inplace, bool):
            raise TypeError(f"inplace must be bool. Got {type(self.__inplace)}")
        if self.__inplace is True:
            raise NotImplementedError("inplace=True not supported yet in Pandas API")

        if not isinstance(self.__copy, bool):
            raise TypeError(f"copy must be bool. Got {type(self.__copy)}")
        if self.__copy is False:
            raise NotImplementedError("copy=False not supported yet in Pandas API")

        if self.__errors not in ("ignore", "raise"):
            raise ValueError(f"errors must be 'ignore' or 'raise'. Got {self.__errors}")

        # axis validation
        if self.__axis not in (1, "columns", 0, "index"):
            raise ValueError(f"Unsupported axis {self.__axis}")
        if self.__axis in (0, "index"):
            raise NotImplementedError("Renaming index not supported yet in Pandas API")

        # index
        if self.__index is not None:
            raise NotImplementedError("Index mapper not supported yet in Pandas API")

        # conflict validation
        if self.__mapper and self.__columns:
            raise ValueError("Cannot specify both 'axis' and any of 'index' or 'columns'")

        self.__resolve_columns_mapping()  # runs validation
        return True
