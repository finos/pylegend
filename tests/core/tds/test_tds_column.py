# Copyright 2023 Goldman Sachs
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

import pytest
from pylegend.core.tds.tds_column import (
    tds_columns_from_json,
    PrimitiveTdsColumn,
    PrimitiveType,
    EnumTdsColumn,
    tds_columns_from_csv_string
)
import pandas as pd


class TestTdsColumn:

    def test_primitive_tds_column_creation(self) -> None:
        c1 = PrimitiveTdsColumn('C1', PrimitiveType.Integer)
        assert "TdsColumn(Name: C1, Type: Integer)" == str(c1)

        c2 = PrimitiveTdsColumn('C2', PrimitiveType.Float)
        assert "TdsColumn(Name: C2, Type: Float)" == str(c2)

        c3 = PrimitiveTdsColumn('C3', PrimitiveType.String)
        assert "TdsColumn(Name: C3, Type: String)" == str(c3)

        c4 = PrimitiveTdsColumn('C4', PrimitiveType.Boolean)
        assert "TdsColumn(Name: C4, Type: Boolean)" == str(c4)

        c5 = PrimitiveTdsColumn('C5', PrimitiveType.Number)
        assert "TdsColumn(Name: C5, Type: Number)" == str(c5)

    def test_primitive_tds_column_creation_with_class_methods(self) -> None:
        c1 = PrimitiveTdsColumn.integer_column('C1')
        assert "TdsColumn(Name: C1, Type: Integer)" == str(c1)

        c2 = PrimitiveTdsColumn.float_column('C2')
        assert "TdsColumn(Name: C2, Type: Float)" == str(c2)

        c3 = PrimitiveTdsColumn.string_column('C3')
        assert "TdsColumn(Name: C3, Type: String)" == str(c3)

        c4 = PrimitiveTdsColumn.boolean_column('C4')
        assert "TdsColumn(Name: C4, Type: Boolean)" == str(c4)

        c5 = PrimitiveTdsColumn.number_column('C5')
        assert "TdsColumn(Name: C5, Type: Number)" == str(c5)

    def test_primitive_tds_column_copy(self) -> None:
        c1 = PrimitiveTdsColumn.integer_column('C1')
        c1_copy = c1.copy()
        assert c1 != c1_copy
        assert "TdsColumn(Name: C1, Type: Integer)" == str(c1)
        assert "TdsColumn(Name: C1, Type: Integer)" == str(c1_copy)

    def test_primitive_tds_column_copy_with_name_change(self) -> None:
        c1 = PrimitiveTdsColumn.integer_column('C1')
        c1_copy = c1.copy_with_changed_name('C1_Copy')
        assert "TdsColumn(Name: C1, Type: Integer)" == str(c1)
        assert "TdsColumn(Name: C1_Copy, Type: Integer)" == str(c1_copy)

    def test_enum_tds_column_creation(self) -> None:
        c1 = EnumTdsColumn('C1', 'my::EnumType', ['A', 'B'])
        assert "TdsColumn(Name: C1, Type: my::EnumType)" == str(c1)

    def test_enum_tds_column_copy(self) -> None:
        c1 = EnumTdsColumn('C1', 'my::EnumType', ['A', 'B'])
        c1_copy = c1.copy()
        assert c1 != c1_copy
        assert "TdsColumn(Name: C1, Type: my::EnumType)" == str(c1)
        assert "TdsColumn(Name: C1, Type: my::EnumType)" == str(c1_copy)

    def test_enum_tds_column_copy_with_changed_name(self) -> None:
        c1 = EnumTdsColumn('C1', 'my::EnumType', ['A', 'B'])
        c1_copy = c1.copy_with_changed_name('C1_Copy')
        assert "TdsColumn(Name: C1, Type: my::EnumType)" == str(c1)
        assert "TdsColumn(Name: C1_Copy, Type: my::EnumType)" == str(c1_copy)

    def test_tds_columns_from_json(self) -> None:

        s = """{
            "columns": []
        }"""
        assert tds_columns_from_json(s) == []

        s = """{
            "columns":
                {
                    "_type": "primitiveSchemaColumn",
                    "type": "String",
                    "name": "First Name"
                }
        }"""
        assert ", ".join([str(x) for x in tds_columns_from_json(s)]) == "TdsColumn(Name: First Name, Type: String)"

        s = """{
            "columns": [
                {
                    "_type": "primitiveSchemaColumn",
                    "type": "String",
                    "name": "First Name"
                }
            ]
        }"""
        assert ", ".join([str(x) for x in tds_columns_from_json(s)]) == "TdsColumn(Name: First Name, Type: String)"

        s = """{
            "columns": [
                {
                    "_type": "primitiveSchemaColumn",
                    "type": "String",
                    "name": "First Name"
                },
                {
                    "_type": "primitiveSchemaColumn",
                    "type": "String",
                    "name": "Last Name"
                }
            ]
        }"""
        assert ", ".join([str(x) for x in tds_columns_from_json(s)]) == \
               "TdsColumn(Name: First Name, Type: String), TdsColumn(Name: Last Name, Type: String)"

        s = """{
            "columns": [
                {
                    "_type": "enumSchemaColumn",
                    "type": "my::EnumType",
                    "name": "CityType"
                }
            ],
            "enums": {
                "type": "my::EnumType",
                "values": ["Tier1", "Tier2"]
            }
        }"""
        assert ", ".join([str(x) for x in tds_columns_from_json(s)]) == \
               "TdsColumn(Name: CityType, Type: my::EnumType)"

        s = """{
            "columns": [
                {
                    "_type": "enumSchemaColumn",
                    "type": "my::EnumType",
                    "name": "CityType"
                }
            ],
            "enums": {
                "type": "my::EnumType",
                "values": "Tier1"
            }
        }"""
        assert ", ".join([str(x) for x in tds_columns_from_json(s)]) == \
               "TdsColumn(Name: CityType, Type: my::EnumType)"

        s = """{
            "columns": [
                {
                    "_type": "enumSchemaColumn",
                    "type": "my::EnumType1",
                    "name": "CityType1"
                },
                {
                    "_type": "enumSchemaColumn",
                    "type": "my::EnumType2",
                    "name": "CityType2"
                }
            ],
            "enums": [
                {
                    "type": "my::EnumType1",
                    "values": ["Tier1", "Tier2"]
                },
                {
                    "type": "my::EnumType2",
                    "values": ["Tier1", "Tier2"]
                }
            ]
        }"""
        assert ", ".join([str(x) for x in tds_columns_from_json(s)]) == \
               "TdsColumn(Name: CityType1, Type: my::EnumType1), TdsColumn(Name: CityType2, Type: my::EnumType2)"

    def test_tds_columns_from_json_error_messages(self) -> None:
        with pytest.raises(RuntimeError) as r:
            tds_columns_from_json(" --- ")
        assert "Unable to parse tds columns from schema: \n --- " in r.value.args[0]

        with pytest.raises(RuntimeError) as r:
            s = """{
                "columns": [
                    {
                        "_type": "enumSchemaColumn",
                        "type": "my::EnumType",
                        "name": "CityType"
                    }
                ]
            }"""
            tds_columns_from_json(s)

        assert "Unable to parse tds columns from schema: " in r.value.args[0]
        assert "Unknown enum type: my::EnumType" in r.value.args[1].args[0]

    def test_tds_columns_from_csv_string(self) -> None:
        with pytest.raises(
                pd.errors.EmptyDataError,
                match="No columns to parse from file"):
            tds_columns_from_csv_string("")

        csv_string = (
            "id,name,is_active,created_at,grp\n"
            "1,A,True,2025-12-16 10:30:45,1.0\n"
            "3,B,False,2025-12-17 10:30:45,2.0"
        )

        columns = tds_columns_from_csv_string(
            csv_string,
            datetime_columns=["created_at"]
        )

        assert len(columns) == 5

        expected = [
            ("id", PrimitiveType.Integer.name),
            ("name", PrimitiveType.String.name),
            ("is_active", PrimitiveType.Boolean.name),
            ("created_at", PrimitiveType.DateTime.name),
            ("grp", PrimitiveType.Float.name),
        ]

        for col, (name, type_) in zip(columns, expected):
            assert col.get_name() == name
            assert col.get_type() == type_
