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

from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.abstract.frames.base_tds_frame import BaseTdsFrame
from pylegend.extensions.tds.abstract.csv_tds_frame import CsvInputFrameAbstract
from pylegend.core.tds.abstract.frames.input_tds_frame import InputTdsFrame
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveType
from pylegend.core.tds.tds_frame import FrameToPureConfig, PyLegendTdsFrame
from pylegend._typing import PyLegendList, PyLegendUnion
from pylegend.extensions.tds.abstract.table_spec_input_frame import TableSpecInputFrameAbstract


def _quote_if_necessary(col: str) -> str:
    return col if col.isidentifier() else f'"{col}"'


def generate_pure_query_and_compile(
        frame: PyLegendTdsFrame,
        config: FrameToPureConfig,
        legend_client: LegendClient
) -> str:
    assert isinstance(frame, BaseTdsFrame)
    tds_frames = frame.get_all_tds_frames()
    input_frames = [x for x in tds_frames if isinstance(x, InputTdsFrame)]
    table_input_frames: PyLegendList[PyLegendUnion[TableSpecInputFrameAbstract, CsvInputFrameAbstract]] = []
    for x in input_frames:
        assert isinstance(x, (TableSpecInputFrameAbstract, CsvInputFrameAbstract))
        table_input_frames.append(x)

    input_text_replacement_map = {}
    db_code = ""
    for i, x in enumerate(input_frames):
        columns_text = ", ".join([f"{_quote_if_necessary(c.get_name())} {__to_relation_type(c)}" for c in x.columns()])
        db_code += f"\nTable test_table_{str(i + 1)} ({columns_text})\n"
        input_text_replacement_map[x.to_pure_query(config)] = "#>{test::DB.test_table_" + str(i + 1) + "}#"
    db_code = f"###Relational\nDatabase test::DB({db_code})\n\n"

    expr = frame.to_pure(config)
    expr_with_table_references = expr
    for (k, v) in input_text_replacement_map.items():
        expr_with_table_references = expr_with_table_references.replace(k, v)

    model_code = db_code + "###Pure\nfunction test::testFunc(): Any[*] {" + expr_with_table_references + "}\n"
    legend_client.parse_and_compile_model(model_code)
    return expr


def __to_relation_type(c: TdsColumn) -> str:
    type_map = {
        PrimitiveType.Integer: "INTEGER",
        PrimitiveType.Float: "FLOAT",
        PrimitiveType.Number: "NUMERIC(20, 6)",
        PrimitiveType.Decimal: "NUMERIC(20, 6)",
        PrimitiveType.String: "VARCHAR(200)",
        PrimitiveType.Boolean: "BOOLEAN",
        PrimitiveType.StrictDate: "DATE",
        PrimitiveType.DateTime: "TIMESTAMP",
        PrimitiveType.Date: "TIMESTAMP",
    }
    return type_map[PrimitiveType[c.get_type()]]
