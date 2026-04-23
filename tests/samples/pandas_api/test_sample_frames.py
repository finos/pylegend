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

import json
import pytest
import sys
from pylegend.samples.pandas_api import northwind_orders_frame


@pytest.mark.skipif(sys.platform == "win32", reason="Not supported on windows")
def test_northwind_orders_frame() -> None:
    frame = northwind_orders_frame()
    frame = frame[["Order Id"]].head(5)  # type: ignore[union-attr]
    expected = {'columns': ['Order Id'],
                'rows': [{'values': [10248]},
                         {'values': [10249]},
                         {'values': [10250]},
                         {'values': [10251]},
                         {'values': [10252]}]}
    res = frame.execute_frame_to_string()
    assert json.loads(res)["result"] == expected


@pytest.mark.skip(reason="Legend engine takes DECIMAL(5, 2) as default instead of DECIMAL(10, 2)")
@pytest.mark.skipif(sys.platform == "win32", reason="Not supported on windows")
def test_decimal_collection_parse_decimal_precision() -> None:
    frame = northwind_orders_frame()
    frame["id_dec"] = frame["Order Id"].to_string().parse_decimal(10, 2)  # type: ignore
    result = frame.groupby("Ship Name")["id_dec"].aggregate(
        lambda x: x.max()
    ).to_pandas().head(3)
    assert len(result) == 3
