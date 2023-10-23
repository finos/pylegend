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

import time
from pylegend.core.request.response_reader import ResponseReader
from pylegend.extensions.tds.legend_api.frames.legend_api_table_spec_input_frame import LegendApiTableSpecInputFrame
from pylegend.core.tds.result_handler import ToStringResultHandler
from pylegend._typing import PyLegendIterator


class TestToStringResultHandler:

    def test_to_string_result_handler_non_lazy(self) -> None:
        handler = ToStringResultHandler()
        const = "<<RESULT>>"
        bytes_iter = [bytes(const, "utf-8")].__iter__()
        res = handler.handle_result(LegendApiTableSpecInputFrame(["dummy"], []), ResponseReader(bytes_iter))
        assert const == res

    def test_to_string_result_handler_lazy(self) -> None:
        def gen() -> PyLegendIterator[bytes]:
            i = 0
            while i < 10:
                yield bytes(str(i), "utf-8")
                i += 1
                time.sleep(0.1)

        handler = ToStringResultHandler()
        res = handler.handle_result(LegendApiTableSpecInputFrame(["dummy"], []), ResponseReader(gen()))
        assert "0123456789" == res
