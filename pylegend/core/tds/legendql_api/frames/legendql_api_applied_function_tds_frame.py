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

from abc import ABCMeta, abstractmethod
from pylegend._typing import (
    PyLegendSequence,
)
from pylegend.core.tds.abstract.frames.applied_function_tds_frame import AppliedFunction, AppliedFunctionTdsFrame
from pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame import LegendQLApiBaseTdsFrame


__all__: PyLegendSequence[str] = [
    "LegendQLApiAppliedFunctionTdsFrame",
    "LegendQLApiAppliedFunction",
]


class LegendQLApiAppliedFunction(AppliedFunction, metaclass=ABCMeta):
    @abstractmethod
    def tds_frame_parameters(self) -> PyLegendSequence["LegendQLApiBaseTdsFrame"]:
        pass  # pragma: no cover


class LegendQLApiAppliedFunctionTdsFrame(LegendQLApiBaseTdsFrame, AppliedFunctionTdsFrame):
    def __init__(self, applied_function: LegendQLApiAppliedFunction):
        AppliedFunctionTdsFrame.__init__(self, applied_function)
