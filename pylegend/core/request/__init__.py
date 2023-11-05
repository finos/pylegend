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

from pylegend._typing import (
    PyLegendSequence,
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.request.auth import (
    AuthScheme,
    LocalhostEmptyAuthScheme,
    HeaderTokenAuthScheme,
    CookieAuthScheme,
)
from pylegend.core.request.response_reader import ResponseReader

__all__: PyLegendSequence[str] = [
    "LegendClient",

    "AuthScheme",
    "LocalhostEmptyAuthScheme",
    "HeaderTokenAuthScheme",
    "CookieAuthScheme",

    "ResponseReader"
]
