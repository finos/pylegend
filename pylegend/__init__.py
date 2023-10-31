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
from pylegend.legend_api_tds_client import (
    LegendApiTdsClient,
    legend_api_tds_client,
)
from pylegend.core.request import (
    LegendClient,
    AuthScheme,
    LocalhostEmptyAuthScheme,
    ResponseReader,
)
from pylegend.core.project_cooridnates import (
    VersionedProjectCoordinates,
    PersonalWorkspaceProjectCoordinates,
    GroupWorkspaceProjectCoordinates,
)
from pylegend.core.language import agg


__all__: PyLegendSequence[str] = [
    "__version__",

    "LegendApiTdsClient",
    "legend_api_tds_client",

    "LegendClient",
    "AuthScheme",
    "LocalhostEmptyAuthScheme",
    "ResponseReader",

    "VersionedProjectCoordinates",
    "PersonalWorkspaceProjectCoordinates",
    "GroupWorkspaceProjectCoordinates",

    "agg",
]


import importlib.metadata

__version__ = importlib.metadata.version("pylegend")
