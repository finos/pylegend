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

from pylegend.core.request.service_client import (
    ServiceClient,
    RequestMethod
)
from pylegend.core.request.response_reader import ResponseReader
from pylegend.core.request.auth import AuthScheme, LocalhostEmptyAuthScheme
from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
)
from pylegend.core.tds.tds_column import TdsColumn, tds_columns_from_json


__all__: PyLegendSequence[str] = [
    "LegendClient",
]


class LegendClient(ServiceClient):
    def __init__(
            self,
            host: str,
            port: int,
            auth_scheme: AuthScheme = LocalhostEmptyAuthScheme(),
            secure_http: bool = True,
            retry_count: int = 2
    ) -> None:
        super().__init__(
            host=host, port=port, auth_scheme=auth_scheme, secure_http=secure_http, retry_count=retry_count
        )

    def get_sql_string_schema(
            self,
            sql: str
    ) -> PyLegendSequence[TdsColumn]:
        response = super()._execute_service(
            method=RequestMethod.POST,
            path="api/sql/v1/execution/getSchemaFromQueryString",
            data=sql,
            headers={"Content-Type": "text/plain"},
            stream=False
        )
        response_text: str = response.text
        return tds_columns_from_json(response_text)

    def execute_sql_string(
            self,
            sql: str,
            chunk_size: PyLegendOptional[int] = None
    ) -> ResponseReader:
        iter_content = super()._execute_service(
            method=RequestMethod.POST,
            path="api/sql/v1/execution/executeQueryString",
            data=sql,
            headers={"Content-Type": "text/plain"},
            stream=True
        ).iter_content(chunk_size=chunk_size)
        return ResponseReader(iter_content)

    def __eq__(self, other: 'object') -> bool:
        if self is other:
            return True
        if isinstance(other, LegendClient):
            return self.get_host() == other.get_host() and self.get_port() == other.get_port()
        return False
