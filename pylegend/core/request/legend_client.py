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


class LegendClient(ServiceClient):
    def __init__(
            self,
            host: str,
            port: int,
            secure_http: bool = True,
            retry_count: int = 3
    ) -> None:
        super().__init__(host=host, port=port, secure_http=secure_http, retry_count=retry_count)

    def get_sql_schema(
            self,
            sql: str
    ) -> str:
        response = super()._execute_service(
            method=RequestMethod.POST,
            path="sql/v1/execution/getSchemaFromQueryString/PROD-123",
            data=sql,
            headers={"Content-Type": "text/plain"},
            stream=False
        )
        response_text: str = response.text
        return response_text
