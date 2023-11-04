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

import mockito  # type: ignore
import requests
from pylegend.core.request.auth import HeaderTokenAuthScheme
from pylegend.core.request.service_client import ServiceClient, RequestMethod


class TestHeaderCopySession:

    def send(self, request, stream):  # type: ignore
        response = requests.Response()
        response.url, response.headers = request.url, request.headers
        response.status_code, response._content = 200, bytes("OK", "utf-8")
        return response

    def mount(self, prefix, adapter) -> None:  # type: ignore
        pass


class TestAuth:

    @staticmethod
    def setup_method() -> None:
        mockito.when(requests).Session().thenReturn(TestHeaderCopySession())

    @staticmethod
    def teardown_method() -> None:
        mockito.unstub()

    def test_header_token_auth(self) -> None:
        def token_provider() -> str:
            return 'TEST-AUTH-TOKEN'

        client1 = ServiceClient(
            host="localhost",
            port=80,
            secure_http=False,
            path_prefix=None,
            auth_scheme=HeaderTokenAuthScheme(header_name="TEST-AUTH-TOKEN-HEADER-NAME", token_provider=token_provider),
            retry_count=1
        )
        response1 = client1._execute_service(method=RequestMethod.GET, path="path")
        assert response1.url == "http://localhost:80/path"
        assert response1.text == "OK"
        assert str(response1.headers) == "{'TEST-AUTH-TOKEN-HEADER-NAME': 'TEST-AUTH-TOKEN'}"

        client2 = ServiceClient(
            host="localhost",
            port=80,
            secure_http=False,
            path_prefix=None,
            auth_scheme=HeaderTokenAuthScheme(
                header_name="TEST-AUTH-TOKEN-HEADER-NAME",
                token_provider=token_provider,
                query_params={"authClient": "token_auth"}
            ),
            retry_count=1
        )
        response2 = client2._execute_service(method=RequestMethod.GET, path="path")
        assert response2.url == "http://localhost:80/path?authClient=token_auth"
        assert response2.text == "OK"
        assert str(response2.headers) == "{'TEST-AUTH-TOKEN-HEADER-NAME': 'TEST-AUTH-TOKEN'}"
