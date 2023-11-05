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
from pylegend import (
    HeaderTokenAuthScheme,
    CookieAuthScheme,
)
from pylegend.core.request.service_client import ServiceClient, RequestMethod


class TestHeaderCopySession:

    def send(self, request, stream):  # type: ignore
        response = requests.Response()
        response.url, response.headers = request.url, request.headers
        response.status_code, response._content = 200, bytes("OK", "utf-8")
        return response

    def mount(self, prefix, adapter) -> None:  # type: ignore
        pass


class TestHeaderTokenAuth:

    @staticmethod
    def setup_method() -> None:
        mockito.when(requests).Session().thenReturn(TestHeaderCopySession())

    @staticmethod
    def teardown_method() -> None:
        mockito.unstub()

    def test_header_token_auth(self) -> None:
        def token_provider() -> str:
            return 'TEST-AUTH-TOKEN'

        client = ServiceClient(
            host="localhost",
            port=80,
            secure_http=False,
            path_prefix=None,
            auth_scheme=HeaderTokenAuthScheme(header_name="TEST-AUTH-TOKEN-HEADER-NAME", token_provider=token_provider),
            retry_count=1
        )
        response = client._execute_service(method=RequestMethod.GET, path="path")
        assert response.url == "http://localhost:80/path"
        assert response.text == "OK"
        assert str(response.headers) == "{'TEST-AUTH-TOKEN-HEADER-NAME': 'TEST-AUTH-TOKEN'}"

    def test_header_token_auth_with_query_params(self) -> None:
        def token_provider() -> str:
            return 'TEST-AUTH-TOKEN'

        client = ServiceClient(
            host="localhost",
            port=80,
            secure_http=False,
            path_prefix=None,
            auth_scheme=HeaderTokenAuthScheme(
                header_name="TEST-AUTH-TOKEN-HEADER-NAME",
                token_provider=token_provider,
                query_params={"auth_client": "token_auth"}
            ),
            retry_count=1
        )
        response = client._execute_service(method=RequestMethod.GET, path="path")
        assert response.url == "http://localhost:80/path?auth_client=token_auth"
        assert response.text == "OK"
        assert str(response.headers) == "{'TEST-AUTH-TOKEN-HEADER-NAME': 'TEST-AUTH-TOKEN'}"


class TestCookieAuth:

    @staticmethod
    def setup_method() -> None:
        mockito.when(requests).Session().thenReturn(TestHeaderCopySession())

    @staticmethod
    def teardown_method() -> None:
        mockito.unstub()

    def test_cookie_auth(self) -> None:
        def cookie_provider() -> str:
            return 'TEST-SSO'

        client = ServiceClient(
            host="localhost",
            port=80,
            secure_http=False,
            path_prefix=None,
            auth_scheme=CookieAuthScheme(cookie_name="LegendSSO", cookie_provider=cookie_provider),
            retry_count=1
        )
        response = client._execute_service(method=RequestMethod.GET, path="path")
        assert response.url == "http://localhost:80/path"
        assert response.text == "OK"
        assert str(response.headers) == "{'Cookie': 'LegendSSO=TEST-SSO'}"

    def test_cookie_auth_with_query_params(self) -> None:
        def cookie_provider() -> str:
            return 'TEST-SSO'

        client = ServiceClient(
            host="localhost",
            port=80,
            secure_http=False,
            path_prefix=None,
            auth_scheme=CookieAuthScheme(
                cookie_name="LegendSSO",
                cookie_provider=cookie_provider,
                query_params={'auth_client': 'cookie_auth'}
            ),
            retry_count=1
        )
        response = client._execute_service(method=RequestMethod.GET, path="path")
        assert response.url == "http://localhost:80/path?auth_client=cookie_auth"
        assert response.text == "OK"
        assert str(response.headers) == "{'Cookie': 'LegendSSO=TEST-SSO'}"

    def test_cookie_auth_with_extra_params_non_matching_domain(self) -> None:
        def cookie_provider() -> str:
            return 'TEST-SSO'

        client = ServiceClient(
            host="engine.test.domain.com",
            port=80,
            secure_http=False,
            path_prefix=None,
            auth_scheme=CookieAuthScheme(
                cookie_name="LegendSSO",
                cookie_provider=cookie_provider,
                query_params={'auth_client': 'cookie_auth'},
                domain=".test.other.domain.com",
                path="/path"
            ),
            retry_count=1
        )
        response = client._execute_service(method=RequestMethod.GET, path="path")
        assert response.url == "http://engine.test.domain.com:80/path?auth_client=cookie_auth"
        assert response.text == "OK"
        assert str(response.headers) == "{}"

    def test_cookie_auth_with_extra_params_matching_domain(self) -> None:
        def cookie_provider() -> str:
            return 'TEST-SSO'

        client = ServiceClient(
            host="engine.test.domain.com",
            port=80,
            secure_http=False,
            path_prefix=None,
            auth_scheme=CookieAuthScheme(
                cookie_name="LegendSSO",
                cookie_provider=cookie_provider,
                query_params={'auth_client': 'cookie_auth'},
                domain=".test.domain.com",
                path="/path"
            ),
            retry_count=1
        )
        response = client._execute_service(method=RequestMethod.GET, path="path")
        assert response.url == "http://engine.test.domain.com:80/path?auth_client=cookie_auth"
        assert response.text == "OK"
        assert str(response.headers) == "{'Cookie': 'LegendSSO=TEST-SSO'}"
