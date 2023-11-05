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

from abc import ABCMeta, abstractmethod

from requests import PreparedRequest
from requests.cookies import create_cookie

from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
    PyLegendCallable,
    PyLegendDict,
)
from requests.auth import AuthBase

__all__: PyLegendSequence[str] = [
    "AuthScheme",
    "LocalhostEmptyAuthScheme",
    "HeaderTokenAuthScheme",
    "CookieAuthScheme",
]


class AuthScheme(metaclass=ABCMeta):

    @abstractmethod
    def get_auth_base(self) -> PyLegendOptional[AuthBase]:
        pass


class LocalhostEmptyAuthScheme(AuthScheme):

    def get_auth_base(self) -> PyLegendOptional[AuthBase]:
        return None


class HeaderTokenAuth(AuthBase):
    __header_name: str
    __token_provider: PyLegendCallable[[], str]
    __query_params: PyLegendOptional[PyLegendDict[str, str]]

    def __init__(
            self,
            header_name: str,
            token_provider: PyLegendCallable[[], str],
            query_params: PyLegendOptional[PyLegendDict[str, str]] = None
    ) -> None:
        self.__header_name = header_name
        self.__token_provider = token_provider
        self.__query_params = query_params

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        if self.__query_params:
            r.prepare_url(r.url, self.__query_params)
        r.headers[self.__header_name] = self.__token_provider()
        return r


class HeaderTokenAuthScheme(AuthScheme):
    __header_name: str
    __token_provider: PyLegendCallable[[], str]
    __query_params: PyLegendOptional[PyLegendDict[str, str]]

    def __init__(
            self,
            header_name: str,
            token_provider: PyLegendCallable[[], str],
            query_params: PyLegendOptional[PyLegendDict[str, str]] = None
    ) -> None:
        self.__header_name = header_name
        self.__token_provider = token_provider
        self.__query_params = query_params

    def get_auth_base(self) -> PyLegendOptional[AuthBase]:
        return HeaderTokenAuth(self.__header_name, self.__token_provider, self.__query_params)


class CookieAuth(AuthBase):
    __cookie_name: str
    __cookie_provider: PyLegendCallable[[], str]
    __query_params: PyLegendOptional[PyLegendDict[str, str]]

    def __init__(  # type: ignore
            self,
            cookie_name: str,
            cookie_provider: PyLegendCallable[[], str],
            query_params: PyLegendOptional[PyLegendDict[str, str]] = None,
            **extra_cookie_params
    ) -> None:
        self.__cookie_name = cookie_name
        self.__cookie_provider = cookie_provider
        self.__query_params = query_params
        self.__extra_cookie_params = extra_cookie_params

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        if self.__query_params:
            r.prepare_url(r.url, self.__query_params)
        r.headers.pop("Cookie", None)
        new_cookie = create_cookie(  # type: ignore
            name=self.__cookie_name,
            value=self.__cookie_provider(),
            **self.__extra_cookie_params
        )
        r._cookies.set_cookie(new_cookie)  # type: ignore
        r.prepare_cookies(r._cookies)  # type: ignore
        return r


class CookieAuthScheme(AuthScheme):
    __cookie_name: str
    __cookie_provider: PyLegendCallable[[], str]
    __query_params: PyLegendOptional[PyLegendDict[str, str]]

    def __init__(  # type: ignore
            self,
            cookie_name: str,
            cookie_provider: PyLegendCallable[[], str],
            query_params: PyLegendOptional[PyLegendDict[str, str]] = None,
            **extra_cookie_params
    ) -> None:
        self.__cookie_name = cookie_name
        self.__cookie_provider = cookie_provider
        self.__query_params = query_params
        self.__extra_cookie_params = extra_cookie_params

    def get_auth_base(self) -> PyLegendOptional[AuthBase]:
        return CookieAuth(self.__cookie_name, self.__cookie_provider, self.__query_params, **self.__extra_cookie_params)
