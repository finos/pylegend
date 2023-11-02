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

from abc import ABCMeta
from enum import Enum

import requests
from requests.adapters import HTTPAdapter, Retry
from pylegend.core.request.auth import AuthScheme
from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
    PyLegendDict,
    PyLegendList,
    PyLegendTuple
)


__all__: PyLegendSequence[str] = [
    "ServiceClient",
    "RequestMethod"
]


class RequestMethod(Enum):
    GET = 1,
    POST = 2


class ServiceClient(metaclass=ABCMeta):

    def __init__(
            self,
            host: str,
            port: int,
            secure_http: bool,
            path_prefix: PyLegendOptional[str],
            auth_scheme: AuthScheme,
            retry_count: int
    ) -> None:
        self.__host = host
        self.__port = port
        self.__auth_scheme = auth_scheme
        self.__path_prefix = path_prefix
        self.__secure_http = secure_http
        if retry_count < 1:
            raise ValueError("Retry count should be a number greater than 1. Got " + str(retry_count))
        self.__retry_count = retry_count

    def get_host(self) -> str:
        return self.__host

    def get_port(self) -> int:
        return self.__port

    def _execute_service(
            self,
            method: RequestMethod,
            path: str,
            query_params: PyLegendOptional[PyLegendList[PyLegendTuple[str, str]]] = None,
            data: PyLegendOptional[str] = None,
            headers: PyLegendOptional[PyLegendDict[str, str]] = None,
            stream: bool = True
    ) -> requests.Response:

        scheme = "https" if self.__secure_http else "http"
        prefix = (self.__path_prefix if self.__path_prefix.startswith("/") else f"/{self.__path_prefix}") \
            if self.__path_prefix is not None else ""
        url = f"{scheme}://{self.__host}:{self.__port}{prefix}/{path}"

        request = requests.Request(
            method=method.name,
            url=url,
            headers=headers,
            data=data,
            params=query_params,
            auth=self.__auth_scheme.get_auth_base(),
        )

        session = requests.Session()
        adapter = HTTPAdapter(max_retries=Retry(
            self.__retry_count,
            allowed_methods=["GET", "POST"],
            status_forcelist=[500, 501, 502, 503, 504],
            raise_on_status=False
        ))
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        response = session.send(request.prepare(), stream=stream)

        if not response.ok:
            raise RuntimeError(
                "API call " + response.url + " failed with error: \n" +
                response.text.replace("\\n", "\n").replace("\\t", "\t") +
                "\nHeaders: " + str(response.headers)
            )

        return response
