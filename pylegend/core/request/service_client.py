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

import requests  # type: ignore
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

    def __init__(self, host: str, port: int, secure_http: bool, retry_count: int) -> None:
        self.__host = host
        self.__port = port
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

        try_no = 0
        while try_no < self.__retry_count:
            try_no += 1

            scheme = "https" if self.__secure_http else "http"
            url = "{scheme}://{host}:{port}/{path}".format(
                scheme=scheme,
                host=self.__host,
                port=self.__port,
                path=path
            )

            request = requests.Request(
                method=method.name,
                url=url,
                headers=headers,
                data=data,
                params=query_params
            )

            session = requests.Session()
            response = session.send(request.prepare(), stream=stream)

            if response.ok:
                return response
            elif response.status_code == 401:
                response.raise_for_status()
            else:
                last_response = response

        last_response.raise_for_status()
        return last_response
