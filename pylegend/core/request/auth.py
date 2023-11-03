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

from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
)
from requests.auth import AuthBase

__all__: PyLegendSequence[str] = [
    "AuthScheme",
    "LocalhostEmptyAuthScheme",
    "BearerAuthScheme"
]


class AuthScheme(metaclass=ABCMeta):

    @abstractmethod
    def get_auth_base(self) -> PyLegendOptional[AuthBase]:
        pass


class LocalhostEmptyAuthScheme(AuthScheme):

    def get_auth_base(self) -> PyLegendOptional[AuthBase]:
        return None


class BearerAuth(AuthBase):
    def __init__(self, headerName: str, token: str) -> None:
        self.headerName = headerName
        self.token = token

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        r.headers[self.headerName] = self.token
        return r


class BearerAuthScheme(AuthScheme):
    def __init__(self, headerName: str, token: str) -> None:
        self.headerName = headerName
        self.token = token

    def get_auth_base(self) -> PyLegendOptional[AuthBase]:
        return BearerAuth(self.headerName, self.token)
