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

import abc
from abc import ABCMeta
from pylegend._typing import (
    PyLegendSequence,
    PyLegendList,
)
from pylegend.core.sql.metamodel import (
    NamedArgumentExpression,
    StringLiteral,
)

__all__: PyLegendSequence[str] = [
    "ProjectCoordinates",
    "VersionedProjectCoordinates",
    "PersonalWorkspaceProjectCoordinates",
    "GroupWorkspaceProjectCoordinates",
]


class ProjectCoordinates(metaclass=ABCMeta):

    @abc.abstractmethod
    def sql_params(self) -> PyLegendList[NamedArgumentExpression]:
        pass


class VersionedProjectCoordinates(ProjectCoordinates):
    __group_id: str
    __artifact_id: str
    __version: str

    def __init__(self, group_id: str, artifact_id: str, version: str) -> None:
        self.__group_id = group_id
        self.__artifact_id = artifact_id
        self.__version = version

    def get_group_id(self) -> str:
        return self.__group_id

    def get_artifact_id(self) -> str:
        return self.__artifact_id

    def get_version(self) -> str:
        return self.__version

    def sql_params(self) -> PyLegendList[NamedArgumentExpression]:
        return [
            NamedArgumentExpression(
                name="coordinates",
                expression=StringLiteral(
                    value=f"{self.__group_id}:{self.__artifact_id}:{self.__version}",
                    quoted=False
                )
            )
        ]


class WorkspaceProjectCoordinates(ProjectCoordinates, metaclass=ABCMeta):
    __project_id: str

    def __init__(self, project_id: str) -> None:
        self.__project_id = project_id

    def get_project_id(self) -> str:
        return self.__project_id


class PersonalWorkspaceProjectCoordinates(WorkspaceProjectCoordinates):
    __workspace: str

    def __init__(self, project_id: str, workspace: str) -> None:
        super().__init__(project_id)
        self.__workspace = workspace

    def get_workspace(self) -> str:
        return self.__workspace

    def sql_params(self) -> PyLegendList[NamedArgumentExpression]:
        return [
            NamedArgumentExpression(
                name="project",
                expression=StringLiteral(value=self.get_project_id(), quoted=False)
            ),
            NamedArgumentExpression(
                name="workspace",
                expression=StringLiteral(value=self.__workspace, quoted=False)
            )
        ]


class GroupWorkspaceProjectCoordinates(WorkspaceProjectCoordinates):
    __group_workspace: str

    def __init__(self, project_id: str, group_workspace: str) -> None:
        super().__init__(project_id)
        self.__group_workspace = group_workspace

    def get_group_workspace(self) -> str:
        return self.__group_workspace

    def sql_params(self) -> PyLegendList[NamedArgumentExpression]:
        return [
            NamedArgumentExpression(
                name="project",
                expression=StringLiteral(value=self.get_project_id(), quoted=False)
            ),
            NamedArgumentExpression(
                name="groupWorkspace",
                expression=StringLiteral(value=self.__group_workspace, quoted=False)
            )
        ]
