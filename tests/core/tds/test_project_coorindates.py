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

from pylegend.core.project_cooridnates import (
    VersionedProjectCoordinates,
    PersonalWorkspaceProjectCoordinates,
    GroupWorkspaceProjectCoordinates,
)
from pylegend.core.databse.sql_to_string import (
    SqlToStringDbExtension,
    SqlToStringConfig,
    SqlToStringFormat
)


class TestProjectCoordinates:
    extension = SqlToStringDbExtension()
    config = SqlToStringConfig(SqlToStringFormat())

    def test_versioned_project_coordinates(self) -> None:
        c = VersionedProjectCoordinates("org.test.group", "test-artifact", "1.0.0")
        params = c.sql_params()
        assert len(params) == 1
        assert [self.extension.process_expression(p, config=self.config) for p in params] == [
            "coordinates => 'org.test.group:test-artifact:1.0.0'"
        ]

    def test_personal_workspace_coordinates(self) -> None:
        c = PersonalWorkspaceProjectCoordinates("PROD-test_project", "test-workspace")
        params = c.sql_params()
        assert len(params) == 2
        assert [self.extension.process_expression(p, config=self.config) for p in params] == [
            "project => 'PROD-test_project'",
            "workspace => 'test-workspace'"
        ]

    def test_group_workspace_coordinates(self) -> None:
        c = GroupWorkspaceProjectCoordinates("PROD-test_project", "test-group-workspace")
        params = c.sql_params()
        assert len(params) == 2
        assert [self.extension.process_expression(p, config=self.config) for p in params] == [
            "project => 'PROD-test_project'",
            "groupWorkspace => 'test-group-workspace'"
        ]
