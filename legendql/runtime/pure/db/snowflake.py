# Copyright 2025 Goldman Sachs
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

from abc import ABC, abstractmethod
from dataclasses import dataclass


from legendql.model.schema import Database
from legendql.runtime.pure.db.type import DatabaseDefinition


class Auth(ABC):
    @abstractmethod
    def generate_pure_auth_type(self) -> str:
        pass

    @abstractmethod
    def generate_pure_auth_configuration(self) -> str:
        pass

@dataclass
class PublicAuth(Auth):
    user_name: str
    private_key_vault_reference: str
    pass_phrase_vault_reference: str

    def generate_pure_auth_type(self) -> str:
        return "SnowflakePublic"

    def generate_pure_auth_configuration(self) -> str:
        return f"""
            publicUserName: '{self.user_name}';
            privateKeyVaultReference: '{self.private_key_vault_reference}';
            passPhraseVaultReference: '{self.pass_phrase_vault_reference}';
        """


@dataclass
class OAuth(Auth):
    key: str
    scope_name: str

    def generate_pure_auth_type(self) -> str:
        return "OAuth"

    def generate_pure_auth_configuration(self) -> str:
        return f"""
            oauthKey: '{self.key}';
            scopeName: '{self.scope_name}';        
        """


@dataclass
class SnowflakeDatabaseDefinition(DatabaseDefinition):
    auth: Auth
    name: str
    account: str
    warehouse: str
    region: str
    cloud_type: str
    account_type: str
    organization: str
    enable_query_tags: bool = False
    quoted_identifiers: bool = True
    quoted_identifiers_ignore_case: bool = False
    role: str = ""

    def get_type_name(self) -> str:
        return "Snowflake"

    def generate_pure_runtime(self, database: Database) -> str:
        return f"""
###Runtime
Runtime legendql::Runtime
{{
  mappings:
  [
  ];
  connections:
  [
    {database.name}:
    [
      connection_1: legendql::Connection
    ]
  ];
}}
"""

    def generate_pure_connection(self, database: Database) -> str:
        return f"""
###Connection
RelationalDatabaseConnection legendql::Connection
{{
  store: {database.name};
  type: Snowflake;
  quoteIdentifiers: {"true" if self.quoted_identifiers else "false"};
  specification: Snowflake
  {{
    name: '{self.name}';
    account: '{self.account}';
    warehouse: '{self.warehouse}';
    region: '{self.region}';
    cloudType: '{self.cloud_type}';
    enableQueryTags: {"true" if self.enable_query_tags else "false"};
    quotedIdentifiersIgnoreCase: {"true" if self.quoted_identifiers_ignore_case else "false"};
    accountType: {self.account_type};
    organization: '{self.organization}';
    role: '{self.role}';
  }};
  auth: {self.auth.generate_pure_auth_type()}
  {{
    {self.auth.generate_pure_auth_configuration()}
  }};
}}
"""
