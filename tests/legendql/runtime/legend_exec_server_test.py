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

import unittest

from legendql.runtime.pure.executionserver.runtime import LegendExecutionServer
from legendql.model.schema import Table


class TestGenerateSchemaPatterns(unittest.TestCase):

    def setUp(self):
        pass

    def test_table_with_schema(self):
        patterns = LegendExecutionServer._generate_schema_patterns("MY_TABLE", "MY_SCHEMA")
        self.assertEqual(1, len(patterns))
        self.assertEqual({'tablePattern': 'MY_TABLE', 'schemaPattern': 'MY_SCHEMA'}, patterns[0])

    def test_table_only(self):
        patterns = LegendExecutionServer._generate_schema_patterns("MY_TABLE")
        self.assertEqual(1, len(patterns))
        self.assertEqual({'tablePattern': 'MY_TABLE'}, patterns[0])


class TestParseSchemaExplorationResponse(unittest.TestCase):

    def setup(self):
        pass

    def test_single_table_case(self):
        response = {'_type': 'data', 'elements': [
            {'_type': 'relational', 'filters': [], 'includedStores': [], 'joins': [], 'schemas': [
                {'name': 'DIMENSION_DATA_PROD_V4', 'tables': [{'columns': [
                    {'name': 'ACTIVE', 'nullable': True, 'type': {'_type': 'Double'}},
                    {'name': 'CN', 'nullable': True, 'type': {'_type': 'Varchar', 'size': 16777216}}],
                                                               'milestoning': [], 'name': 'METADIR', 'primaryKey': []}],
                 'tabularFunctions': [], 'views': []}], 'stereotypes': []}]}
        database = LegendExecutionServer._parse_schema_exploration_response(response)
        self.assertEqual('legendql::Database', database.name)
        self.assertEqual(1, len(database.children))
        tables = []
        for x in database.children[0]:
            if isinstance(x, Table):
                tables.append(x)
        # Assert exactly one table
        self.assertEqual(1, len(tables))
        # Assert table has correct name
        self.assertEqual('METADIR', tables[0].table)
        # Assert table has 2 columns
        self.assertEqual(2, len(tables[0].columns))


