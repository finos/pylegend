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

import pytest
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer
from pylegend.core.request.legend_client import LegendClient
from pylegend.tests.test_helpers.dynamic_port_generator import generate_dynamic_port


class TestLegendClient:
    dynamic_port = generate_dynamic_port()

    def setup_class(self) -> None:
        class MockLegendServerHandler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                content_len = int(self.headers.get_all('content-length')[0])  # type: ignore
                data = self.rfile.read(content_len).decode()

                if self.path == "/api/sql/v1/execution/getSchemaFromQueryString" and \
                        data == "select 1+2 as a":
                    output = """{
                        "columns":
                            {
                                "__TYPE": "meta::external::query::sql::PrimitiveValueSchemaColumn",
                                "type": "String",
                                "name": "First Name"
                            }
                    }"""
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header('Content-Length', str(len(output)))
                    self.end_headers()
                    self.wfile.write(output.encode("UTF-8"))
                    return

                self.send_error(500, "Unexpected error when executing on path: " + self.path)
                return

        mock_legend_server = HTTPServer(("localhost", self.dynamic_port), MockLegendServerHandler)
        mock_legend_server_thread = Thread(target=mock_legend_server.serve_forever)
        mock_legend_server_thread.daemon = True  # Daemon threads automatically shutdown
        mock_legend_server_thread.start()

    def test_legend_client(self) -> None:
        client = LegendClient("localhost", self.dynamic_port, secure_http=False)
        assert ", ".join([str(x) for x in client.get_sql_string_schema("select 1+2 as a")]) == \
               'TdsColumn(Name: First Name, Type: String)'

    def test_legend_client_retry_error_message(self) -> None:
        with pytest.raises(ValueError, match="Retry count should be a number greater than 1. Got 0"):
            LegendClient("localhost", self.dynamic_port, secure_http=False, retry_count=0)

    def test_legend_client_unhandled_error_message(self) -> None:
        with pytest.raises(RuntimeError, match=".*Unexpected error when executing on path.*"):
            client = LegendClient("localhost", self.dynamic_port, secure_http=False)
            client.get_sql_string_schema("unknown sql")
