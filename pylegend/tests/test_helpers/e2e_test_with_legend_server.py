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

import shlex
import time
import requests
import subprocess
import pylegend
from abc import ABCMeta
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from pylegend.tests.test_helpers.dynamic_port_generator import generate_dynamic_port


class E2ETestWithLegendServer(metaclass=ABCMeta):
    engine_port = -1
    engine_process = None

    def setup_class(self) -> None:
        self.engine_port = generate_dynamic_port()
        metadata_port = generate_dynamic_port()
        relative_path = pylegend.tests.__file__.replace("\\", "/")[0: pylegend.tests.__file__.replace("\\", "/").rindex("/")]
        self.engine_process = subprocess.Popen(
            shlex.split(
                'java -jar '
                '-Ddw.server.connector.port=' + str(self.engine_port) + ' '
                '-Ddw.metadataserver.alloy.port=' + str(metadata_port) + ' ' +
                relative_path + '/resources/legend/server/pylegend-sql-server/target/'
                                'pylegend-sql-server-1.0-shaded.jar server ' +
                relative_path + '/resources/legend/server/pylegend_sql_server_config.json'
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            shell=False
        )

        class MetadataServerHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == ("/projects/org.finos.legend.pylegend/pylegend-test-models/versions/0.0.1-SNAPSHOT/" +
                                 "pureModelContextData?clientVersion=v1_31_0"):
                    file = "org.finos.legend.pylegend_pylegend-test-models_0.0.1-SNAPSHOT.json"
                else:
                    raise RuntimeError("Unhandled metadata path: " + self.path)

                with open(relative_path + "/resources/legend/metadata/" + file, "r") as f:
                    content = f.read()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header('Content-Length', str(len(content)))
                    self.end_headers()
                    self.wfile.write(content.encode("UTF-8"))
                    return

        metadata_server = HTTPServer(("localhost", metadata_port), MetadataServerHandler)
        metadata_server_thread = Thread(target=metadata_server.serve_forever)
        metadata_server_thread.daemon = True
        metadata_server_thread.start()

        try_count = 0
        while True:
            try_count += 1
            try:
                requests.get("http://localhost:" + str(self.engine_port) + "/api/server/v1/info").raise_for_status()
            except Exception:
                if try_count == 15:
                    raise RuntimeError("Unable to start legend server for testing")
                else:
                    time.sleep(2)
                    continue
            break

    def teardown_class(self) -> None:
        if self.engine_process:
            self.engine_process.terminate()
            self.engine_process.wait()
