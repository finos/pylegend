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
import os
import shlex
import datetime
import time
import requests
import subprocess
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from tests.test_helpers.dynamic_port_generator import generate_dynamic_port
from pylegend._typing import (
    PyLegendGenerator,
    PyLegendDict,
    PyLegendUnion
)
import tests

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def legend_test_server() -> PyLegendGenerator[PyLegendDict[str, PyLegendUnion[int, ]], None, None]:
    LOGGER.info("Start Legend Test Server ....")
    start = datetime.datetime.now()

    engine_port = generate_dynamic_port()
    metadata_port = generate_dynamic_port()
    relative_path = tests.__file__.replace("\\", "/")[0:tests.__file__.replace("\\", "/").rindex("/")]

    java_home = os.environ.get("JAVA_HOME")
    if java_home is None:
        raise RuntimeError("JAVA_HOME environment variable is not set")
    java_home = java_home.replace("\\", "/")
    cmd = f'{java_home}/bin/java -jar ' +\
          '-Duser.timezone=UTC ' +\
          '-Ddw.server.connector.port=' + str(engine_port) + ' ' +\
          '-Ddw.metadataserver.alloy.port=' + str(metadata_port) + ' ' +\
          relative_path + '/resources/legend/server/pylegend-sql-server/target/' + \
          'pylegend-sql-server-1.0-shaded.jar server ' +\
          relative_path + '/resources/legend/server/pylegend_sql_server_config.json'

    LOGGER.info("Command: " + cmd)
    engine_process = subprocess.Popen(
        shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True, shell=False
    )

    class MetadataServerHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == ("/depot/api/projects/org.finos.legend.pylegend/pylegend-test-models/versions/"
                             "0.0.1-SNAPSHOT/pureModelContextData?convertToNewProtocol=false&clientVersion=v1_33_0"):
                file = "org.finos.legend.pylegend_pylegend-test-models_0.0.1-SNAPSHOT.json"
            elif self.path == ("/depot/api/projects/org.finos.legend.pylegend/pylegend-northwind-models/versions/"
                               "0.0.1-SNAPSHOT/pureModelContextData?convertToNewProtocol=false&clientVersion=v1_33_0"):
                file = "org.finos.legend.pylegend_pylegend-northwind-models_0.0.1-SNAPSHOT.json"
            else:
                raise RuntimeError("Unhandled metadata path: " + self.path)  # pragma: no cover

            with open(relative_path + "/resources/legend/metadata/" + file, "r") as f:
                print(f"Came to file read\n")
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
            requests.get("http://localhost:" + str(engine_port) + "/api/server/v1/info").raise_for_status()
        except Exception:
            if try_count == 15:
                raise RuntimeError("Unable to start legend server for testing")  # pragma: no cover
            else:
                time.sleep(4)
                continue
        break

    LOGGER.info(f"Legend Test Server started in {(datetime.datetime.now() - start).seconds} seconds ...")

    yield {"engine_port": engine_port}

    LOGGER.info("Terminate Legend Test Server ....")
    engine_process.terminate()
    engine_process.wait()
    LOGGER.info("Legend Test Server terminated ....")
