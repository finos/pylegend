# Copyright 2026 Goldman Sachs
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

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
from collections import namedtuple

import sphinx.util.inspect as ins


def isstaticmethod(obj, cls=None, name=None) -> bool:
    return False


def isabstractmethod(obj) -> bool:
    return False


ins.isabstractmethod = isabstractmethod
ins.isstaticmethod = isstaticmethod

namedtuple.__repr__ = lambda x: x.name

sys.path.insert(0, os.path.abspath('../../'))
sys.path.insert(0, os.path.abspath('.'))
import shlex
import datetime
import time
import requests
import subprocess
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from sphinx.application import Sphinx
import tests
from tests.test_helpers.dynamic_port_generator import generate_dynamic_port

project = 'PyLegend'
copyright = '2026, Vithesh'
author = 'Vithesh'
release = '0.1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.coverage',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx_rtd_theme',
    'sphinx.ext.todo',
    'sphinx.ext.githubpages',
    'sphinx_autodoc_typehints',
    'IPython.sphinxext.ipython_console_highlighting',
    'IPython.sphinxext.ipython_directive',
]

autoclass_content = 'init'
autodoc_default_options = {
    'member-order': 'bysource',
}
ipython_savefig_dir = '.'

templates_path = ['./_templates']
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['./_static']
html_favicon = './_static/img/favicon.ico'
html_show_copyright = False
html_show_sphinx = False
# html_style = 'css/style.css'
html_output_encoding = 'ascii'
html_theme_options = {
    'canonical_url': True,
    'logo': "/img/logo.png",
    'logo_name': True,
    'logo_text_align': "center",
    "show_powered_by": False,
    "page_width": "1200px",
    "sidebar_width": "300px",
    "font_size": "17px",
    "fixed_sidebar": "true",
}

html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
    ]
}

# -- Custom Server Setup for Sphinx ------------------------------------------

LOGGER = logging.getLogger(__name__)
engine_process = None
metadata_server = None


def start_legend_server(app: Sphinx) -> None:
    global engine_process, metadata_server
    LOGGER.info("Starting Legend Test Server for Sphinx build....")
    start = datetime.datetime.now()

    engine_port = generate_dynamic_port()
    metadata_port = generate_dynamic_port()
    relative_path = os.path.dirname(tests.__file__).replace("\\", "/")

    java_home = os.environ.get("JAVA_HOME")
    if java_home is None:
        raise RuntimeError("JAVA_HOME environment variable is not set")
    java_home = java_home.replace("\\", "/")
    cmd = (
        f'{java_home}/bin/java -jar '
        f'-Duser.timezone=UTC '
        f'-Dfile.encoding=UTF-8 '
        f'-Ddw.server.connector.port={engine_port} '
        f'-Ddw.metadataserver.alloy.port={metadata_port} '
        f'{relative_path}/resources/legend/server/pylegend-sql-server/target/pylegend-sql-server-1.0-shaded.jar server '
        f'{relative_path}/resources/legend/server/pylegend_sql_server_config.json'
    )

    LOGGER.info(f"Command: {cmd}")
    engine_process = subprocess.Popen(
        shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True, shell=False
    )

    class MetadataServerHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            path_map = {
                "/depot/api/projects/org.finos.legend.pylegend/pylegend-test-models/versions/0.0.1-SNAPSHOT/pureModelContextData?convertToNewProtocol=false&clientVersion=v1_33_0":
                    "org.finos.legend.pylegend_pylegend-test-models_0.0.1-SNAPSHOT.json",
                "/depot/api/projects/org.finos.legend.pylegend/pylegend-northwind-models/versions/0.0.1-SNAPSHOT/pureModelContextData?convertToNewProtocol=false&clientVersion=v1_33_0":
                    "org.finos.legend.pylegend_pylegend-northwind-models_0.0.1-SNAPSHOT.json"
            }

            file = path_map.get(self.path)

            if not file:
                self.send_error(404, f"Unhandled metadata path: {self.path}")
                return

            try:
                with open(f"{relative_path}/resources/legend/metadata/{file}", "r", encoding="utf-8") as f:
                    content = f.read()

                content_bytes = content.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header('Content-Length', str(len(content_bytes)))
                self.end_headers()
                self.wfile.write(content_bytes)
            except FileNotFoundError:
                self.send_error(404, "Metadata file not found")

        def log_message(self, format: str, *args: any) -> None:
            return

    metadata_server = HTTPServer(("localhost", metadata_port), MetadataServerHandler)
    metadata_server_thread = Thread(target=metadata_server.serve_forever)
    metadata_server_thread.daemon = True
    metadata_server_thread.start()

    try_count = 0
    while True:
        try_count += 1
        try:
            requests.get(f"http://localhost:{engine_port}/api/server/v1/info").raise_for_status()
            break
        except Exception:
            if try_count >= 15:
                raise RuntimeError("Unable to start legend server for testing")
            time.sleep(4)

    os.environ['PYLEGEND_DOC_GEN_ENGINE_PORT'] = str(engine_port)
    LOGGER.info(f"Legend Test Server started in {(datetime.datetime.now() - start).seconds} seconds.")


def stop_legend_server(app: Sphinx, exception: Exception) -> None:
    global engine_process, metadata_server
    LOGGER.info("Terminating Legend Test Server....")
    if engine_process:
        engine_process.terminate()
        engine_process.wait()
        LOGGER.info("Legend Engine process terminated.")
    if metadata_server:
        metadata_server.shutdown()
        LOGGER.info("Legend Metadata server terminated.")
    if 'PYLEGEND_DOC_GEN_ENGINE_PORT' in os.environ:
        del os.environ['PYLEGEND_DOC_GEN_ENGINE_PORT']


def setup(app: Sphinx) -> None:
    app.connect('builder-inited', start_legend_server)
    app.connect('build-finished', stop_legend_server)
