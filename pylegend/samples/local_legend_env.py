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

import atexit
import json
import os
import tempfile
import time
import logging
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import requests
from testcontainers.core.container import DockerContainer  # type: ignore

from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.utils.dynamic_port_generator import generate_dynamic_port
from pylegend.core.project_cooridnates import VersionedProjectCoordinates


__all__: PyLegendSequence[str] = [
    "get_local_legend_env",
    "NORTHWIND_PROJECT_COORDINATES",
]

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
if not LOGGER.handlers:
    _ch = logging.StreamHandler()
    _ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    LOGGER.addHandler(_ch)

_SAMPLES_DIR = Path(__file__).resolve().parent
_METADATA_DIR = _SAMPLES_DIR / "resources" / "metadata"

_DEFAULT_IMAGE = "eclipse-temurin:11-jdk"

_LEGEND_ENGINE_VERSION = "4.121.0"
_LEGEND_JAR_URL = (
    f"https://repo1.maven.org/maven2/org/finos/legend/engine/legend-engine-server-http-server/"
    f"{_LEGEND_ENGINE_VERSION}/legend-engine-server-http-server-{_LEGEND_ENGINE_VERSION}-shaded.jar"
)


class LocalLegendEnv:
    def __init__(
        self,
        image: str = _DEFAULT_IMAGE,
        metadata_port: PyLegendOptional[int] = None,
        max_wait_seconds: int = 600,
    ) -> None:
        self._image = image
        self._metadata_port = metadata_port or generate_dynamic_port()
        self._max_wait_seconds = max_wait_seconds

        self._engine_container: PyLegendOptional[DockerContainer] = None
        self._metadata_server: PyLegendOptional[HTTPServer] = None
        self._config_path: PyLegendOptional[str] = None
        self._engine_port: PyLegendOptional[int] = None
        self._testdb_port: PyLegendOptional[int] = None
        self._legend_client: PyLegendOptional[LegendClient] = None
        self._server_jar_path: PyLegendOptional[Path] = None

    @property
    def engine_port(self) -> int:
        if self._engine_port is None:
            raise RuntimeError("Server not started")
        return self._engine_port

    @property
    def legend_client(self) -> LegendClient:
        if self._legend_client is None:
            raise RuntimeError("Server not started")
        return self._legend_client

    def start(self) -> "LocalLegendEnv":
        self._validate_resources()
        self._server_jar_path = _get_server_jar_path()
        self._start_metadata_server()
        self._start_engine_container()
        self._wait_for_engine()
        self._legend_client = LegendClient(
            "127.0.0.1", self._engine_port, secure_http=False  # type: ignore[arg-type]
        )
        return self

    def stop(self) -> None:
        if self._engine_container:
            self._engine_container.stop()
        if self._metadata_server:
            self._metadata_server.shutdown()
        if self._config_path:
            try:
                os.unlink(self._config_path)
                os.rmdir(os.path.dirname(self._config_path))
            except OSError:
                pass
        self._engine_container = self._metadata_server = self._config_path = None
        self._engine_port = self._testdb_port = self._legend_client = None

    def _validate_resources(self) -> None:
        meta = _METADATA_DIR
        if not meta.exists():
            raise FileNotFoundError(
                f"Metadata directory not found at {meta}. "
                "Make sure you are running from a pylegend repository checkout."
            )

    def _start_metadata_server(self) -> None:
        handler_class = type(
            "_Handler",
            (_MetadataServerHandler,),
            {"metadata_dir": str(_METADATA_DIR.resolve())},
        )
        self._metadata_server = HTTPServer(
            ("127.0.0.1", self._metadata_port), handler_class
        )
        t = Thread(target=self._metadata_server.serve_forever, daemon=True)
        t.start()
        LOGGER.info("Metadata server listening on port %d", self._metadata_port)

    def _start_engine_container(self) -> None:
        if self._server_jar_path is None:
            raise RuntimeError("Server JAR path is not set")

        self._engine_port = generate_dynamic_port()
        self._testdb_port = generate_dynamic_port()
        self._config_path = _write_server_config(self._engine_port, "127.0.0.1", self._metadata_port, self._testdb_port)

        self._engine_container = (
            DockerContainer(self._image)
            .with_volume_mapping(str(self._server_jar_path.resolve()), "/legend/server.jar", "ro")
            .with_volume_mapping(self._config_path, "/legend/config.json", "ro")
            .with_env("JAVA_TOOL_OPTIONS", "-Duser.timezone=UTC -Dfile.encoding=UTF-8")
            .with_command("sh -c 'java -jar /legend/server.jar server /legend/config.json'")
        )

        self._engine_container.with_kwargs(network_mode="host")

        LOGGER.info("Starting Legend engine container (%s) …", self._image)
        self._engine_container.start()

        LOGGER.info("Engine container started; responding on host network port = %d", self._engine_port)

    def _wait_for_engine(self) -> None:
        url = f"http://127.0.0.1:{self._engine_port}/api/server/v1/info"
        deadline = time.monotonic() + self._max_wait_seconds
        while time.monotonic() < deadline:
            try:
                requests.get(url, timeout=5).raise_for_status()
                return LOGGER.info("Legend engine is healthy.")
            except Exception:
                time.sleep(3)
        raise RuntimeError(f"Engine not healthy within {self._max_wait_seconds}s at {url}")


class _MetadataServerHandler(BaseHTTPRequestHandler):
    metadata_dir: str = ""

    def do_GET(self) -> None:
        if (
            "/depot/api/projects/org.finos.legend.pylegend/pylegend-northwind-models/versions/"
            "0.0.1-SNAPSHOT/pureModelContextData?convertToNewProtocol=false&clientVersion=v1_33_0"
        ) != self.path:
            return self.send_error(404, f"Unhandled metadata path: {self.path}")

        file_path = os.path.join(self.metadata_dir, "org.finos.legend.pylegend_pylegend-northwind-models_0.0.1-SNAPSHOT.json")
        try:
            with open(file_path, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except OSError:
            self.send_error(404, f"Metadata file not found: {file_path}")

    def log_message(self, format: str, *args: object) -> None: pass


def _write_server_config(engine_port: int, metadata_host: str, metadata_port: int, testdb_port: int) -> str:
    with open(_SAMPLES_DIR / "resources" / "config" / "engine_config.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    config["server"]["connector"]["port"] = engine_port
    config["server"]["connector"]["bindHost"] = "127.0.0.1"
    for comp in ("alloy", "sdlc"):
        config["metadataserver"][comp].update({"host": metadata_host, "port": metadata_port})
    config["relationalexecution"]["temporarytestdb"]["port"] = testdb_port

    config_path = os.path.join(tempfile.mkdtemp(prefix="pylegend_"), "config.json")
    with open(config_path, "w") as f:
        json.dump(config, f)
    return config_path


def _get_server_jar_path() -> Path:
    cache_dir = Path.home() / ".cache" / "pylegend"
    cache_dir.mkdir(parents=True, exist_ok=True)
    jar = cache_dir / f"legend-engine-server-http-server-{_LEGEND_ENGINE_VERSION}-shaded.jar"
    if not jar.exists():
        LOGGER.info("Downloading Legend server JAR %s...", _LEGEND_ENGINE_VERSION)
        try:
            with requests.get(_LEGEND_JAR_URL, stream=True) as r, open(jar, 'wb') as f:
                r.raise_for_status()
                for chunk in r.iter_content(8192):
                    f.write(chunk)
        except Exception as e:
            if jar.exists():
                jar.unlink()
            raise RuntimeError(f"Failed to download JAR: {e}") from e
    return jar


_singleton: PyLegendOptional[LocalLegendEnv] = None


def get_local_legend_env(**kwargs: object) -> LocalLegendEnv:
    global _singleton
    if _singleton is None:
        _singleton = LocalLegendEnv(**kwargs).start()  # type: ignore[arg-type]
        atexit.register(_singleton.stop)
    return _singleton


NORTHWIND_PROJECT_COORDINATES = VersionedProjectCoordinates(
    group_id="org.finos.legend.pylegend",
    artifact_id="pylegend-northwind-models",
    version="0.0.1-SNAPSHOT",
)
