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
import subprocess
import sys
import docker  # type: ignore
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import requests
from testcontainers.core.container import DockerContainer  # type: ignore

from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
    PyLegendDict,
    PyLegendUnion,
    PyLegendAny,
    PyLegendTuple,
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

_LEGEND_ENGINE_VERSION = "4.112.0"
_LEGEND_JAR_URL = (
    f"https://repo1.maven.org/maven2/org/finos/legend/engine/legend-engine-server-http-server/"
    f"{_LEGEND_ENGINE_VERSION}/legend-engine-server-http-server-{_LEGEND_ENGINE_VERSION}-shaded.jar"
)


class LocalLegendEnv:
    def __init__(  # type: ignore
        self,
        image: str = _DEFAULT_IMAGE,
        metadata_port: PyLegendOptional[int] = None,
        metadata_resources: PyLegendOptional[
            PyLegendDict[VersionedProjectCoordinates, PyLegendUnion[str, PyLegendDict[PyLegendAny, PyLegendAny]]]
        ] = None,
        max_wait_seconds: int = 120,
    ) -> None:
        self._image = image
        self._metadata_port = metadata_port or generate_dynamic_port()
        self._metadata_resources = metadata_resources
        self._max_wait_seconds = max_wait_seconds

        self._engine_container: PyLegendOptional[DockerContainer] = None
        self._engine_process: PyLegendOptional[subprocess.Popen[bytes]] = None
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
        if self._engine_container is not None or self._engine_process is not None:
            return self
        atexit.register(self.stop)
        res = dict(self._metadata_resources) if self._metadata_resources is not None else {}
        nw = NORTHWIND_PROJECT_COORDINATES
        if not any(c.get_group_id() == nw.get_group_id() and
                   c.get_artifact_id() == nw.get_artifact_id() and
                   c.get_version() == nw.get_version() for c in res.keys()):
            northwind_file = (
                _METADATA_DIR / "org.finos.legend.pylegend_pylegend-northwind-models_0.0.1-SNAPSHOT.json"
            ).resolve()
            with open(northwind_file, "r", encoding="utf-8") as f:
                res[NORTHWIND_PROJECT_COORDINATES] = f.read()
        self._metadata_resources = res

        self._server_jar_path = _get_server_jar_path()
        self._start_metadata_server()
        self._start_engine()
        self._wait_for_engine()
        self._legend_client = LegendClient(
            "127.0.0.1", self._engine_port, secure_http=False  # type: ignore[arg-type]
        )
        return self

    def stop(self) -> None:
        if self._engine_container:
            self._engine_container.stop()
        if self._engine_process:
            self._engine_process.terminate()
            self._engine_process.wait()
        if self._metadata_server:
            self._metadata_server.shutdown()
        if self._config_path:
            try:
                os.unlink(self._config_path)
                os.rmdir(os.path.dirname(self._config_path))
            except OSError:
                pass
        self._engine_container = self._engine_process = self._metadata_server = self._config_path = None
        self._engine_port = self._testdb_port = self._legend_client = None

    def _start_metadata_server(self) -> None:
        metadata_map: PyLegendDict[str, str] = {}
        if self._metadata_resources:
            for coords, content in self._metadata_resources.items():
                if isinstance(content, dict):
                    content = json.dumps(content)
                metadata_map[_get_metadata_path(coords)] = content

        handler_class = type(
            "_Handler",
            (_MetadataServerHandler,),
            {"metadata_map": metadata_map},
        )
        self._metadata_server = HTTPServer(
            ("127.0.0.1", self._metadata_port), handler_class
        )
        t = Thread(target=self._metadata_server.serve_forever, daemon=True)
        t.start()
        LOGGER.info("Metadata server listening on port %d", self._metadata_port)

    def _start_engine(self) -> None:
        if self._server_jar_path is None:
            raise RuntimeError("Server JAR path is not set")

        self._engine_port = generate_dynamic_port()
        self._testdb_port = generate_dynamic_port()
        self._config_path = _write_server_config(self._engine_port, "127.0.0.1", self._metadata_port, self._testdb_port)

        has_docker = _is_docker_available()
        if has_docker:
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
        else:
            LOGGER.info("Docker is unavailable. Falling back to direct Java subprocess.")
            java_home = os.environ.get("JAVA_HOME")
            if not java_home:
                raise RuntimeError("JAVA_HOME environment variable is not set. "
                                   "It is required to run the local Legend engine without Docker.")
            java_cmd = os.path.join(java_home, "bin", "java")

            cmd = [
                java_cmd,
                "-jar",
                "-Duser.timezone=UTC",
                "-Dfile.encoding=UTF-8",
                str(self._server_jar_path.resolve()),
                "server",
                self._config_path
            ]

            self._engine_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
                shell=False
            )
            LOGGER.info("Engine Java subprocess started; responding on port = %d", self._engine_port)

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
    metadata_map: PyLegendDict[str, str] = {}

    def do_GET(self) -> None:
        content = self.metadata_map.get(self.path)

        if not content:
            return self.send_error(404, f"Unhandled metadata path: {self.path}")

        try:
            payload = content.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except Exception as e:
            self.send_error(500, f"Error processing metadata: {e}")

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


def _get_metadata_path(coords: VersionedProjectCoordinates) -> str:
    return (
        f"/depot/api/projects/{coords.get_group_id()}/{coords.get_artifact_id()}/versions/"
        f"{coords.get_version()}/pureModelContextData?convertToNewProtocol=false&clientVersion=v1_33_0"
    )


def _is_docker_available() -> bool:
    if sys.platform != "linux":
        return False
    try:
        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


_envs: PyLegendDict[PyLegendTuple[PyLegendAny, ...], LocalLegendEnv] = {}  # type: ignore


def get_local_legend_env(  # type: ignore
    metadata_resources: PyLegendOptional[
        PyLegendDict[VersionedProjectCoordinates, PyLegendUnion[str, PyLegendDict[PyLegendAny, PyLegendAny]]]
    ] = None
) -> LocalLegendEnv:
    res_keys = list(metadata_resources.keys()) if metadata_resources else []
    nw = NORTHWIND_PROJECT_COORDINATES
    if not any(c.get_group_id() == nw.get_group_id() and
               c.get_artifact_id() == nw.get_artifact_id() and
               c.get_version() == nw.get_version() for c in res_keys):
        res_keys.append(nw)

    key = tuple(sorted(
        json.dumps({
            "groupId": c.get_group_id(),
            "artifactId": c.get_artifact_id(),
            "version": c.get_version()
        }, sort_keys=True)
        for c in res_keys
    ))

    if key not in _envs:
        _envs[key] = LocalLegendEnv(
            metadata_resources=metadata_resources
        ).start()
    return _envs[key]


NORTHWIND_PROJECT_COORDINATES = VersionedProjectCoordinates(
    group_id="org.finos.legend.pylegend",
    artifact_id="pylegend-northwind-models",
    version="0.0.1-SNAPSHOT",
)
