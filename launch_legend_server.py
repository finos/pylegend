import os
import shlex
from textwrap import dedent
import time
import requests
import subprocess
import logging
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import uvicorn
from fastapi import FastAPI, Body, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse
from tabulate import tabulate 

from pylegend.core.project_cooridnates import VersionedProjectCoordinates
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_legend_service_input_frame import PandasApiLegendServiceInputFrame
from tests.test_helpers.dynamic_port_generator import generate_dynamic_port
import tests


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


LEGEND_ENGINE_PORT = None


def legend_test_server():
    engine_port = generate_dynamic_port()
    metadata_port = generate_dynamic_port()

    relative_path = Path(tests.__file__).parent

    if "JAVA_HOME" not in os.environ:
        raise RuntimeError("JAVA_HOME environment variable is not set")
    java_home = Path(os.environ["JAVA_HOME"])

    java_executable = java_home / "bin" / "java"

    jar_path = relative_path / "resources/legend/server/pylegend-sql-server/target/pylegend-sql-server-1.0-shaded.jar"
    config_path = relative_path / "resources/legend/server/pylegend_sql_server_config.json"

    if not jar_path.is_file():
        raise FileNotFoundError(f"jar file not found at path: {jar_path}")
    if not config_path.is_file():
        raise FileNotFoundError(f"config file not found at path: {config_path}")

    cmd = (
        f"{java_executable.as_posix()} -jar "
        f"-Duser.timezone=UTC "
        f"-Ddw.server.connector.port={engine_port} "
        f"-Ddw.metadataserver.alloy.port={metadata_port} "
        f"{jar_path.as_posix()} server "
        f"{config_path.as_posix()}"
    )

    print(f"Starting Legend Engine server on port {engine_port}...")
    engine_process = subprocess.Popen(
        shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True, shell=False
    )

    class MetadataServerHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if "pureModelContextData" in self.path:
                file_name = "org.finos.legend.pylegend_pylegend-test-models_0.0.1-SNAPSHOT.json"
            else:
                self.send_error(404, "Unknown path")
                return

            file_path = relative_path / "resources/legend/metadata" / file_name
            
            try:
                with open(file_path, "r") as f:
                    content = f.read()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header('Content-Length', str(len(content)))
                    self.end_headers()
                    self.wfile.write(content.encode("UTF-8"))
            except FileNotFoundError:
                self.send_error(404, "Metadata file not found")

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
            if try_count == 30:
                engine_process.terminate()
                raise RuntimeError("Unable to start legend server")
            time.sleep(2)

    yield {"engine_port": engine_port}

    print("Stopping Legend Engine Server...")
    engine_process.terminate()
    engine_process.wait()


app = FastAPI(title="Legend Engine Tester")


example_sql: str = '''
    SELECT
        "root"."First Name" AS "First Name",
        "root"."Last Name" AS "Last Name",
        "root"."Age" AS "Age",
        "root"."Firm/Legal Name" AS "Firm/Legal Name"
    FROM
        service(
            pattern => '/simpleRelationPersonService',
            coordinates => 'org.finos.legend.pylegend:pylegend-test-models:0.0.1-SNAPSHOT'
        ) AS "root"
'''
example_sql = dedent(example_sql).strip()


@app.post("/execute-sql-query", summary="Execute SQL Query on Legend Engine Server")
def execute_sql(
    sql: str = Body(..., media_type="text/plain", example=example_sql),
    output_format: str = Query(
        "table", enum=["table", "json-with-extracted-column-data", "json-with-raw-legend-engine-output"])
):

    if LEGEND_ENGINE_PORT is None:
        raise HTTPException(status_code=500, detail="Legend Server is not running")

    url = f"http://localhost:{LEGEND_ENGINE_PORT}/api/sql/v1/execution/executeQueryString?serializationFormat=DEFAULT"

    try:
        response = requests.post(
            url, 
            data=sql, 
            headers={"Content-Type": "text/plain", "Accept": "application/json"}
        )

        if response.status_code != 200:
            try:
                return JSONResponse(status_code=response.status_code, content=response.json())
            except ValueError:
                return PlainTextResponse(status_code=response.status_code, content=response.text)

        raw_data = response.json()

        if "result" in raw_data and "columns" in raw_data["result"]:
            columns = raw_data["result"]["columns"]
            rows = raw_data["result"]["rows"]

            formatted_data = [dict(zip(columns, r["values"])) for r in rows]

            if output_format == "table":
                table_str = tabulate(formatted_data, headers="keys", tablefmt="psql", missingval="NULL")
                return PlainTextResponse(table_str)
            elif output_format == "json-with-extracted-column-data":
                return formatted_data
            elif output_format == "json-with-raw-legend-engine-output":
                return raw_data

        return raw_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def run_test():
    frame = PandasApiLegendServiceInputFrame(
        pattern="/simpleRelationPersonService",
        project_coordinates=VersionedProjectCoordinates(
            group_id="org.finos.legend.pylegend",
            artifact_id="pylegend-test-models",
            version="0.0.1-SNAPSHOT"
        ),
        legend_client=LegendClient("localhost", LEGEND_ENGINE_PORT, secure_http=False)
    )
    sql = frame.to_sql_query(FrameToSqlConfig())
    print(f"sql = {sql}")


def main():
    global LEGEND_ENGINE_PORT

    server_generator = legend_test_server()
    
    try:
        server_info = next(server_generator)
        LEGEND_ENGINE_PORT = server_info['engine_port']
        SWAGGER_PORT = 42069

        print("\n" + "="*60)
        print(f"Legend Engine Server is running on port: {LEGEND_ENGINE_PORT}")
        print(f"Swagger page is available at: http://127.0.0.1:{SWAGGER_PORT}/docs")
        print("="*60 + "\n")

        # run_test()

        uvicorn.run(app, host="127.0.0.1", port=SWAGGER_PORT, log_level="warning")

    except KeyboardInterrupt:
        pass
    finally:
        try:
            next(server_generator)
        except StopIteration:
            print("Legend Engine Server successfully stopped")


if __name__ == "__main__":
    main()
