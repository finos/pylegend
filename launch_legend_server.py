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

    cmd = (
        f"{java_executable.as_posix()} -jar "
        f"-Duser.timezone=UTC "
        f"-Ddw.server.connector.port={engine_port} "
        f"-Ddw.metadataserver.alloy.port={metadata_port} "
        f"{jar_path.as_posix()} server "
        f"{config_path.as_posix()}"
    )

    print(f"Launching Legend Engine server on port {engine_port}...")
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

    print("Stopping Legend Server...")
    engine_process.terminate()
    engine_process.wait()


app = FastAPI(title="Legend Engine Tester")


example_sql: str = '''
    SELECT
        "First Name" AS "First Name",
        "Last Name" AS "Last Name",
        "Age" AS "Age",
        "Firm/Legal Name" AS "Firm/Legal Name"
    FROM
        func(
            path => 'pylegend::test::function::SimplePersonFunction__TabularDataSet_1_',
            coordinates => 'org.finos.legend.pylegend:pylegend-test-models:0.0.1-SNAPSHOT'
        ) AS "root"
'''
example_sql = dedent(example_sql).strip()


@app.post("/execute-sql-query-string", summary="Execute SQL query on Legend Engine server")
def execute_sql(
    sql: str = Body(..., media_type="text/plain", example=example_sql),
    output_format: str = Query(
        "table", enum=["table", "json-with-extracted-column-data", "json-with-raw-data"])
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
            return JSONResponse(status_code=response.status_code, content=response.text)

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
            elif output_format == "json-with-raw-data":
                return raw_data

        return raw_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def main():
    global LEGEND_ENGINE_PORT

    server_generator = legend_test_server()
    
    try:
        server_info = next(server_generator)
        LEGEND_ENGINE_PORT = server_info['engine_port']
        
        print("\n" + "="*60)
        print(f"Legend Engine server is running on port: {LEGEND_ENGINE_PORT}")
        print(f"Swagger page is available at: http://127.0.0.1:8000/docs")
        print("="*60 + "\n")

        uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")

    except KeyboardInterrupt:
        pass
    finally:
        try:
            next(server_generator)
        except StopIteration:
            print("Legend Engine server successfully stopped")


if __name__ == "__main__":
    main()
