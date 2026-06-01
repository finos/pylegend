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

import logging

from pylegend.core.request.service_client import (
    ServiceClient,
    RequestMethod
)
import json
from pylegend.core.request.response_reader import ResponseReader
from pylegend.core.request.auth import AuthScheme, LocalhostEmptyAuthScheme
from pylegend._typing import (
    PyLegendDict,
    PyLegendSequence,
    PyLegendOptional,
    PyLegendList,
)
from pylegend.core.project_cooridnates import ProjectCoordinates, VersionedProjectCoordinates
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn, PrimitiveType, tds_columns_from_json


__all__: PyLegendSequence[str] = [
    "LegendClient",
]

LOGGER = logging.getLogger(__name__)

_DEPOT_MODEL_PATH = (
    "depot/api/projects/{groupId}/{artifactId}/versions/{version}"
    "/pureModelContextData?convertToNewProtocol=false&clientVersion=v1_33_0"
)


class LegendClient(ServiceClient):
    __depot_server_host: PyLegendOptional[str]
    __depot_server_port: PyLegendOptional[int]

    def __init__(
            self,
            host: str,
            port: int,
            secure_http: bool = True,
            path_prefix: PyLegendOptional[str] = "/api",
            auth_scheme: AuthScheme = LocalhostEmptyAuthScheme(),
            retry_count: int = 2,
            depot_server_host: PyLegendOptional[str] = None,
            depot_server_port: PyLegendOptional[int] = None,
    ) -> None:
        super().__init__(
            host=host,
            port=port,
            secure_http=secure_http,
            path_prefix=path_prefix,
            auth_scheme=auth_scheme,
            retry_count=retry_count
        )
        self.__depot_server_host = depot_server_host
        self.__depot_server_port = depot_server_port

    def get_sql_string_schema(
            self,
            sql: str
    ) -> PyLegendSequence[TdsColumn]:
        response = super()._execute_service(
            method=RequestMethod.POST,
            path="sql/v1/execution/schema",
            data=json.dumps({"sql": sql}),
            headers={"Content-Type": "application/json"},
            stream=False
        )
        response_text: str = response.text
        return tds_columns_from_json(response_text)

    def execute_sql_string(
            self,
            sql: str,
            chunk_size: PyLegendOptional[int] = None
    ) -> ResponseReader:
        iter_content = super()._execute_service(
            method=RequestMethod.POST,
            path="sql/v1/execution/execute",
            data=json.dumps({"sql": sql}),
            headers={"Content-Type": "application/json"},
            stream=True
        ).iter_content(chunk_size=chunk_size)
        return ResponseReader(iter_content)

    def get_pure_string_schema(
            self,
            pure: str,
            project_coordinates: ProjectCoordinates
    ) -> PyLegendSequence[TdsColumn]:
        lambda_response = super()._execute_service(
            method=RequestMethod.POST,
            path="pure/v1/grammar/grammarToJson/lambda",
            data=pure,
            headers={"Content-Type": "text/plain"},
            stream=False
        )
        lambda_json: PyLegendDict[str, object] = json.loads(lambda_response.text)
        execute_input = self._build_execute_input(lambda_json, project_coordinates)
        try:
            plan_response = super()._execute_service(
                method=RequestMethod.POST,
                path="pure/v1/execution/generatePlan",
                data=json.dumps(execute_input),
                headers={"Content-Type": "application/json"},
                stream=False
            )
            plan_json = json.loads(plan_response.text)
            try:
                result_type = plan_json["rootExecutionNode"]["resultType"]
            except (KeyError, TypeError) as e:
                raise RuntimeError(
                    "Unexpected resultType JSON shape from generatePlan: " + repr(str(plan_json))[:200], e
                )
            return self._tds_columns_from_plan_result_type(result_type)
        except RuntimeError as pure_err:
            LOGGER.debug("Pure generatePlan failed (%s); attempting depot-based schema", pure_err)
            if self.__depot_server_host is not None and self.__depot_server_port is not None:
                depot_input = self._build_depot_execute_input(pure, project_coordinates)
                plan_resp2 = super()._execute_service(
                    method=RequestMethod.POST,
                    path="pure/v1/execution/generatePlan",
                    data=json.dumps(depot_input),
                    headers={"Content-Type": "application/json"},
                    stream=False
                )
                plan_json2 = json.loads(plan_resp2.text)
                try:
                    result_type2 = plan_json2["rootExecutionNode"]["resultType"]
                except (KeyError, TypeError) as e2:
                    raise RuntimeError(
                        "Unexpected resultType JSON shape from depot generatePlan: "
                        + repr(str(plan_json2))[:200], e2
                    )
                return self._tds_columns_from_plan_result_type(result_type2)
            raise

    def execute_pure_string(
            self,
            pure: str,
            project_coordinates: ProjectCoordinates,
            chunk_size: PyLegendOptional[int] = None
    ) -> ResponseReader:
        lambda_response = super()._execute_service(
            method=RequestMethod.POST,
            path="pure/v1/grammar/grammarToJson/lambda",
            data=pure,
            headers={"Content-Type": "text/plain"},
            stream=False
        )
        lambda_json: PyLegendDict[str, object] = json.loads(lambda_response.text)
        execute_input = self._build_execute_input(lambda_json, project_coordinates)
        try:
            iter_content = super()._execute_service(
                method=RequestMethod.POST,
                path="pure/v1/execution/execute",
                data=json.dumps(execute_input),
                headers={"Content-Type": "application/json"},
                stream=True
            ).iter_content(chunk_size=chunk_size)
            return ResponseReader(iter_content)
        except RuntimeError as pure_err:
            LOGGER.debug("Pure execute failed (%s); attempting depot-based execution", pure_err)
            if self.__depot_server_host is not None and self.__depot_server_port is not None:
                depot_input = self._build_depot_execute_input(pure, project_coordinates)
                iter_content2 = super()._execute_service(
                    method=RequestMethod.POST,
                    path="pure/v1/execution/execute",
                    data=json.dumps(depot_input),
                    headers={"Content-Type": "application/json"},
                    stream=True
                ).iter_content(chunk_size=chunk_size)
                return ResponseReader(iter_content2)
            raise

    def _get_model_context_data(
            self,
            project_coordinates: ProjectCoordinates
    ) -> "PyLegendDict[str, object]":
        if not isinstance(project_coordinates, VersionedProjectCoordinates):
            raise RuntimeError(
                "Depot model fetch requires VersionedProjectCoordinates; "
                "got " + type(project_coordinates).__name__
            )
        if self.__depot_server_host is None or self.__depot_server_port is None:
            raise RuntimeError("Depot server host and port must be configured for model context data fetch")
        import requests as req_lib
        url = (
            f"http://{self.__depot_server_host}:{self.__depot_server_port}/"
            + _DEPOT_MODEL_PATH.format(
                groupId=project_coordinates.get_group_id(),
                artifactId=project_coordinates.get_artifact_id(),
                version=project_coordinates.get_version(),
            )
        )
        response = req_lib.get(url)
        if not response.ok:
            raise RuntimeError(
                f"Depot model fetch failed for {url}: {response.text[:200]}"
            )
        return json.loads(response.text)  # type: ignore[no-any-return]

    def _build_depot_execute_input(
            self,
            pure: str,
            project_coordinates: ProjectCoordinates
    ) -> "PyLegendDict[str, object]":
        if not isinstance(project_coordinates, VersionedProjectCoordinates):
            raise RuntimeError(
                "Depot execute input requires VersionedProjectCoordinates; "
                "got " + type(project_coordinates).__name__
            )
        model_data = self._get_model_context_data(project_coordinates)
        elements: PyLegendList[PyLegendDict[str, object]] = model_data.get("elements", [])  # type: ignore[assignment]
        sdlc_info: PyLegendDict[str, object] = {
            "_type": "alloy",
            "groupId": project_coordinates.get_group_id(),
            "artifactId": project_coordinates.get_artifact_id(),
            "version": project_coordinates.get_version(),
        }
        model_pointer: PyLegendDict[str, object] = {"_type": "pointer", "sdlcInfo": sdlc_info}
        # Try service: pure string matches |pkg::ServiceName.all() pattern
        import re
        service_match = re.match(r"^\|(.+)\.all\(\)$", pure.strip())
        if service_match:
            service_full_path = service_match.group(1)
            parts = service_full_path.rsplit("::", 1)
            service_name = parts[-1] if len(parts) >= 1 else service_full_path
            service_pkg = parts[0] if len(parts) == 2 else ""
            for elem in elements:
                if (elem.get("_type") == "service"
                        and elem.get("name") == service_name
                        and elem.get("package", "") == service_pkg):
                    execution: PyLegendDict[str, object] = elem.get("execution", {})  # type: ignore[assignment]
                    return {
                        "function": execution["func"],
                        "model": model_pointer,
                        "context": {"_type": "BaseExecutionContext"},
                        "mapping": execution["mapping"],
                        "runtime": execution["runtime"],
                    }
            raise RuntimeError(f"Service '{service_full_path}' not found in depot model for {project_coordinates}")
        # Try function: pure string matches |pkg::FunctionPath() pattern
        func_match = re.match(r"^\|(.+)\(\)$", pure.strip())
        if func_match:
            func_full_path = func_match.group(1)
            parts = func_full_path.rsplit("::", 1)
            func_name = parts[-1] if len(parts) >= 1 else func_full_path
            func_pkg = parts[0] if len(parts) == 2 else ""
            for elem in elements:
                if (elem.get("_type") == "function"
                        and elem.get("name") == func_name
                        and elem.get("package", "") == func_pkg):
                    body_lambda: PyLegendDict[str, object] = {
                        "_type": "lambda",
                        "body": elem.get("body", []),
                        "parameters": [],
                    }
                    return {
                        "function": body_lambda,
                        "model": model_pointer,
                        "context": {"_type": "BaseExecutionContext"},
                    }
            raise RuntimeError(f"Function '{func_full_path}' not found in depot model for {project_coordinates}")
        raise RuntimeError(
            f"Pure string '{pure[:80]}' does not match known service or function patterns for depot execution"
        )

    def _tds_columns_from_plan_result_type(
            self,
            result_type: "PyLegendDict[str, object]"
    ) -> PyLegendSequence[TdsColumn]:
        """Parse TdsColumn list from Pure generatePlan resultType JSON.

        The Pure execution plan result type has shape:
          {"_type": "tds", "tdsColumns": [{"name": "...", "type": "...", ...}]}
        whereas the SQL schema endpoint returns:
          {"columns": [{"_type": "primitiveSchemaColumn", "name": "...", "type": "..."}]}
        This method handles the Pure plan format specifically.
        """
        try:
            result_columns: PyLegendList[TdsColumn] = []
            raw_tds_cols = result_type.get("tdsColumns") or result_type.get("columns")
            tds_cols: PyLegendList[PyLegendDict[str, object]] = raw_tds_cols  # type: ignore[assignment]
            if tds_cols is None:
                raise RuntimeError(
                    "Neither 'tdsColumns' nor 'columns' found in plan result_type: "
                    + repr(str(result_type))[:200]
                )
            for col in tds_cols:
                col_type_str: str = str(col["type"])
                col_name: str = str(col["name"])
                try:
                    prim_type = PrimitiveType[col_type_str]
                    result_columns.append(PrimitiveTdsColumn(col_name, prim_type))
                except KeyError:
                    result_columns.append(
                        PrimitiveTdsColumn(col_name, PrimitiveType.String)
                    )
            return result_columns
        except Exception as e:
            raise RuntimeError(
                "Unable to parse tds columns from plan result_type: " + repr(str(result_type))[:200], e
            )

    def _build_execute_input(
            self,
            lambda_json: "PyLegendDict[str, object]",
            project_coordinates: ProjectCoordinates
    ) -> "PyLegendDict[str, object]":
        if not isinstance(project_coordinates, VersionedProjectCoordinates):
            raise RuntimeError(
                "Pure execution requires VersionedProjectCoordinates; "
                "got " + type(project_coordinates).__name__
            )
        sdlc_info: PyLegendDict[str, object] = {
            "_type": "alloy",
            "groupId": project_coordinates.get_group_id(),
            "artifactId": project_coordinates.get_artifact_id(),
            "version": project_coordinates.get_version(),
        }
        return {
            "function": lambda_json,
            "model": {"_type": "pointer", "sdlcInfo": sdlc_info},
            "context": {"_type": "BaseExecutionContext"},
        }

    def parse_model(
            self,
            model_code: str,
            return_source_information: bool = False
    ) -> str:
        response = super()._execute_service(
            method=RequestMethod.POST,
            path="pure/v1/grammar/grammarToJson/model",
            data=model_code,
            headers={"Content-Type": "text/plain"},
            query_params=[("returnSourceInformation", "true" if return_source_information else "false")]
        )
        return response.text

    def compile_model(
            self,
            model_json: str
    ) -> None:
        super()._execute_service(
            method=RequestMethod.POST,
            path="pure/v1/compilation/compile",
            data=model_json,
            headers={"Content-Type": "application/json"}
        )

    def parse_and_compile_model(
            self,
            model_code: str
    ) -> None:
        parse_response = self.parse_model(model_code)
        self.compile_model(parse_response)

    def __eq__(self, other: 'object') -> bool:
        if self is other:
            return True
        if isinstance(other, LegendClient):
            return self.get_host() == other.get_host() and self.get_port() == other.get_port()
        return False
