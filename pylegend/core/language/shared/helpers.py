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

from pylegend._typing import PyLegendList
__all__ = [
    "generate_pure_functional_call",
    "generate_pure_lambda",
    "escape_column_name",
    "expr_has_matching_start_and_end_parentheses",
]


def generate_pure_functional_call(
        func: str,
        params: PyLegendList[str],
        force_prefix: bool = False
) -> str:
    should_prefix = force_prefix or (len(params) == 0)

    updated_params: PyLegendList[str] = []
    for param in params:
        if param.startswith("(") and param.endswith(")"):
            if expr_has_matching_start_and_end_parentheses(param):
                updated_params.append(param[1:-1])
            else:
                updated_params.append(param)
        else:
            updated_params.append(param)

    if should_prefix:
        return f"{func}({', '.join(updated_params)})"
    else:
        return f"{params[0]}->{func}({', '.join(updated_params[1:])})"


def generate_pure_lambda(param_name: str, expr: str, wrap_in_braces: bool = True) -> str:
    lambda_code = param_name + " | " + (expr[1:-1] if expr_has_matching_start_and_end_parentheses(expr) else expr)
    return "{" + lambda_code + "}" if wrap_in_braces else lambda_code


def escape_column_name(name: str) -> str:
    return (name if name.isidentifier() else "'" + name.replace("'", "\\'") + "'")


def expr_has_matching_start_and_end_parentheses(expr: str) -> bool:
    if expr.startswith("(") and expr.endswith(")"):
        bracket_indices = [0]
        for (i, char) in enumerate(expr[1:-1]):
            if char == "(":
                bracket_indices.append(i + 1)
            elif char == ")":
                if bracket_indices:
                    bracket_indices.pop()
        return (len(bracket_indices) == 1) and (bracket_indices[0] == 0)
    else:
        return False
