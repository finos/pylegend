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

from typing import Callable, Any
from pylegend.core.language import (
    PyLegendBoolean,
    PyLegendString,
    PyLegendNumber,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendDateTime,
    PyLegendStrictDate,
    PyLegendDate,
    PyLegendBooleanVariableExpression,
    PyLegendStringVariableExpression,
    PyLegendNumberVariableExpression,
    PyLegendIntegerVariableExpression,
    PyLegendFloatVariableExpression,
    PyLegendDateVariableExpression,
    PyLegendStrictDateVariableExpression,
    PyLegendDateTimeVariableExpression,
)
from pylegend.core.tds.tds_frame import FrameToPureConfig


class TestPyLegendVariableExpressions:
    frame_to_pure_config = FrameToPureConfig()

    def test_boolean_variable_expression(self) -> None:
        var1 = PyLegendBoolean(PyLegendBooleanVariableExpression("var1"))
        assert var1.to_pure_expression(self.frame_to_pure_config) == '$var1'
        var2 = PyLegendBoolean(PyLegendBooleanVariableExpression("var2"))
        assert (var1 | var2).to_pure_expression(self.frame_to_pure_config) == '(toOne($var1) || toOne($var2))'

    def test_string_variable_expression(self) -> None:
        var1 = PyLegendString(PyLegendStringVariableExpression("var1"))
        assert var1.to_pure_expression(self.frame_to_pure_config) == '$var1'
        assert (var1 + '__').to_pure_expression(self.frame_to_pure_config) == "(toOne($var1) + '__')"
        assert var1.value().get_sub_expressions() == [var1.value()]

    def test_number_variable_expressions(self) -> None:
        var1 = PyLegendNumber(PyLegendNumberVariableExpression("var1"))
        assert var1.to_pure_expression(self.frame_to_pure_config) == '$var1'
        var2 = PyLegendInteger(PyLegendIntegerVariableExpression("var2"))
        assert var2.to_pure_expression(self.frame_to_pure_config) == '$var2'
        var3 = PyLegendFloat(PyLegendFloatVariableExpression("var3"))
        assert var3.to_pure_expression(self.frame_to_pure_config) == '$var3'
        assert (var1 + 1).to_pure_expression(self.frame_to_pure_config) == '(toOne($var1) + 1)'
        assert (var2 + 1.0).to_pure_expression(self.frame_to_pure_config) == '(toOne($var2) + 1.0)'
        assert (var3 + 1).to_pure_expression(self.frame_to_pure_config) == '(toOne($var3) + 1)'

    def test_date_variable_expressions(self) -> None:
        var1 = PyLegendDate(PyLegendDateVariableExpression("var1"))
        assert var1.to_pure_expression(self.frame_to_pure_config) == '$var1'
        var2 = PyLegendStrictDate(PyLegendStrictDateVariableExpression("var2"))
        assert var2.to_pure_expression(self.frame_to_pure_config) == '$var2'
        var3 = PyLegendDateTime(PyLegendDateTimeVariableExpression("var3"))
        assert var3.to_pure_expression(self.frame_to_pure_config) == '$var3'
        assert (var1.first_day_of_month()).to_pure_expression(self.frame_to_pure_config) == \
               'toOne($var1)->firstDayOfMonth()'
        assert (var2.day()).to_pure_expression(self.frame_to_pure_config) == 'toOne($var2)->dayOfMonth()'
        assert (var3.date_part()).to_pure_expression(self.frame_to_pure_config) == \
               'toOne($var3)->datePart()->cast(@StrictDate)'

    def test_variable_use_in_function_translation(self) -> None:
        def to_pure(func: Callable[..., Any]) -> str:  # type: ignore
            import inspect
            signature = inspect.signature(func)
            stub_params = {}
            param_strs = []
            for name, param in signature.parameters.items():
                if param.annotation == int:
                    stub_params[name] = PyLegendInteger(PyLegendIntegerVariableExpression(name))
                    param_strs.append(f"{name}:Integer[0..1]")
                elif param.annotation == str:
                    stub_params[name] = PyLegendString(PyLegendStringVariableExpression(name))  # type: ignore
                    param_strs.append(f"{name}:String[0..1]")
                else:
                    raise RuntimeError("Not handled")
            return ("{" + (", ".join(param_strs)) + " | " +  # type: ignore
                    func(**stub_params).to_pure_expression(self.frame_to_pure_config) + "}")

        def my_func(var1: int, var2: str) -> int:
            return var1 + var2.length()  # type: ignore

        assert to_pure(my_func) == '{var1:Integer[0..1], var2:String[0..1] | (toOne($var1) + toOne($var2)->length())}'
