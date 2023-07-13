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

from abc import ABCMeta, abstractmethod
from datetime import datetime, date


from typing import Union
from pylegend._typing import (
    PyLegendSequence,
)

__all__: PyLegendSequence[str] = [
    "ApiTdsFrame"
]

from pylegend.core.sql.metamodel import (
    QualifiedNameReference,
    ArithmeticExpression,
    IntegerLiteral,
    ArithmeticType,
    DoubleLiteral,
    BooleanLiteral,
    LogicalBinaryExpression,
    LogicalBinaryType,
    NotExpression,
    StringLiteral,
    FunctionCall,
    ComparisonExpression,
    ComparisonOperator,
    Extract,
    ExtractField,
    IntervalLiteral,
    QualifiedName,
    Expression,
    QualifiedNameReference,
    FunctionCall
)
from pylegend._typing import (
    PyLegendSequence,
    PyLegendCallable,
    PyLegendTypeVar,
    PyLegendIterator,
    PyLegendList,
    PyLegendOptional,
)


from pylegend.core.tds.tds_column import TdsColumn

R = PyLegendTypeVar('R')


class PyLegendAbstract(metaclass=ABCMeta):

    def __init__(self, value):  # type: ignore[no-untyped-def]
        self.value = value

    @abstractmethod
    def _handle_binary_function(self, operation, other, reverse=False) -> 'PyLegendAbstract':  # type: ignore[misc]
        pass

    def __eq__(self, other: Union[bool, int, float, str, datetime, date, 'PyLegendAbstract']) -> 'PyLegendBoolean':  # type: ignore[override]
        return self._handle_binary_function(ComparisonOperator.EQUAL, other)  # type: ignore[no-untyped-call]

    def equals(self, other: Union[bool, int, float, str, datetime, date, 'PyLegendAbstract']) -> 'PyLegendBoolean':
        return self.__eq__(other)


class PyLegendBoolean(PyLegendAbstract, metaclass=ABCMeta):

    def _handle_binary_function(self, operation: Union[LogicalBinaryType, ComparisonOperator], other, reverse=False) -> 'PyLegendBoolean':  # type: ignore[no-untyped-def]
        if isinstance(other, bool):
            right = BooleanLiteral(other)
        elif isinstance(other, PyLegendBoolean):
            right = other.value
        else:
            raise TypeError('Not supported yet!')
        if isinstance(operation, LogicalBinaryType):
            if reverse:
                return PyLegendBoolean(LogicalBinaryExpression(type_=operation, left=right, right=self.value))  # type: ignore[no-untyped-call]
            return PyLegendBoolean(LogicalBinaryExpression(type_=operation, left=self.value, right=right))  # type: ignore[no-untyped-call]
        elif isinstance(operation, ComparisonOperator):
            return PyLegendBoolean(ComparisonExpression(self.value, right, operation))  # type: ignore[no-untyped-call]

    def _handle_unary_function(self) -> 'PyLegendBoolean':
        return PyLegendBoolean(NotExpression(self.value))  # type: ignore[no-untyped-call]

    def __or__(self, other: Union[bool, 'PyLegendBoolean']) -> 'PyLegendBoolean':
        return self._handle_binary_function(LogicalBinaryType.OR, other, reverse=False)

    def __ror__(self, other: Union[bool, 'PyLegendBoolean']) -> 'PyLegendBoolean':
        return self._handle_binary_function(LogicalBinaryType.OR, other, reverse=False)

    def __and__(self, other: Union[bool, 'PyLegendBoolean']) -> 'PyLegendBoolean':
        return self._handle_binary_function(LogicalBinaryType.AND, other, reverse=False)

    def __rand__(self, other: Union[bool, 'PyLegendBoolean']) -> 'PyLegendBoolean':
        return self._handle_binary_function(LogicalBinaryType.AND, other, reverse=False)

    def __invert__(self) -> 'PyLegendBoolean':
        return self._handle_unary_function()


class PyLegendString(PyLegendAbstract, metaclass=ABCMeta):

    def _handle_binary_function(self, operation: ComparisonOperator, other: Union[str, 'PyLegendString'], reverse=False) -> PyLegendBoolean:  # type: ignore[no-untyped-def]
        if isinstance(other, str):
            right = StringLiteral(other, quoted=False)
        elif isinstance(other, PyLegendString):
            right = other.value
        else:
            raise TypeError('Not supported yet')
        return PyLegendBoolean(ComparisonExpression(self.value, right, operation))  # type: ignore[no-untyped-call]

    def _handle_binary_function_call(self, function_call: list[str], other: Union[str, list, 'PyLegendString'], reverse=False) -> FunctionCall:  # type: ignore[no-untyped-def]
        if isinstance(other, str):
            right = StringLiteral(other, quoted=False)
        elif isinstance(other, PyLegendString):
            right = other.value
        else:
            raise TypeError('Not supported yet')
        if reverse:
            return FunctionCall(QualifiedName(function_call), False, [right, self.value])
        return FunctionCall(QualifiedName(function_call), False, [self.value, right])

    def _handle_unary_function(self, function_call: list[str]) -> FunctionCall:
        return FunctionCall(QualifiedName(function_call), False, self.value)

    def __add__(self, other: Union[str, 'PyLegendString']) -> 'PyLegendString':
        return PyLegendString(self._handle_binary_function_call(['concat'], other))  # type: ignore[no-untyped-call]

    def __radd__(self, other: Union[str, 'PyLegendString']) -> 'PyLegendString':
        return PyLegendString(self._handle_binary_function_call(['concat'], other, reverse=True))  # type: ignore[no-untyped-call]

    # @abstractmethod
    # def __getitem__(self, item) -> 'PyLegendString':
    #     pass

    def substring(self, start: Union[int, 'PyLegendNumber'], end: Union[int, 'PyLegendNumber']) -> 'PyLegendString':
        return PyLegendString(self._handle_binary_function_call(['substring'], [start, end]))  # type: ignore[no-untyped-call]

    def index(self, search_string: Union[str, 'PyLegendString']) -> 'PyLegendInteger':
        return PyLegendInteger(self._handle_binary_function_call(['strpos'], search_string))  # type: ignore[no-untyped-call]

    def len(self) -> 'PyLegendInteger':
        return PyLegendInteger(self._handle_unary_function(['char_length']))  # type: ignore[no-untyped-call]

    def parse_int(self) -> 'PyLegendInteger':
        return PyLegendInteger(IntegerLiteral(int(self.value)))  # type: ignore[no-untyped-call]

    def parse_integer(self) -> 'PyLegendInteger':
        return self.parse_int()

    def parse_bool(self) -> 'PyLegendBoolean':
        raise RuntimeError("Not supported yet!")

    def parse_boolean(self) -> 'PyLegendBoolean':
        return self.parse_bool()

    def parse_float(self) -> 'PyLegendFloat':
        return PyLegendFloat(DoubleLiteral(float(self.value)))  # type: ignore[no-untyped-call]

    def parse_date(self) -> 'PyLegendDate':
        raise RuntimeError("Not supported yet!")

    def endswith(self, other: Union[str, 'PyLegendString']) -> None:
        pass

    def startswith(self, other: Union[str, 'PyLegendString']) -> 'PyLegendBoolean':
        return PyLegendBoolean(self._handle_binary_function_call(['starts_with'], other))  # type: ignore[no-untyped-call]

    def __contains__(self, other: Union[str, 'PyLegendString']) -> 'PyLegendBoolean':
        return PyLegendBoolean(self._handle_binary_function_call(['regexp_like'], other))  # type: ignore[no-untyped-call]

    def string_contains(self, other: Union[str, 'PyLegendString']) -> 'PyLegendBoolean':
        return self.__contains__(other)

    def lower(self) -> 'PyLegendString':
        return PyLegendString(self._handle_unary_function(['lower']))  # type: ignore[no-untyped-call]

    def upper(self) -> 'PyLegendString':
        return PyLegendString(self._handle_unary_function(['upper']))  # type: ignore[no-untyped-call]

    def strip(self) -> 'PyLegendString':
        return PyLegendString(self._handle_unary_function(['btrim']))  # type: ignore[no-untyped-call]

    def __gt__(self, other: Union[str, 'PyLegendString']) -> 'PyLegendBoolean':
        return self._handle_binary_function(ComparisonOperator.GREATER_THAN, other)

    def __ge__(self, other: Union[str, 'PyLegendString']) -> 'PyLegendBoolean':
        return self._handle_binary_function(ComparisonOperator.GREATER_THAN_OR_EQUAL, other)

    def __lt__(self, other: Union[str, 'PyLegendString']) -> 'PyLegendBoolean':
        return self._handle_binary_function(ComparisonOperator.LESS_THAN, other)

    def __le__(self, other: Union[str, 'PyLegendString']) -> 'PyLegendBoolean':
        return self._handle_binary_function(ComparisonOperator.LESS_THAN_OR_EQUAL, other)


class PyLegendNumber(PyLegendAbstract, metaclass=ABCMeta):

    def _handle_binary_function(self, operation: Union[ArithmeticType, ComparisonOperator], other: Union[int, float, 'PyLegendNumber'], reverse=False) -> Union[PyLegendBoolean, ArithmeticExpression]:  # type: ignore[override]
        if isinstance(other, int):
            right = IntegerLiteral(other)
        elif isinstance(other, PyLegendInteger):
            right = IntegerLiteral(other.value)
        elif isinstance(other, float):
            right = DoubleLiteral(other)  # type: ignore[assignment]
        elif isinstance(other, PyLegendFloat):
            right = DoubleLiteral(other.value)  # type: ignore[assignment]
        elif isinstance(other, PyLegendNumber):
            right = DoubleLiteral(other.value)  # type: ignore[assignment]
        else:
            raise TypeError('Not supported yet!')
        if isinstance(operation, ArithmeticType):
            if reverse:
                return ArithmeticExpression(type_=operation, left=right, right=self.value)
            return ArithmeticExpression(type_=operation, left=self.value, right=right)
        elif isinstance(operation, ComparisonOperator):
            return PyLegendBoolean(ComparisonExpression(self.value, right, operation))  # type: ignore[no-untyped-call]

    def _handle_binary_function_call(self, function_call: list[str], other: Union[int, float, 'PyLegendNumber'], reverse = False) -> FunctionCall:  # type: ignore[no-untyped-def]
        if isinstance(other, int):
            right = IntegerLiteral(other)
        elif isinstance(other, PyLegendInteger):
            right = IntegerLiteral(other.value)
        elif isinstance(other, float):
            right = DoubleLiteral(other)  # type: ignore[assignment]
        elif isinstance(other, PyLegendFloat):
            right = DoubleLiteral(other.value)  # type: ignore[assignment]
        elif isinstance(other, PyLegendNumber):
            right = DoubleLiteral(other.value)  # type: ignore[assignment]
        else:
            raise TypeError('Not supported yet!')
        if reverse:
            return FunctionCall(QualifiedName(function_call), False, [right, self.value])
        return FunctionCall(QualifiedName(function_call), False, [self.value, right])

    def _handle_unary_function_call(self, function_call: list[str]) -> FunctionCall:
        return FunctionCall(QualifiedName(function_call), False, self.value)

    def __add__(self, other: Union[int, float, 'PyLegendNumber']) -> 'PyLegendNumber':
        return PyLegendNumber(self._handle_binary_function(ArithmeticType.ADD, other, reverse=False))  # type: ignore[no-untyped-call]

    def __radd__(self, other: Union[int, float, 'PyLegendNumber']) -> 'PyLegendNumber':
        return PyLegendNumber(self._handle_binary_function(ArithmeticType.ADD, other, reverse=True))  # type: ignore[no-untyped-call]

    def __pos__(self) -> 'PyLegendNumber':
        return PyLegendNumber(self)  # type: ignore[no-untyped-call]

    def __sub__(self, other: Union[int, float, 'PyLegendNumber']) -> 'PyLegendNumber':
        return PyLegendNumber(self._handle_binary_function(ArithmeticType.SUBTRACT, other, reverse=False))  # type: ignore[no-untyped-call]

    def __rsub__(self, other: Union[int, float, 'PyLegendNumber']) -> 'PyLegendNumber':
        return PyLegendNumber(self._handle_binary_function(ArithmeticType.SUBTRACT, other, reverse=True))  # type: ignore[no-untyped-call]

    def __neg__(self) -> 'PyLegendNumber':
        return PyLegendNumber(self * -1)  # type: ignore[no-untyped-call]

    def __mul__(self, other: Union[int, float, 'PyLegendNumber']) -> 'PyLegendNumber':
        return PyLegendNumber(self._handle_binary_function(ArithmeticType.MULTIPLY, other, reverse=False))  # type: ignore[no-untyped-call]

    def __rmul__(self, other: Union[int, float, 'PyLegendNumber']) -> 'PyLegendNumber':
        return PyLegendNumber(self._handle_binary_function(ArithmeticType.MULTIPLY, other, reverse=True))  # type: ignore[no-untyped-call]

    def __pow__(self, other: Union[int, float, 'PyLegendNumber']) -> 'PyLegendNumber':
        return PyLegendNumber(self._handle_binary_function_call(['power'], other))  # type: ignore[no-untyped-call]

    def __rpow__(self, other: Union[int, float, 'PyLegendNumber']) -> 'PyLegendNumber':
        return PyLegendNumber(self._handle_binary_function_call(['power'], other, reverse = True))  # type: ignore[no-untyped-call]

    def __abs__(self) -> 'PyLegendNumber':
        return PyLegendNumber(self._handle_unary_function_call(['abs']))  # type: ignore[no-untyped-call]

    def __round__(self, n: int) -> 'PyLegendNumber':
        return PyLegendNumber(self._handle_binary_function_call(['round'], n))  # type: ignore[no-untyped-call]

    def round(self, n: int) -> 'PyLegendNumber':
        return self.__round__(n)

    def __ceil__(self) -> 'PyLegendInteger':
        return PyLegendInteger(self._handle_unary_function_call(['ceiling']))  # type: ignore[no-untyped-call]

    def ceil(self) -> 'PyLegendInteger':
        return self.__ceil__()

    def __floor__(self) -> 'PyLegendInteger':
        return PyLegendInteger(self._handle_unary_function_call(['floor']))  # type: ignore[no-untyped-call]

    def floor(self) -> 'PyLegendInteger':
        return self.__floor__()

    def sqrt(self) -> 'PyLegendFloat':
        return PyLegendFloat(self._handle_unary_function_call(['sqrt']))  # type: ignore[no-untyped-call]

    def exp(self) -> 'PyLegendFloat':
        return PyLegendFloat(self._handle_unary_function_call(['exp']))  # type: ignore[no-untyped-call]

    def log(self) -> 'PyLegendFloat':
        return PyLegendFloat(self._handle_unary_function_call(['ln']))  # type: ignore[no-untyped-call]

    def sin(self) -> 'PyLegendFloat':
        return PyLegendFloat(self._handle_unary_function_call(['sin']))  # type: ignore[no-untyped-call]

    def asin(self) -> 'PyLegendFloat':
        return PyLegendFloat(self._handle_unary_function_call(['asin']))  # type: ignore[no-untyped-call]

    def cos(self) -> 'PyLegendFloat':
        return PyLegendFloat(self._handle_unary_function_call(['cos']))  # type: ignore[no-untyped-call]

    def acos(self) -> 'PyLegendFloat':
        raise RuntimeError("Not supported yet!")
        # return self._handle_unary_function(self._unary_number_operations().ArcCos)

    def tan(self) -> 'PyLegendFloat':
        return PyLegendFloat(self._handle_unary_function_call(['tan']))  # type: ignore[no-untyped-call]

    def atan(self) -> 'PyLegendFloat':
        return PyLegendFloat(self._handle_unary_function_call(['atan']))  # type: ignore[no-untyped-call]

    def atan2(self, other: Union[int, float, 'PyLegendNumber']) -> 'PyLegendFloat':
        return PyLegendFloat(self._handle_binary_function_call(['round'], other))  # type: ignore[no-untyped-call]

    def __gt__(self, other: Union[int, float, 'PyLegendNumber']) -> 'PyLegendBoolean':
        return self._handle_binary_function(ComparisonOperator.GREATER_THAN, other)  # type: ignore[return-value]

    def __ge__(self, other: Union[int, float, 'PyLegendNumber']) -> 'PyLegendBoolean':
        return self._handle_binary_function(ComparisonOperator.GREATER_THAN_OR_EQUAL, other)  # type: ignore[return-value]

    def __lt__(self, other: Union[int, float, 'PyLegendNumber']) -> 'PyLegendBoolean':
        return self._handle_binary_function(ComparisonOperator.LESS_THAN, other)  # type: ignore[return-value]

    def __le__(self, other: Union[int, float, 'PyLegendNumber']) -> 'PyLegendBoolean':
        return self._handle_binary_function(ComparisonOperator.LESS_THAN_OR_EQUAL, other)  # type: ignore[return-value]


class PyLegendInteger(PyLegendNumber, metaclass=ABCMeta):
    pass


class PyLegendFloat(PyLegendNumber):
    pass


class PyLegendDate(PyLegendAbstract, metaclass=ABCMeta):

    def _handle_binary_function(self, operation: ComparisonOperator, other: Union[date, 'PyLegendDate'], reverse=False) -> 'PyLegendBoolean':  # type: ignore[no-untyped-def]
        if isinstance(other, datetime):
            right = StringLiteral(str(other))
        elif isinstance(other, PyLegendDate):
            right = StringLiteral(other.value)
        else:
            raise TypeError('Not supported yet')
        return PyLegendBoolean(ComparisonExpression(self.value, right, operation))  # type: ignore[no-untyped-call]

    def year(self) -> 'PyLegendInteger':
        return PyLegendInteger(Extract(self.value, ExtractField.YEAR))  # type: ignore[no-untyped-call]

    def month_number(self) -> 'PyLegendInteger':
        return PyLegendInteger(Extract(self.value, ExtractField.MONTH))  # type: ignore[no-untyped-call]

    def monthNumber(self) -> 'PyLegendInteger':
        return self.month_number()

    def quarter_number(self) -> 'PyLegendInteger':
        return PyLegendInteger(Extract(self.value, ExtractField.QUARTER))  # type: ignore[no-untyped-call]

    def quarterNumber(self) -> 'PyLegendInteger':
        return self.quarter_number()

    def day_of_week_number(self) -> 'PyLegendInteger':
        return PyLegendInteger(Extract(self.value, ExtractField.DAY_OF_WEEK))  # type: ignore[no-untyped-call]

    def dayOfWeekNumber(self) -> 'PyLegendInteger':
        return self.day_of_week_number()

    def day_of_month(self) -> 'PyLegendInteger':
        return PyLegendInteger(Extract(self.value, ExtractField.DAY_OF_MONTH))  # type: ignore[no-untyped-call]

    def dayOfMonth(self) -> 'PyLegendInteger':
        return self.day_of_month()

    def week_of_year(self) -> 'PyLegendInteger':
        return PyLegendInteger(Extract(self.value, ExtractField.WEEK))  # type: ignore[no-untyped-call]

    def weekOfYear(self) -> 'PyLegendInteger':
        return self.week_of_year()

    def hour(self) -> 'PyLegendInteger':
        return PyLegendInteger(Extract(self.value, ExtractField.HOUR))  # type: ignore[no-untyped-call]

    def minute(self) -> 'PyLegendInteger':
        return PyLegendInteger(Extract(self.value, ExtractField.MINUTE))  # type: ignore[no-untyped-call]

    def second(self) -> 'PyLegendInteger':
        return PyLegendInteger(Extract(self.value, ExtractField.SECOND))  # type: ignore[no-untyped-call]

    def date_part(self) -> None:
        pass

    def datePart(self) -> None:
        pass

    def date_diff(self, other: Union[datetime, 'PyLegendDate'], duration_unit: None) -> None:
        pass

    def adjust(self, number: int, duration_unit: None) -> None:
        pass

    def firstDayOfYear(self) -> None:
        pass

    @property
    def firstDayOfMonth(self) -> None:
        pass

    def firstDayOfWeek(self) -> None:
        pass

    def firstDayOfQuarter(self) -> None:
        pass

    def __gt__(self, other: Union[date, 'PyLegendDate']) -> 'PyLegendBoolean':
        return self._handle_binary_function(ComparisonOperator.GREATER_THAN, other)

    def __ge__(self, other: Union[date, 'PyLegendDate']) -> 'PyLegendBoolean':
        return self._handle_binary_function(ComparisonOperator.GREATER_THAN_OR_EQUAL, other)

    def __lt__(self, other: Union[date, 'PyLegendDate']) -> 'PyLegendBoolean':
        return self._handle_binary_function(ComparisonOperator.LESS_THAN, other)

    def __le__(self, other: Union[date, 'PyLegendDate']) -> 'PyLegendBoolean':
        return self._handle_binary_function(ComparisonOperator.LESS_THAN_OR_EQUAL, other)


class PyLegendDateTime(PyLegendDate, metaclass=ABCMeta):
    pass


class PyLegendStrictDate(PyLegendDate, metaclass=ABCMeta):
    pass


class TdsFrameRow(metaclass=ABCMeta):
    def __init__(self, columns, row_param: str):  # type: ignore[no-untyped-def]
        self.__columns = columns
        self.__row_param = row_param

    def __get_column(self, column):  # type: ignore[no-untyped-def]
        # if not isinstance(column, str):
        #     raise TypeError('\'get\' functions expect column to be a string')
        for x in self.__columns():
            if x.get_name() == column:
                return x
        raise TypeError('Column \'{0}\' not found in the TDS. Found columns : [{1}]'.format(
            column,
            ', '.join(['\'' + x.__name + '\'' for x in self.__columns()])
        ))

    def __getitem__(self, item) -> "PyLegendAbstract":  # type: ignore[no-untyped-def]
        col = self.__get_column(item)  # type: ignore[no-untyped-call]
        if col.get_type() == 'Integer':
            return PyLegendInteger(QualifiedNameReference(QualifiedName([item])))  # type: ignore[no-untyped-call]
        elif col.get_type() == 'String':
            return PyLegendString(QualifiedNameReference(QualifiedName([item])))  # type: ignore[no-untyped-call]
        elif col.get_type() == 'Boolean':
            return PyLegendBoolean(QualifiedNameReference(QualifiedName([item])))  # type: ignore[no-untyped-call]
        elif col.get_type() == 'Date':
            return PyLegendDate(QualifiedNameReference(QualifiedName([item])))  # type: ignore[no-untyped-call]
        elif col.get_type() == 'Float':
            return PyLegendFloat(QualifiedNameReference(QualifiedName([item])))  # type: ignore[no-untyped-call]
        elif col.get_type() == 'Number':
            return PyLegendNumber(QualifiedNameReference(QualifiedName([item])))  # type: ignore[no-untyped-call]
        else:
            raise TypeError('not found')
