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
import uuid
from abc import ABC, abstractmethod

from pylegend._typing import (
    PyLegendList,
    PyLegendTuple
)


class PureExpression(ABC):
    @abstractmethod
    def compile(self, tds_row_alias: str) -> PyLegendTuple[PyLegendList[str], str]:
        pass  # pragma: no cover

    @staticmethod
    def from_raw_string(raw_string: str) -> "PureExpression":
        return StringPureExpression(raw_string)

    @staticmethod
    def from_prerequisite_expr(raw_string: str) -> "PureExpression":
        return StringPureExpression(raw_string)


class StringPureExpression(PureExpression):
    _pure_expr: str

    def __init__(self, pure_expr: str):
        self._pure_expr = pure_expr

    def compile(self, tds_row_alias: str) -> PyLegendTuple[PyLegendList[str], str]:
        return [], self._pure_expr


class PrerequisitePureExpression(PureExpression):
    _prerequisite_expr: str
    _column_name: str

    def __init__(self, prerequisite_expr: str, column_name: str):
        self._prerequisite_expr = prerequisite_expr
        self._column_name = column_name

        new_column_name = str(uuid.uuid4())
        self._change_column_name(new_column_name)

    def compile(self, tds_row_alias: str) -> PyLegendTuple[PyLegendList[str], str]:
        final_expr = f"${tds_row_alias}.{self._column_name}"
        return [self._prerequisite_expr], final_expr

    def _change_column_name(self, new_column_name) -> None:
        self._prerequisite_expr = self._prerequisite_expr.replace(self._column_name, new_column_name)
        self._column_name = new_column_name


class CompositePureExpression(PureExpression):
    _left: PureExpression
    _right: PureExpression
    _operation: str

    def __init__(self, left: PureExpression, right: PureExpression, operation: str):
        self._left = left
        self._right = right
        self._operation = operation

    def compile(self, tds_row_alias: str) -> PyLegendTuple[PyLegendList[str], str]:
        prerequisites_left, expression_left = self._left.compile(tds_row_alias)
        prerequisites_right, expression_right = self._right.compile(tds_row_alias)

        combined_prerequisites = prerequisites_left + prerequisites_right
        combined_expression = f"{expression_left} {self._operation} {expression_right}"

        return combined_prerequisites, combined_expression
