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

from pylegend._typing import (
    PyLegendList,
    PyLegendOptional,
)


class PrerequisitePureExpression:
    _pure_expr: str
    _column_alias: str

    def __init__(self, pure_expr: str, column_alias: str):
        self._pure_expr = pure_expr
        self._column_alias = column_alias

    def get_prerequisite_expr(self, column_name: PyLegendOptional[str]=None):
        if column_name is not None:
            return self._pure_expr.replace(self._column_alias, column_name)
        return self._pure_expr


class PureExpression:
    _prerequisite_expr_list: PyLegendList[PrerequisitePureExpression]
    _final_expr: str
    _internal_column_name: str

    def __init__(
            self,
            prerequisite_expr_list: PyLegendList[PrerequisitePureExpression],
            final_expr: str,
            internal_column_name: str = "__INTERNAL_COLUMN_NAME__",
    ):
        self._prerequisite_expr_list = prerequisite_expr_list
        self._final_expr = final_expr
        self._internal_column_name = internal_column_name

    def get_all_prerequisite_exprs(self) -> PyLegendList[str]:
        exprs: PyLegendList[str] = []
        for i, prerequisite_expr in enumerate(self._prerequisite_expr_list):
            column_name = self._internal_column_name + str(i)
            exprs.append(prerequisite_expr.get_prerequisite_expr(column_name))
        return exprs

    def get_final_expr(self, tds_row_alias: str) -> str:  # not implemented properly
        column_name = self._internal_column_name + '1'
        return f"${tds_row_alias}.{column_name}"


def combine_pure_expressions(
        expr1: PureExpression,
        expr2: PureExpression,
        operation: str
) -> PureExpression:
    pass