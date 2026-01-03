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

from pylegend._typing import (
    PyLegendCallable,
    PyLegendHashable,
    PyLegendList,
    PyLegendOptional,
    PyLegendSequence,
    PyLegendTuple,
    PyLegendUnion
)
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiDirectSortInfo,
    PandasApiPartialFrame,
    PandasApiSortDirection,
    PandasApiWindow,
    PandasApiWindowReference
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.helpers import (
    escape_column_name,
    generate_pure_lambda,
)
from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.sql.metamodel import (
    Expression,
    IntegerLiteral,
    QualifiedName,
    QualifiedNameReference,
    QuerySpecification,
    SelectItem,
    SingleColumn,
)
from pylegend.core.sql.metamodel_extension import WindowExpression
from pylegend.core.tds.pandas_api.frames.functions.shift_function import ShiftFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig


class DiffFunction(ShiftFunction):
    __base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame]
    __periods: int
    __axis: PyLegendUnion[int, str]

    _column_expression_and_window_tuples: PyLegendList[
        PyLegendTuple[
            PyLegendTuple[str, PyLegendPrimitive],
            PandasApiWindow
        ]
    ]
    _zero_column_name: str
    _temp_column_name_suffix: str

    @classmethod
    def name(cls) -> str:
        return "shift"  # pragma: no cover

    def __init__(
            self,
            base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame],
            periods: int = 1,
            axis: PyLegendUnion[int, str] = 0
    ) -> None:
        self.__base_frame = base_frame
        self.__periods = periods
        self.__axis = axis

        self._zero_column_name = "__pylegend_internal_column_name__"
        self._temp_column_name_suffix = "__pylegend_internal_column_name__"


    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:

        source_columns: PyLegendSequence["TdsColumn"]

        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            grouping_column_names = set([col.get_name() for col in self.__base_frame.get_grouping_columns()])
            selected_columns: PyLegendOptional[PyLegendList[TdsColumn]] = self.__base_frame.get_selected_columns()

            if selected_columns is None:
                source_columns = []
                for col in self.base_frame().columns():
                    if col.get_name() in grouping_column_names:
                        continue
                    source_columns.append(col)
            else:
                source_columns = selected_columns
        else:
            source_columns = self.base_frame().columns()

        return source_columns

    def validate(self) -> bool:

        valid_periods: PyLegendList[int] = [1, -1]
        if self.__periods not in valid_periods:
            raise NotImplementedError(
                f"The 'periods' argument of the shift function is only supported for the values of {valid_periods}"
                f" or a list of these, but got: periods={self.__periods!r}")

        if self.__axis not in [0, "index"]:
            raise NotImplementedError(
                f"The 'axis' argument of the shift function must be 0 or 'index', but got: axis={self.__axis!r}")

        self._column_expression_and_window_tuples = self._construct_column_expression_and_window_tuples()

        return True

    def _construct_column_expression_and_window_tuples(self) -> PyLegendList[
        PyLegendTuple[
            PyLegendTuple[str, PyLegendPrimitive],
            PandasApiWindow
        ]
    ]:
        column_names: list[str] = []
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            grouping_column_names = set([col.get_name() for col in self.__base_frame.get_grouping_columns()])
            selected_columns: PyLegendOptional[PyLegendList[TdsColumn]] = self.__base_frame.get_selected_columns()

            if selected_columns is None:
                for col in self.base_frame().columns():
                    if col.get_name() in grouping_column_names:
                        continue
                    column_names.append(col.get_name())
            else:
                column_names = [col.get_name() for col in selected_columns]
        else:
            column_names = [col.get_name() for col in self.base_frame().columns()]

        periods_list: PyLegendList[int] = [self.__periods] if isinstance(self.__periods, int) else list(self.__periods)

        extend_columns: PyLegendList[
            PyLegendTuple[
                str,
                PyLegendCallable[
                    [PandasApiPartialFrame, PandasApiWindowReference, PandasApiTdsRow],
                    PyLegendPrimitive
                ]
            ]
        ] = []

        for period in periods_list:
            for column_name in column_names:
                if isinstance(self.__periods, int):
                    current_col_name = column_name
                else:
                    suffix = self.__suffix if self.__suffix is not None else ""
                    current_col_name = f"{column_name}{suffix}_{period}"

                if period > 0:
                    def lambda_func(
                            p: PandasApiPartialFrame,
                            w: PandasApiWindowReference,
                            r: PandasApiTdsRow,
                            column_name: str = column_name,
                            period: int = period
                    ) -> PyLegendPrimitive:
                        return p[column_name] - p.lag(r, period)[column_name]
                else:
                    def lambda_func(
                            p: PandasApiPartialFrame,
                            w: PandasApiWindowReference,
                            r: PandasApiTdsRow,
                            column_name: str = column_name,
                            period: int = period
                    ) -> PyLegendPrimitive:
                        return p[column_name] - p.lead(r, -period)[column_name]

                extend_columns.append((current_col_name, lambda_func))

        tds_row = PandasApiTdsRow.from_tds_frame("r", self.base_frame())
        partial_frame = PandasApiPartialFrame(base_frame=self.base_frame(), var_name="p")

        column_expression_and_window_tuples: PyLegendList[
            PyLegendTuple[
                PyLegendTuple[str, PyLegendPrimitive],
                PandasApiWindow
            ]
        ] = []

        for extend_column in extend_columns:
            current_column_name: str = extend_column[0]

            partition_by: PyLegendOptional[PyLegendList[str]] = None
            if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
                partition_by = [col.get_name() for col in self.__base_frame.get_grouping_columns()]

            order_by = PandasApiDirectSortInfo(self._zero_column_name, PandasApiSortDirection.ASC)

            window = PandasApiWindow(partition_by, [order_by], frame=None)

            window_ref = PandasApiWindowReference(window=window, var_name="w")
            result: PyLegendPrimitive = extend_column[1](partial_frame, window_ref, tds_row)
            column_expression = (current_column_name, result)

            column_expression_and_window_tuples.append((column_expression, window))

        return column_expression_and_window_tuples
