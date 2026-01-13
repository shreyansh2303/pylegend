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

from copy import deepcopy
from pylegend._typing import (
    PyLegendCallable,
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
    ArithmeticExpression,
    ArithmeticType,
    Expression,
    IntegerLiteral,
    QualifiedName,
    QualifiedNameReference,
    QuerySpecification,
    SelectItem,
    SingleColumn,
)
from pylegend.core.sql.metamodel_extension import WindowExpression
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig


class DiffFunction(PandasApiAppliedFunction):
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
        return "diff"  # pragma: no cover

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

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:

        base_query = self.base_frame().to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()

        base_query.select.selectItems.append(
            SingleColumn(alias=db_extension.quote_identifier(self._zero_column_name), expression=IntegerLiteral(0))
        )

        new_query: QuerySpecification = create_sub_query(base_query, config, "root")
        new_select_items: list[SelectItem] = deepcopy(base_query.select.selectItems[:-1])

        def generate_lambda_func(column_name: str) -> PyLegendCallable[
                [PandasApiPartialFrame, PandasApiWindowReference, PandasApiTdsRow],
                PyLegendPrimitive
        ]:
            if self.__periods > 0:
                def lambda_func(
                        p: PandasApiPartialFrame,
                        w: PandasApiWindowReference,
                        r: PandasApiTdsRow
                ) -> PyLegendPrimitive:
                    return p.lag(r, self.__periods)[column_name]
            else:
                def lambda_func(
                        p: PandasApiPartialFrame,
                        w: PandasApiWindowReference,
                        r: PandasApiTdsRow
                ) -> PyLegendPrimitive:
                    return p.lead(r, -self.__periods)[column_name]
            return lambda_func

        self._column_expression_and_window_tuples = self._construct_column_expression_and_window_tuples(generate_lambda_func)

        for c, window in self._column_expression_and_window_tuples:
            col_sql_expr: Expression = c[1].to_sql_expression({"r": new_query}, config)
            window_expr = WindowExpression(
                nested=col_sql_expr,
                window=window.to_sql_node(new_query, config)
            )

            new_select_items.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(c[0] + self._temp_column_name_suffix),
                    expression=window_expr
                )
            )

        new_query.select.selectItems = new_select_items

        new_query = create_sub_query(new_query, config, "root")

        final_select_items: list[SelectItem] = []
        for col in self.calculate_columns():
            col_name: str = col.get_name()
            left_column = QualifiedNameReference(QualifiedName([
                db_extension.quote_identifier("root"),
                db_extension.quote_identifier(col_name)
            ]))
            right_column = QualifiedNameReference(QualifiedName([
                db_extension.quote_identifier("root"),
                db_extension.quote_identifier(col_name + self._temp_column_name_suffix)
            ]))
            final_expression = ArithmeticExpression(
                ArithmeticType.SUBTRACT,
                left_column,
                right_column
            )

            final_select_items.append(
                SingleColumn(
                    alias=db_extension.quote_identifier(col_name),
                    expression=final_expression
                )
            )

        new_query.select.selectItems = final_select_items

        return new_query
    
    def to_pure(self, config: FrameToPureConfig) -> str:

        def generate_lambda_func(column_name: str) -> PyLegendCallable[
                [PandasApiPartialFrame, PandasApiWindowReference, PandasApiTdsRow],
                PyLegendPrimitive
        ]:
            if self.__periods > 0:
                def lambda_func(
                        p: PandasApiPartialFrame,
                        w: PandasApiWindowReference,
                        r: PandasApiTdsRow
                ) -> PyLegendPrimitive:
                    return r[column_name] - p.lag(r, self.__periods)[column_name]
            else:
                def lambda_func(
                        p: PandasApiPartialFrame,
                        w: PandasApiWindowReference,
                        r: PandasApiTdsRow
                ) -> PyLegendPrimitive:
                    return r[column_name] - p.lead(r, -self.__periods)[column_name]
            return lambda_func

        self._column_expression_and_window_tuples = self._construct_column_expression_and_window_tuples(generate_lambda_func)

        def render_single_column_expression(c: PyLegendTuple[str, PyLegendPrimitive]) -> str:
            escaped_col_name: str = escape_column_name(c[0] + self._temp_column_name_suffix)
            expr_str: str = c[1].to_pure_expression(config)
            return f"{escaped_col_name}:{generate_pure_lambda('p,w,r', expr_str)}"

        extend_0_column = f"->extend(~{self._zero_column_name}:{{r|0}})"

        extend_strs: PyLegendList[str] = []
        for c, window in self._column_expression_and_window_tuples:
            window_expression: str = window.to_pure_expression(config)
            extend_strs.append(
                f"->extend({window_expression}, ~{render_single_column_expression(c)})"
            )
        extend_str: str = f"{config.separator(1)}".join(extend_strs)

        project_str = (
                "->project(~[" +
                ", ".join([f"{escape_column_name(c[0])}:p|$p.{escape_column_name(c[0] + self._temp_column_name_suffix)}"
                           for c, _ in self._column_expression_and_window_tuples]) +
                "])"
        )

        return (
                f"{self.base_frame().to_pure(config)}{config.separator(1)}"
                f"{extend_0_column}{config.separator(1)}"
                f"{extend_str}{config.separator(1)}"
                f"{project_str}"
        )


    def base_frame(self) -> PandasApiBaseTdsFrame:
        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            return self.__base_frame.base_frame()
        return self.__base_frame
    
    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:

        source_columns: PyLegendList["TdsColumn"]

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
            source_columns = list(self.base_frame().columns())

        return source_columns

    def validate(self) -> bool:

        valid_periods: PyLegendList[int] = [1, -1]
        if self.__periods not in valid_periods:
            raise NotImplementedError(
                f"The 'periods' argument of the diff function is only supported for values {valid_periods},"
                f" but got: periods={self.__periods!r}")

        if self.__axis not in [0, "index"]:
            raise NotImplementedError(
                f"The 'axis' argument of the diff function must be 0 or 'index', but got: axis={self.__axis!r}")

        valid_column_types = ["Integer", "Float", "Number"]
        mismatched_columns: PyLegendList[TdsColumn] = []
        for col in self.calculate_columns():
            if col.get_type() not in valid_column_types:
                mismatched_columns.append(col)

        if len(mismatched_columns) > 0:
            raise TypeError(
                f"The diff function can only be applied to the following column types: {valid_column_types}, "
                f"but got the following invalid columns: {[str(col) for col in mismatched_columns]}"
            )

        return True

    def _construct_column_expression_and_window_tuples(
            self,
            generate_lambda_func: PyLegendCallable[
                [str],
                PyLegendCallable[
                    [PandasApiPartialFrame, PandasApiWindowReference, PandasApiTdsRow],
                    PyLegendPrimitive
                ]
            ]
    ) -> PyLegendList[
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

        extend_columns: PyLegendList[
            PyLegendTuple[
                str,
                PyLegendCallable[
                    [PandasApiPartialFrame, PandasApiWindowReference, PandasApiTdsRow],
                    PyLegendPrimitive
                ]
            ]
        ] = []

        for column_name in column_names:
            lambda_func = generate_lambda_func(column_name)
            extend_columns.append((column_name, lambda_func))

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
