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

from abc import ABCMeta
from enum import Enum
from typing import TYPE_CHECKING
from pylegend.core.language import (
    PyLegendPrimitive,
    PyLegendBoolean,
    PyLegendString,
    PyLegendNumber,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendDate,
    PyLegendDateTime,
    PyLegendStrictDate,
)
from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
    PyLegendList,
    PyLegendDict,
)
from pylegend.core.language.shared.column_expressions import PyLegendColumnExpression
from pylegend.core.language.shared.expression import PyLegendExpressionFloatReturn, PyLegendExpressionIntegerReturn, \
    PyLegendExpressionNumberReturn, PyLegendExpression
from pylegend.core.language.shared.helpers import escape_column_name
from pylegend.core.sql.metamodel import (
    Expression,
    FunctionCall,
    QualifiedName,
    QuerySpecification,
    SingleColumn,
    SortItem,
    SortItemNullOrdering,
    SortItemOrdering,
    Window, WindowFrame, Node, WindowFrameMode, FrameBound, FrameBoundType
)
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig

__all__: PyLegendSequence[str] = [
    "PandasApiPrimitive",
    "PandasApiBoolean",
    "PandasApiString",
    "PandasApiNumber",
    "PandasApiInteger",
    "PandasApiFloat",
    "PandasApiDate",
    "PandasApiDateTime",
    "PandasApiStrictDate",
]


class PandasApiPrimitive(PyLegendPrimitive, metaclass=ABCMeta):
    pass


class PandasApiBoolean(PandasApiPrimitive, PyLegendBoolean):
    def __init__(self, expr: PyLegendBoolean):
        PyLegendBoolean.__init__(self, expr.value())


class PandasApiString(PandasApiPrimitive, PyLegendString):
    def __init__(self, expr: PyLegendString):
        PyLegendString.__init__(self, expr.value())


class PandasApiNumber(PandasApiPrimitive, PyLegendNumber):
    def __init__(self, expr: PyLegendNumber):
        PyLegendNumber.__init__(self, expr.value())


class PandasApiInteger(PandasApiPrimitive, PyLegendInteger):
    def __init__(self, expr: PyLegendInteger):
        PyLegendInteger.__init__(self, expr.value())


class PandasApiFloat(PandasApiPrimitive, PyLegendFloat):
    def __init__(self, expr: PyLegendFloat):
        PyLegendFloat.__init__(self, expr.value())


class PandasApiDate(PandasApiPrimitive, PyLegendDate):
    def __init__(self, expr: PyLegendDate):
        PyLegendDate.__init__(self, expr.value())


class PandasApiDateTime(PandasApiPrimitive, PyLegendDateTime):
    def __init__(self, expr: PyLegendDateTime):
        PyLegendDateTime.__init__(self, expr.value())


class PandasApiStrictDate(PandasApiPrimitive, PyLegendStrictDate):
    def __init__(self, expr: PyLegendStrictDate):
        PyLegendStrictDate.__init__(self, expr.value())


class PandasApiSortDirection(Enum):
    ASC = 1,
    DESC = 2


class PandasApiSortInfo:
    _column: str
    _direction: PandasApiSortDirection
    _null_ordering: SortItemNullOrdering

    def __init__(
            self,
            column_expr: PyLegendColumnExpression,
            direction: PandasApiSortDirection,
            null_ordering: SortItemNullOrdering = SortItemNullOrdering.UNDEFINED
    ) -> None:
        self._column = column_expr.get_column()
        self._direction = direction
        self._null_ordering = null_ordering

    def to_sql_node(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig
    ) -> SortItem:
        return SortItem(
            sortKey=self.__find_column_expression(query, config),
            ordering=(SortItemOrdering.ASCENDING if self._direction == PandasApiSortDirection.ASC
                      else SortItemOrdering.DESCENDING),
            nullOrdering=self._null_ordering
        )

    def __find_column_expression(self, query: QuerySpecification, config: FrameToSqlConfig) -> Expression:
        db_extension = config.sql_to_string_generator().get_db_extension()
        filtered = [
            s for s in query.select.selectItems
            if (isinstance(s, SingleColumn) and
                s.alias == db_extension.quote_identifier(self._column))
        ]
        if len(filtered) == 0:
            raise RuntimeError("Cannot find column: " + self._column)  # pragma: no cover
        return filtered[0].expression

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        func = 'ascending' if self._direction == PandasApiSortDirection.ASC else 'descending'
        return f"{func}(~{escape_column_name(self._column)})"


class PandasApiDirectSortInfo(PandasApiSortInfo):
    _column: str
    _direction: PandasApiSortDirection
    _null_ordering: SortItemNullOrdering

    def __init__(
            self,
            column_name: str,
            direction: PandasApiSortDirection,
            null_ordering: SortItemNullOrdering = SortItemNullOrdering.UNDEFINED
    ) -> None:
        self._column = column_name
        self._direction = direction
        self._null_ordering = null_ordering


class PandasApiFrameBoundType(Enum):
    UNBOUNDED_PRECEDING = 1,
    PRECEDING = 2,
    CURRENT_ROW = 3,
    FOLLOWING = 4,
    UNBOUNDED_FOLLOWING = 5

    def to_sql_node(self) -> FrameBoundType:
        pandas_api_to_sql_node_map = {
            PandasApiFrameBoundType.UNBOUNDED_PRECEDING: FrameBoundType.UNBOUNDED_PRECEDING,
            PandasApiFrameBoundType.PRECEDING: FrameBoundType.PRECEDING,
            PandasApiFrameBoundType.CURRENT_ROW: FrameBoundType.CURRENT_ROW,
            PandasApiFrameBoundType.FOLLOWING: FrameBoundType.FOLLOWING,
            PandasApiFrameBoundType.UNBOUNDED_FOLLOWING: FrameBoundType.UNBOUNDED_FOLLOWING
        }
        return pandas_api_to_sql_node_map[self]


class PandasApiFrameBound:
    type_: "PandasApiFrameBoundType"
    value: "PyLegendOptional[Expression]"

    def __init__(
            self,
            type_: "PandasApiFrameBoundType",
            value: "PyLegendOptional[Expression]"
    ) -> None:
        self.type_ = type_
        self.value = value

    def to_sql_node(self) -> FrameBound:
        return FrameBound(
            type_=self.type_.to_sql_node(),
            value=self.value
        )


class PandasApiWindowFrameMode(Enum):
    RANGE = 1,
    ROWS = 2

    def to_sql_node(self) -> WindowFrameMode:
        pandas_api_to_sql_node_map = {
            PandasApiWindowFrameMode.RANGE: WindowFrameMode.RANGE,
            PandasApiWindowFrameMode.ROWS: WindowFrameMode.ROWS
        }
        return pandas_api_to_sql_node_map[self]


class PandasApiWindowFrame:
    __mode: PandasApiWindowFrameMode
    __start: PandasApiFrameBound
    __end: PyLegendOptional[PandasApiFrameBound]

    def __init__(
            self,
            mode: PandasApiWindowFrameMode,
            start: PandasApiFrameBound,
            end: PyLegendOptional[PandasApiFrameBound]
    ):
        self.__mode = mode
        self.__start = start
        self.__end = end

    def to_sql_node(self):
        return WindowFrame(
            mode=self.__mode.to_sql_node(),
            start=self.__start.to_sql_node(),
            end=(
                None if self.__end is None else
                self.__end.to_sql_node()
            )
        )


class PandasApiWindow:
    __partition_by: PyLegendOptional[PyLegendList[str]]
    __order_by: PyLegendOptional[PyLegendList[PandasApiSortInfo]]
    __frame: PyLegendOptional[PandasApiWindowFrame]

    def __init__(
            self,
            partition_by: PyLegendOptional[PyLegendList[str]] = None,
            order_by: PyLegendOptional[PyLegendList[PandasApiSortInfo]] = None,
            frame: PyLegendOptional[PandasApiWindowFrame] = None
    ) -> None:
        self.__partition_by = partition_by
        self.__order_by = order_by
        self.__frame = frame

    def to_sql_node(
            self,
            query: QuerySpecification,
            config: FrameToSqlConfig
    ) -> Window:
        return Window(
            windowRef=None,
            partitions=(
                [] if self.__partition_by is None else
                [PandasApiWindow.__find_column_expression(query, col, config) for col in self.__partition_by]
            ),
            orderBy=(
                [] if self.__order_by is None else
                [sort_info.to_sql_node(query, config) for sort_info in self.__order_by]
            ),
            windowFrame=None
        )

    @staticmethod
    def __find_column_expression(query: QuerySpecification, col: str, config: FrameToSqlConfig) -> Expression:
        db_extension = config.sql_to_string_generator().get_db_extension()
        filtered = [
            s for s in query.select.selectItems
            if (isinstance(s, SingleColumn) and
                s.alias == db_extension.quote_identifier(col))
        ]
        if len(filtered) == 0:
            raise RuntimeError("Cannot find column: " + col)  # pragma: no cover
        return filtered[0].expression

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        partitions_str = (
            "" if self.__partition_by is None or len(self.__partition_by) == 0
            else "~[" + (', '.join(map(escape_column_name, self.__partition_by))) + "], "
        )
        sorts_str = (
            "[]" if self.__order_by is None or len(self.__order_by) == 0
            else "[" + (', '.join([s.to_pure_expression(config) for s in self.__order_by])) + "]"
        )
        return f"over({partitions_str}{sorts_str})"


class PandasApiPartialFrame:
    if TYPE_CHECKING:
        from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow

    __base_frame: "PandasApiBaseTdsFrame"
    __var_name: str

    def __init__(self, base_frame: "PandasApiBaseTdsFrame", var_name: str) -> None:
        self.__base_frame = base_frame
        self.__var_name = var_name

    def row_number(
            self,
            row: "PandasApiTdsRow"
    ) -> PyLegendInteger:
        return PyLegendInteger(PandasApiRowNumberExpression(self, row))

    def rank(
            self,
            window: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
    ) -> PyLegendInteger:
        return PyLegendInteger(PandasApiRankExpression(self, window, row))

    def dense_rank(
            self,
            window: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
    ) -> PyLegendInteger:
        return PyLegendInteger(PandasApiDenseRankExpression(self, window, row))

    def percent_rank(
            self,
            window: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
    ) -> PyLegendFloat:
        return PyLegendFloat(PandasApiPercentRankExpression(self, window, row))

    def lead(
            self,
            row: "PandasApiTdsRow",
            num_rows_to_lead_by: int = 1
    ) -> "PandasApiTdsRow":
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiLeadRow
        return PandasApiLeadRow(self, row, num_rows_to_lead_by)

    def lag(
            self,
            row: "PandasApiTdsRow",
            num_rows_to_lag_by: int = 1
    ) -> "PandasApiTdsRow":
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiLagRow
        return PandasApiLagRow(self, row, num_rows_to_lag_by)

    def sum(
            self,
            window: "PandasApiWindowReference",
            column: "PyLegendColumnExpression"
    ) -> PyLegendNumber:
        return PyLegendNumber(PandasApiWindowSumExpression(self, window, column))

    def min(
            self,
            window: "PandasApiWindowReference",
            column: "PyLegendColumnExpression"
    ) -> PyLegendNumber:
        return PyLegendNumber(PandasApiWindowMinExpression(self, window, column))

    def max(
            self,
            window: "PandasApiWindowReference",
            column: "PyLegendColumnExpression"
    ) -> PyLegendNumber:
        return PyLegendNumber(PandasApiWindowMaxExpression(self, window, column))

    def mean(
            self,
            window: "PandasApiWindowReference",
            column: "PyLegendColumnExpression"
    ) -> PyLegendFloat:
        return PyLegendFloat(PandasApiWindowMeanExpression(self, window, column))

    def count(
            self,
            window: "PandasApiWindowReference",
            column: "PyLegendColumnExpression"
    ) -> PyLegendInteger:
        return PyLegendInteger(PandasApiWindowCountExpression(self, window, column))

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"${self.__var_name}"

    def get_base_frame(self) -> "PandasApiBaseTdsFrame":
        return self.__base_frame


class PandasApiWindowReference:
    __window: PandasApiWindow
    __var_name: str

    def __init__(self, window: PandasApiWindow, var_name: str) -> None:
        self.__window = window
        self.__var_name = var_name

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"${self.__var_name}"


class PandasApiRowNumberExpression(PyLegendExpressionIntegerReturn):
    if TYPE_CHECKING:
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow

    __partial_frame: PandasApiPartialFrame
    __row: "PandasApiTdsRow"

    def __init__(
            self,
            partial_frame: PandasApiPartialFrame,
            row: "PandasApiTdsRow"
    ) -> None:
        self.__partial_frame = partial_frame
        self.__row = row

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["row_number"]), distinct=False, arguments=[], filter_=None, window=None
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"{self.__partial_frame.to_pure_expression(config)}->rowNumber({self.__row.to_pure_expression(config)})"


class PandasApiRankExpression(PyLegendExpressionIntegerReturn):
    if TYPE_CHECKING:
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow

    __partial_frame: PandasApiPartialFrame
    __window_ref: "PandasApiWindowReference"
    __row: "PandasApiTdsRow"

    def __init__(
            self,
            partial_frame: PandasApiPartialFrame,
            window_ref: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
    ) -> None:
        self.__partial_frame = partial_frame
        self.__window_ref = window_ref
        self.__row = row

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["rank"]), distinct=False, arguments=[], filter_=None, window=None
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return (f"{self.__partial_frame.to_pure_expression(config)}->rank("
                f"{self.__window_ref.to_pure_expression(config)}, {self.__row.to_pure_expression(config)})")


class PandasApiDenseRankExpression(PyLegendExpressionIntegerReturn):
    if TYPE_CHECKING:
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow

    __partial_frame: PandasApiPartialFrame
    __window_ref: "PandasApiWindowReference"
    __row: "PandasApiTdsRow"

    def __init__(
            self,
            partial_frame: PandasApiPartialFrame,
            window_ref: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
    ) -> None:
        self.__partial_frame = partial_frame
        self.__window_ref = window_ref
        self.__row = row

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["dense_rank"]), distinct=False, arguments=[], filter_=None, window=None
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return (f"{self.__partial_frame.to_pure_expression(config)}->denseRank("
                f"{self.__window_ref.to_pure_expression(config)}, {self.__row.to_pure_expression(config)})")


class PandasApiPercentRankExpression(PyLegendExpressionFloatReturn):
    if TYPE_CHECKING:
        from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow

    __partial_frame: PandasApiPartialFrame
    __window_ref: "PandasApiWindowReference"
    __row: "PandasApiTdsRow"

    def __init__(
            self,
            partial_frame: PandasApiPartialFrame,
            window_ref: "PandasApiWindowReference",
            row: "PandasApiTdsRow"
    ) -> None:
        self.__partial_frame = partial_frame
        self.__window_ref = window_ref
        self.__row = row

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["percent_rank"]), distinct=False, arguments=[], filter_=None, window=None
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return (f"{self.__partial_frame.to_pure_expression(config)}->percentRank("
                f"{self.__window_ref.to_pure_expression(config)}, {self.__row.to_pure_expression(config)})")


class PandasApiWindowAggregateExpression(PyLegendExpression, metaclass=ABCMeta):
    __partial_frame: "PandasApiPartialFrame"
    __window_ref: "PandasApiWindowReference"
    __column: "PyLegendColumnExpression"
    __func_name: str

    def __init__(
            self,
            partial_frame: "PandasApiPartialFrame",
            window_ref: "PandasApiWindowReference",
            column: "PyLegendColumnExpression",
            func_name: str
    ) -> None:
        self.__partial_frame = partial_frame
        self.__window_ref = window_ref
        self.__column = column
        self.__func_name = func_name

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        col_expr = self.__column.to_sql_expression(frame_name_to_base_query_map, config)
        return FunctionCall(
            name=QualifiedName(parts=[self.__func_name]),
            distinct=False,
            arguments=[col_expr],
            filter_=None,
            window=None  # Window is handled by the wrapper in the main function logic
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        pure_func_map = {
            "stddev_samp": "stdDev",
            "variance": "variance",
            "average": "average",
            "sum": "plus",  # Pure usually uses 'plus' for sum accumulation or 'sum' depending on context. Tests used 'plus'.
            "count": "count",
            "max": "max",
            "min": "min"
        }
        p_func = pure_func_map.get(self.__func_name, self.__func_name)

        # Matches logic in test: : {y | $y->plus()}
        return (f"{self.__partial_frame.to_pure_expression(config)}->{p_func}("
                f"{self.__window_ref.to_pure_expression(config)}, {self.__column.to_pure_expression(config)})")


class PandasApiWindowSumExpression(PandasApiWindowAggregateExpression, PyLegendExpressionNumberReturn):
    def __init__(self, p, w, c): super().__init__(p, w, c, "sum")


class PandasApiWindowMinExpression(PandasApiWindowAggregateExpression, PyLegendExpressionNumberReturn):
    def __init__(self, p, w, c): super().__init__(p, w, c, "min")


class PandasApiWindowMaxExpression(PandasApiWindowAggregateExpression, PyLegendExpressionNumberReturn):
    def __init__(self, p, w, c): super().__init__(p, w, c, "max")


class PandasApiWindowMeanExpression(PandasApiWindowAggregateExpression, PyLegendExpressionFloatReturn):
    def __init__(self, p, w, c): super().__init__(p, w, c, "average")


class PandasApiWindowCountExpression(PandasApiWindowAggregateExpression, PyLegendExpressionIntegerReturn):
    def __init__(self, p, w, c): super().__init__(p, w, c, "count")
