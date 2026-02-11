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
    PyLegendAny,
    PyLegendDict,
    PyLegendList,
    PyLegendTuple,
    PyLegendUnion,
    PyLegendMapping,
    PyLegendSequence,
    TYPE_CHECKING,
)
from pylegend.core.language import (
    PyLegendPrimitive,
    PyLegendPrimitiveOrPythonPrimitive,
    convert_literal_to_literal_expression
)
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import PyLegendAggInput
from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
from pylegend.core.language.pandas_api.pandas_api_series import Series
from pylegend.core.language.shared.helpers import (
    escape_column_name,
    generate_pure_lambda,
)
from pylegend.core.language.shared.pure_expression import PureExpression
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
    IntegerLiteral,
    SelectItem,
    Expression,
    SortItemNullOrdering,
    SortItem,
    SortItemOrdering,
)
from pylegend.core.sql.metamodel_extension import WindowExpression
from pylegend.core.tds.pandas_api.frames.helpers.helper_aggregate import construct_aggregate_list
from pylegend.core.tds.pandas_api.frames.helpers.helper_shared import infer_column_from_expression
from pylegend.core.tds.pandas_api.frames.helpers.helper_window_function import get_true_base_frame
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_frame import (
    FrameToSqlConfig,
    FrameToPureConfig,
)

if TYPE_CHECKING:
    from pylegend.core.tds.tds_column import TdsColumn


class PandasApiWindowAggregateFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiWindowTdsFrame[PyLegendAny]  # type: ignore[explicit-any]
    __func: PyLegendAggInput
    __axis: PyLegendUnion[int, str]
    __args: PyLegendSequence[PyLegendPrimitiveOrPythonPrimitive]
    __kwargs: PyLegendMapping[str, PyLegendPrimitiveOrPythonPrimitive]
    __zero_column_name: str

    @classmethod
    def name(cls) -> str:
        return "window_aggregate"

    def __init__(  # type: ignore[explicit-any]
            self,
            base_frame: PandasApiWindowTdsFrame[PyLegendAny],
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str],
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive,
    ) -> None:
        self.__base_frame = base_frame
        self.__func = func
        self.__axis = axis
        self.__args = args
        self.__kwargs = kwargs
        self.__zero_column_name = "__internal_pylegend_column__"

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.base_frame().to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()

        base_query.select.selectItems.append(
            SingleColumn(alias=db_extension.quote_identifier(self.__zero_column_name), expression=IntegerLiteral(0))
        )

        new_query: QuerySpecification = create_sub_query(base_query, config, "root")
        new_select_items: list[SelectItem] = []

        window = self.__base_frame.construct_window(sorting_column_names=[self.__zero_column_name])
        aggregates_list = construct_aggregate_list(self.__base_frame.base_frame(), self.__func, frame_name="r")
        for agg in aggregates_list:
            agg_sql_expr = agg[2].to_sql_expression({"r": new_query}, config)
            window_expr = WindowExpression(
                nested=agg_sql_expr,
                window=window.to_sql_node(new_query, config),
            )
            new_select_items.append(
                SingleColumn(alias=db_extension.quote_identifier(agg[0]), expression=window_expr)
            )

        new_query.select.selectItems = new_select_items
        return new_query

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        window = self.__base_frame.construct_window(sorting_column_names=None)
        aggregates_list = construct_aggregate_list(self.__base_frame.base_frame(), self.__func, frame_name="c")

        assert isinstance(self.__base_frame.base_frame(), (Series, GroupbySeries)) and len(aggregates_list) == 1, (
            "To get an SQL expression for window aggregation, exactly one column must be selected."
        )

        agg = aggregates_list[0]
        agg_sql_expr = agg[2].to_sql_expression(frame_name_to_base_query_map, config)
        window_node = window.to_sql_node(frame_name_to_base_query_map["c"], config)
        window_node.orderBy = [SortItem(IntegerLiteral(0), SortItemOrdering.ASCENDING, SortItemNullOrdering.UNDEFINED)]

        return WindowExpression(
            nested=agg_sql_expr,
            window=window_node
        )

    @staticmethod
    def _render_single_column_expression(
            agg: PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive],
            temp_column_name_suffix: str,
            config: FrameToPureConfig
    ) -> str:
        escaped_col_name = escape_column_name(agg[0] + temp_column_name_suffix)
        expr_str = (
            agg[1].to_pure_expression(config) if isinstance(agg[1], PyLegendPrimitive) else
            convert_literal_to_literal_expression(agg[1]).to_pure_expression(config)
        )
        agg_expr_str = agg[2].to_pure_expression(config).replace(expr_str, "$c")
        return (
            f"{escaped_col_name}:"
            f"{generate_pure_lambda('p,w,r', expr_str)}:"
            f"{generate_pure_lambda('c', agg_expr_str)}"
        )

    def to_pure(self, config: FrameToPureConfig) -> str:
        temp_column_name_suffix = "__internal_pylegend_column__"
        window_expression = (
            self.__base_frame
            .construct_window(sorting_column_names=[self.__zero_column_name])
            .to_pure_expression(config)
        )
        aggregates_list = construct_aggregate_list(self.__base_frame.base_frame(), self.__func, frame_name="r")

        extend_col_expressions: PyLegendList[str] = [
            self._render_single_column_expression(agg, temp_column_name_suffix, config)
            for agg in aggregates_list
        ]
        extend_str = (
            f"->extend({window_expression}, ~[{config.separator(2)}" +
            ("," + config.separator(2, True)).join(extend_col_expressions) +
            f"{config.separator(1)}])"
        )

        project_col_expressions = [
            f"{escape_column_name(agg[0])}:p|$p.{escape_column_name(agg[0] + temp_column_name_suffix)}"
            for agg in aggregates_list
        ]
        project_str = (
            f"->project(~[{config.separator(2)}" +
            ("," + config.separator(2, True)).join(project_col_expressions) +
            f"{config.separator(1)}])"
        )

        return (
            self.base_frame().to_pure(config) +
            config.separator(1) + "->extend(~" + escape_column_name(self.__zero_column_name) + ":{r|0})" +
            config.separator(1) + extend_str +
            config.separator(1) + project_str
        )

    def to_pure_expression(self, config: FrameToPureConfig) -> PyLegendUnion[str, PureExpression]:
        temp_column_name_suffix: str = "__internal_pylegend_column__"

        window_expression = (
            self.__base_frame
            .construct_window(sorting_column_names=None)
            .to_pure_expression(config)
        )
        aggregates_list = construct_aggregate_list(self.__base_frame.base_frame(), self.__func, frame_name="r")

        assert isinstance(self.__base_frame.base_frame(), (Series, GroupbySeries)) and len(aggregates_list) == 1, (
            "To get a pure expression for window aggregation, exactly one column must be selected."
        )

        agg = aggregates_list[0]
        extend_str = (
            f"->extend({window_expression}, ~{self._render_single_column_expression(agg, temp_column_name_suffix, config)})"
        )

        return PureExpression.from_prerequisite_expr(
            extend_str, column_name=escape_column_name(agg[0] + temp_column_name_suffix)
        )

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return get_true_base_frame(self.__base_frame.base_frame())

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        aggregates_list = construct_aggregate_list(self.__base_frame.base_frame(), self.__func, frame_name="r")
        return [
            infer_column_from_expression(alias, agg_expr)
            for alias, _, agg_expr in aggregates_list
        ]

    def validate(self) -> bool:
        if self.__axis not in [0, "index"]:
            raise NotImplementedError(
                f"The 'axis' parameter of the aggregate function must be 0 or 'index', but got: {self.__axis}"
            )

        if len(self.__args) > 0 or len(self.__kwargs) > 0:
            raise NotImplementedError(
                "AggregateFunction currently does not support additional positional "
                "or keyword arguments. Please remove extra *args/**kwargs."
            )

        return True
