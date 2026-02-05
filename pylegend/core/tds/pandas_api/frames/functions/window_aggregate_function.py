import collections

import numpy as np

from pylegend._typing import (
    PyLegendList,
    PyLegendTuple,
    PyLegendUnion,
    PyLegendMapping,
    PyLegendSequence,
    PyLegendCallable,
    PyLegendOptional,
    TYPE_CHECKING,
)
from pylegend.core.language import PyLegendPrimitive, PyLegendColumnExpression, PyLegendPrimitiveOrPythonPrimitive, \
    PyLegendPrimitiveCollection, create_primitive_collection, PyLegendInteger, PyLegendFloat, PyLegendNumber, PyLegendString, \
    PyLegendBoolean, PyLegendDate, PyLegendDateTime, PyLegendStrictDate, convert_literal_to_literal_expression
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import PyLegendAggInput, PyLegendAggFunc, \
    PyLegendAggList
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import PandasApiWindow, PandasApiFrameBound, \
    PandasApiFrameBoundType, PandasApiWindowFrameMode, PandasApiWindowFrame, PandasApiSortDirection, PandasApiWindowReference, \
    PandasApiPartialFrame, PandasApiDirectSortInfo, PandasApiPrimitive, PandasApiSortInfo
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.helpers import escape_column_name, generate_pure_lambda
from pylegend.core.sql.metamodel import QuerySpecification, SingleColumn, QualifiedNameReference, QualifiedName, \
    IntegerLiteral, SelectItem, Expression, SortItemNullOrdering
from pylegend.core.sql.metamodel_extension import WindowExpression
from pylegend.core.tds.pandas_api.frames.helpers.helper_aggregate import \
    normalize_input_func_to_standard_dict, normalize_agg_func_to_lambda_function, generate_column_alias
from pylegend.core.tds.pandas_api.frames.helpers.helper_shared import infer_column_from_expression
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_expanding_tds_frame import PandasApiExpandingTdsFrame
from pylegend.core.tds.sql_query_helpers import create_sub_query
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig

if TYPE_CHECKING:
    from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
    from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame


class PandasApiWindowAggregateFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiExpandingTdsFrame
    __func: PyLegendAggInput
    __axis: PyLegendUnion[int, str]
    __args: PyLegendSequence[PyLegendPrimitiveOrPythonPrimitive]
    __kwargs: PyLegendMapping[str, PyLegendPrimitiveOrPythonPrimitive]

    __internal_col_name: str
    __calculated_expressions: PyLegendList[
        PyLegendTuple[str, PyLegendPrimitive, PandasApiWindow]
    ]

    __zero_column_name: str
    __window: PandasApiWindow
    __aggregates_list: PyLegendList[PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]]

    @classmethod
    def name(cls) -> str:
        return "window_aggregate"

    def __init__(
            self,
            base_frame: PandasApiExpandingTdsFrame,
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
        self.__aggregates_list = []
        self.__construct_expressions()

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        base_query = self.base_frame().to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()

        base_query.select.selectItems.append(
            SingleColumn(alias=db_extension.quote_identifier(self.__zero_column_name), expression=IntegerLiteral(0))
        )

        new_query: QuerySpecification = create_sub_query(base_query, config, "root")
        new_select_items: list[SelectItem] = []

        for agg in self.__aggregates_list:
            agg_sql_expr = agg[2].to_sql_expression({"r": new_query}, config)
            window_expr = WindowExpression(
                nested=agg_sql_expr,
                window=self.__window.to_sql_node(new_query, config),
            )
            new_select_items.append(
                SingleColumn(alias=db_extension.quote_identifier(agg[0]), expression=window_expr)
            )

        new_query.select.selectItems = new_select_items
        return new_query

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
        window_expression = self.__window.to_pure_expression(config)

        extend_col_expressions: PyLegendList[str] = [
            self._render_single_column_expression(agg, temp_column_name_suffix, config)
            for agg in self.__aggregates_list
        ]
        extend_str = (
            f"->extend({window_expression}, ~[{config.separator(2)}" +
            ("," + config.separator(2, True)).join(extend_col_expressions) +
            f"{config.separator(1)}])"
        )

        project_col_expressions = [
            f"{escape_column_name(agg[0])}:p|$p.{escape_column_name(agg[0] + temp_column_name_suffix)}"
            for agg in self.__aggregates_list
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

    def base_frame(self) -> PandasApiBaseTdsFrame:
        from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
        base_frame_of_expanding = self.__base_frame.base_frame()
        if isinstance(base_frame_of_expanding, PandasApiGroupbyTdsFrame):
            return base_frame_of_expanding.base_frame()
        return base_frame_of_expanding

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendList["TdsColumn"]:
        from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame
        base_frame_of_expanding = self.__base_frame.base_frame()

        new_columns = []

        if isinstance(base_frame_of_expanding, PandasApiGroupbyTdsFrame):
            base_cols_map = {c.get_name(): c for c in self.base_frame().columns()}
            for group_col in base_frame_of_expanding.get_grouping_columns():
                group_col_name = group_col.get_name()
                if group_col_name in base_cols_map:
                    new_columns.append(base_cols_map[group_col_name].copy())

        for alias, _, agg_expr in self.__aggregates_list:
            new_columns.append(infer_column_from_expression(alias, agg_expr))

        return new_columns


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

    def __construct_expressions(self):
        from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame

        base_frame_of_expanding = self.__base_frame.base_frame()
        true_base_frame: PandasApiBaseTdsFrame = self.base_frame()

        # Construct window
        partition_by: PyLegendOptional[PyLegendList[str]] = None
        if isinstance(base_frame_of_expanding, PandasApiGroupbyTdsFrame):
            partition_by = [col.get_name() for col in base_frame_of_expanding.get_grouping_columns()]

        order_by = [PandasApiSortInfo(self.__zero_column_name, PandasApiSortDirection.ASC)]

        start_bound = PandasApiFrameBound(PandasApiFrameBoundType.UNBOUNDED_PRECEDING)
        end_bound = PandasApiFrameBound(PandasApiFrameBoundType.CURRENT_ROW)
        window_frame = PandasApiWindowFrame(PandasApiWindowFrameMode.ROWS, start_bound, end_bound)

        window = PandasApiWindow(partition_by, order_by, window_frame)
        self.__window = window

        # Construct aggregates list
        tds_row = PandasApiTdsRow.from_tds_frame("r", true_base_frame)
        normalized_input_func: dict[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]] = (
            normalize_input_func_to_standard_dict(self.__func, base_frame_of_expanding)
        )

        for column_name, agg_input in normalized_input_func.items():
            column_primitive: PandasApiPrimitive = tds_row[column_name]
            collection: PyLegendPrimitiveCollection = create_primitive_collection(column_primitive)

            if isinstance(agg_input, list):
                lambda_counter = 0
                for func in agg_input:
                    is_anonymous_lambda = False
                    if not isinstance(func, str):
                        if getattr(func, "__name__", "<lambda>") == "<lambda>":
                            is_anonymous_lambda = True

                    if is_anonymous_lambda:
                        lambda_counter += 1

                    normalized_agg_func = normalize_agg_func_to_lambda_function(func)
                    agg_result = normalized_agg_func(collection)

                    alias = generate_column_alias(column_name, func, lambda_counter)
                    self.__aggregates_list.append((alias, column_primitive, agg_result))

            else:
                normalized_agg_func = normalize_agg_func_to_lambda_function(agg_input)
                agg_result = normalized_agg_func(collection)

                self.__aggregates_list.append((column_name, column_primitive, agg_result))
