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
    PandasApiPartialFrame, PandasApiDirectSortInfo, PandasApiPrimitive
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.helpers import escape_column_name, generate_pure_lambda
from pylegend.core.sql.metamodel import QuerySpecification, SingleColumn, QualifiedNameReference, QualifiedName, \
    IntegerLiteral, SelectItem, Expression, SortItemNullOrdering
from pylegend.core.sql.metamodel_extension import WindowExpression
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
        from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame

        base_frame_of_expanding = self.__base_frame.base_frame()
        true_base_frame: PandasApiBaseTdsFrame = self.base_frame()

        base_query = true_base_frame.to_sql_query_object(config)
        db_extension = config.sql_to_string_generator().get_db_extension()

        base_query.select.selectItems.append(
            SingleColumn(alias=db_extension.quote_identifier(self.__zero_column_name), expression=IntegerLiteral(0))
        )

        new_query: QuerySpecification = create_sub_query(base_query, config, "root")
        new_select_items: list[SelectItem] = []

        if isinstance(base_frame_of_expanding, PandasApiGroupbyTdsFrame):
            columns_to_retain: PyLegendList[str] = [
                db_extension.quote_identifier(x) for x in base_frame_of_expanding.grouping_column_name_list()
            ]
            new_cols_with_index: PyLegendList[PyLegendTuple[int, "SelectItem"]] = []
            for col in new_query.select.selectItems:
                if not isinstance(col, SingleColumn):
                    raise ValueError(
                        "Group By operation not supported for queries " "with columns other than SingleColumn"
                    )  # pragma: no cover
                if col.alias is None:
                    raise ValueError(
                        "Group By operation not supported for queries " "with SingleColumns with missing alias"
                    )  # pragma: no cover
                if col.alias in columns_to_retain:
                    new_cols_with_index.append((columns_to_retain.index(col.alias), col))

            new_select_items = [y[1] for y in sorted(new_cols_with_index, key=lambda x: x[0])]

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

        if isinstance(base_frame_of_expanding, PandasApiGroupbyTdsFrame):
            tds_row = PandasApiTdsRow.from_tds_frame("r", self.base_frame())
            new_query.groupBy = [
                (lambda x: x[c])(tds_row).to_sql_expression({"r": new_query}, config)
                for c in base_frame_of_expanding.grouping_column_name_list()
            ]

        return new_query

    def to_pure(self, config: FrameToPureConfig) -> str:
        def render_single_column_expression(
                c: PyLegendUnion[
                    PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive],
                    PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]
                ]
        ) -> str:
            escaped_col_name = escape_column_name(c[0])
            expr_str = (c[1].to_pure_expression(config) if isinstance(c[1], PyLegendPrimitive) else
                        convert_literal_to_literal_expression(c[1]).to_pure_expression(config))
            if len(c) == 2:
                return f"{escaped_col_name}:{generate_pure_lambda('p,w,r', expr_str)}"
            else:
                agg_expr_str = c[2].to_pure_expression(config).replace(expr_str, "$c")
                return (f"{escaped_col_name}:"
                        f"{generate_pure_lambda('p,w,r', expr_str)}:"
                        f"{generate_pure_lambda('c', agg_expr_str)}")

        window = self.__window.to_pure_expression(config)

        if all([len(t) == 2 for t in self.__aggregates_list]) or all(
                [len(t) == 3 for t in self.__aggregates_list]):
            if len(self.__aggregates_list) == 1:
                extend_str = f"->extend({window}, ~{render_single_column_expression(self.__aggregates_list[0])})"
            else:
                extend_str = (f"->extend({window}, ~[{config.separator(2)}" +
                              ("," + config.separator(2, True)).join(
                                  [render_single_column_expression(x) for x in self.__aggregates_list]
                              ) +
                              f"{config.separator(1)}])")
            return f"{self.base_frame().to_pure(config)}{config.separator(1)}" + extend_str
        else:
            extend_str = self.base_frame().to_pure(config)
            for c in self.__aggregates_list:
                extend_str += f"{config.separator(1)}->extend({window}, ~{render_single_column_expression(c)})"
            return extend_str

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
            for group_col_name in base_frame_of_expanding.grouping_column_name_list():
                if group_col_name in base_cols_map:
                    new_columns.append(base_cols_map[group_col_name].copy())

        for alias, _, agg_expr in self.__aggregates_list:
            new_columns.append(self.__infer_column_from_expression(alias, agg_expr))

        return new_columns

    def __infer_column_from_expression(self, name: str, expr: PyLegendPrimitive) -> "TdsColumn":
        from pylegend.core.tds.tds_column import PrimitiveTdsColumn
        if isinstance(expr, PyLegendInteger):
            return PrimitiveTdsColumn.integer_column(name)
        elif isinstance(expr, PyLegendFloat):
            return PrimitiveTdsColumn.float_column(name)
        elif isinstance(expr, PyLegendNumber):
            return PrimitiveTdsColumn.number_column(name)
        elif isinstance(expr, PyLegendString):
            return PrimitiveTdsColumn.string_column(name)
        elif isinstance(expr, PyLegendBoolean):
            return PrimitiveTdsColumn.boolean_column(name)  # pragma: no cover
        elif isinstance(expr, PyLegendDate):
            return PrimitiveTdsColumn.date_column(name)
        elif isinstance(expr, PyLegendDateTime):
            return PrimitiveTdsColumn.datetime_column(name)
        elif isinstance(expr, PyLegendStrictDate):
            return PrimitiveTdsColumn.strictdate_column(name)
        else:
            raise TypeError(f"Could not infer TdsColumn type for aggregation result type: {type(expr)}")  # pragma: no cover


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

    def __normalize_input_func_to_standard_dict(
        self, func_input: PyLegendAggInput
    ) -> dict[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]]:
        from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame

        validation_columns: PyLegendList[str]
        default_broadcast_columns: PyLegendList[str]
        group_cols: set[str] = set()

        all_cols = [col.get_name() for col in self.base_frame().columns()]

        if isinstance(self.__base_frame, PandasApiGroupbyTdsFrame):
            group_cols = set(self.__base_frame.grouping_column_name_list())

            selected_cols = self.__base_frame.selected_columns()

            if selected_cols is not None:
                validation_columns = selected_cols
                default_broadcast_columns = selected_cols
            else:
                validation_columns = all_cols
                default_broadcast_columns = [c for c in all_cols if c not in group_cols]
        else:
            validation_columns = all_cols
            default_broadcast_columns = all_cols

        if isinstance(func_input, collections.abc.Mapping):
            normalized: dict[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]] = {}

            for key, value in func_input.items():
                if not isinstance(key, str):
                    raise TypeError(
                        f"Invalid `func` argument for the aggregate function.\n"
                        f"When a dictionary is provided, all keys must be strings.\n"
                        f"But got key: {key!r} (type: {type(key).__name__})\n"
                    )

                if key not in validation_columns:
                    raise ValueError(
                        f"Invalid `func` argument for the aggregate function.\n"
                        f"When a dictionary is provided, all keys must be column names.\n"
                        f"Available columns are: {sorted(validation_columns)}\n"
                        f"But got key: {key!r} (type: {type(key).__name__})\n"
                    )

                if isinstance(value, collections.abc.Sequence) and not isinstance(value, str):
                    for i, f in enumerate(value):
                        if not (callable(f) or isinstance(f, str) or isinstance(f, np.ufunc)):
                            raise TypeError(
                                f"Invalid `func` argument for the aggregate function.\n"
                                f"When a list is provided for a column, all elements must be callable, str, or np.ufunc.\n"
                                f"But got element at index {i}: {f!r} (type: {type(f).__name__})\n"
                            )
                    normalized[key] = value

                else:
                    if not (callable(value) or isinstance(value, str) or isinstance(value, np.ufunc)):
                        raise TypeError(
                            f"Invalid `func` argument for the aggregate function.\n"
                            f"When a dictionary is provided, the value must be a callable, str, or np.ufunc "
                            f"(or a list containing these).\n"
                            f"But got value for key '{key}': {value} (type: {type(value).__name__})\n"
                        )

                    if key in group_cols:
                        normalized[key] = [value]
                    else:
                        normalized[key] = value

            return normalized

        elif isinstance(func_input, collections.abc.Sequence) and not isinstance(func_input, str):
            for i, f in enumerate(func_input):
                if not (callable(f) or isinstance(f, str) or isinstance(f, np.ufunc)):
                    raise TypeError(
                        f"Invalid `func` argument for the aggregate function.\n"
                        f"When a list is provided as the main argument, all elements must be callable, str, or np.ufunc.\n"
                        f"But got element at index {i}: {f!r} (type: {type(f).__name__})\n"
                    )

            return {col: func_input for col in default_broadcast_columns}

        elif callable(func_input) or isinstance(func_input, str) or isinstance(func_input, np.ufunc):
            return {col: func_input for col in default_broadcast_columns}

        else:
            raise TypeError(
                "Invalid `func` argument for aggregate function. "
                "Expected a callable, str, np.ufunc, a list containing exactly one of these, "
                "or a mapping[str -> callable/str/ufunc/a list containing exactly one of these]. "
                f"But got: {func_input!r} (type: {type(func_input).__name__})"
            )

    def __normalize_agg_func_to_lambda_function(
        self, func: PyLegendAggFunc
    ) -> PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]:

        PYTHON_FUNCTION_TO_LEGEND_FUNCTION_MAPPING: PyLegendMapping[str, PyLegendList[str]] = {
            "average": ["mean", "average", "nanmean"],
            "sum": ["sum", "nansum"],
            "min": ["min", "amin", "minimum", "nanmin"],
            "max": ["max", "amax", "maximum", "nanmax"],
            "std_dev_sample": ["std", "std_dev", "nanstd"],
            "variance_sample": ["var", "variance", "nanvar"],
            "count": ["count", "size", "len", "length"],
        }

        FLATTENED_FUNCTION_MAPPING: dict[str, str] = {}
        for target_method, source_list in PYTHON_FUNCTION_TO_LEGEND_FUNCTION_MAPPING.items():
            for alias in source_list:
                FLATTENED_FUNCTION_MAPPING[alias] = target_method

        lambda_source: str
        final_lambda: PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive]

        if isinstance(func, str):
            func_lower = func.lower()
            if func_lower in FLATTENED_FUNCTION_MAPPING:
                internal_method_name = FLATTENED_FUNCTION_MAPPING[func_lower]
            else:
                raise NotImplementedError(
                    f"Invalid `func` argument for the aggregate function.\n"
                    f"The string {func!r} does not correspond to any supported aggregation.\n"
                    f"Available string functions are: {sorted(FLATTENED_FUNCTION_MAPPING.keys())}"
                )  # pragma: no cover
            lambda_source = self._generate_lambda_source(internal_method_name)
            final_lambda = eval(lambda_source)
            return final_lambda

        elif isinstance(func, np.ufunc):
            func_name = func.__name__
            if func_name in FLATTENED_FUNCTION_MAPPING:
                internal_method_name = FLATTENED_FUNCTION_MAPPING[func_name]
            else:
                raise NotImplementedError(
                    f"Invalid `func` argument for the aggregate function.\n"
                    f"The NumPy function {func_name!r} is not supported.\n"
                    f"Supported aggregate functions are: {sorted(FLATTENED_FUNCTION_MAPPING.keys())}"
                )  # pragma: no cover
            lambda_source = self._generate_lambda_source(internal_method_name)
            final_lambda = eval(lambda_source)
            return final_lambda

        else:
            func_name = getattr(func, "__name__", "").lower()
            if func_name in FLATTENED_FUNCTION_MAPPING and func_name != "<lambda>":
                internal_method_name = FLATTENED_FUNCTION_MAPPING[func_name]
                lambda_source = self._generate_lambda_source(internal_method_name)
                final_lambda = eval(lambda_source)
                return final_lambda
            else:

                def validation_wrapper(x: PyLegendPrimitiveCollection) -> PyLegendPrimitive:
                    result = func(x)
                    if not isinstance(result, PyLegendPrimitive):
                        raise TypeError(
                            f"Custom aggregation function must return a PyLegendPrimitive (Expression).\n"
                            f"But got type: {type(result).__name__}\n"
                            f"Value: {result!r}"
                        )  # pragma: no cover
                    return result

                return validation_wrapper

    def _generate_lambda_source(self, internal_method_name: str) -> str:
        return f"lambda x: x.{internal_method_name}()"

    def _generate_column_alias(self, col_name: str, func: PyLegendAggFunc, lambda_counter: int) -> str:
        if isinstance(func, str):
            return f"{func}({col_name})"

        func_name = getattr(func, "__name__", "<lambda>")

        if func_name != "<lambda>":
            return f"{func_name}({col_name})"
        else:
            return f"lambda_{lambda_counter}({col_name})"

    def __construct_expressions(self):
        from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame

        # Define base frames
        base_frame_of_expanding = self.__base_frame.base_frame()
        true_base_frame: PandasApiBaseTdsFrame = self.base_frame()

        # Construct window
        partition_by: PyLegendOptional[PyLegendList[str]] = None
        if isinstance(base_frame_of_expanding, PandasApiGroupbyTdsFrame):
            partition_by = [col.get_name() for col in base_frame_of_expanding.get_grouping_columns()]

        order_by = [PandasApiDirectSortInfo(self.__zero_column_name, PandasApiSortDirection.ASC)]

        start_bound = PandasApiFrameBound(PandasApiFrameBoundType.UNBOUNDED_PRECEDING)
        end_bound = PandasApiFrameBound(PandasApiFrameBoundType.CURRENT_ROW)
        window_frame = PandasApiWindowFrame(PandasApiWindowFrameMode.ROWS, start_bound, end_bound)

        window = PandasApiWindow(partition_by, order_by, window_frame)
        self.__window = window

        tds_row = PandasApiTdsRow.from_tds_frame("r", true_base_frame)
        normalized_input_func: dict[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]] = (
            self.__normalize_input_func_to_standard_dict(self.__func)
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

                    normalized_agg_func = self.__normalize_agg_func_to_lambda_function(func)
                    agg_result = normalized_agg_func(collection)

                    alias = self._generate_column_alias(column_name, func, lambda_counter)
                    self.__aggregates_list.append((alias, column_primitive, agg_result))

            else:
                normalized_agg_func = self.__normalize_agg_func_to_lambda_function(agg_input)
                agg_result = normalized_agg_func(collection)

                self.__aggregates_list.append((column_name, column_primitive, agg_result))
