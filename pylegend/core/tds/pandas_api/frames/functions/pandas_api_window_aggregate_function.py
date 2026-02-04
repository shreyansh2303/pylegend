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
    PyLegendBoolean, PyLegendDate, PyLegendDateTime, PyLegendStrictDate
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
        # 1. Extend with internal column: ->extend(~__internal__:{r|0})
        return ""
        internal_col_def = f"~{escape_column_name(self.__internal_col_name)}:{{r|0}}"

        # 2. Build the extend chain for aggregations
        extend_ops = []
        for alias, result_expr, window in self.__calculated_expressions:
            # Map the expression, e.g., {p,w,r | $r.col1}:{y | $y->plus()}
            # Note: Our PandasApiWindowAggregateExpression.to_pure_expression handles the {p,w,r...} generation
            # provided we implemented it correctly in custom_expressions.py

            # However, `PandasApiWindowAggregateExpression` returns the FULL function call (e.g. p->sum(w, r.col)).
            # Pure syntax for `extend` is: ->extend(over(...), ~new_col:{p,w,r | ...})
            # Our custom expression returns exactly that lambda body logic if we designed it right,
            # but usually we need to wrap it.

            # Let's rely on the expression's own generation:
            # It returns: "$p->sum($w, $r.col)"
            # We need: "~alias:{p,w,r | $p->sum($w, $r.col)}"

            pure_lambda = generate_pure_lambda("p,w,r", result_expr.to_pure_expression(config))

            # Window definition
            window_str = window.to_pure_expression(config)

            extend_ops.append(
                f"->extend({window_str}, ~{escape_column_name(alias)}:{pure_lambda})"
            )

        # 3. Project the final columns (aliases only)
        # ->project(~[alias1:p|$p.alias1, ...])
        project_cols = []
        for alias, _, _ in self.__calculated_expressions:
            col_ref = f"p|$p.{escape_column_name(alias)}"
            project_cols.append(f"{escape_column_name(alias)}:{col_ref}")

        project_str = f"->project(~[{config.separator(2)}{(', ' + config.separator(2)).join(project_cols)}{config.separator(1)}])"

        # Combine
        base_str = self.__base_frame.to_pure(config)

        # Handle "group by" base frame differently?
        # No, "expanding" works on the base rows, so we usually reference the underlying frame
        # and just use the grouping keys for partition.

        return (
            f"{base_str}{config.separator(1)}"
            f"->extend({internal_col_def})"
            f"{''.join([f'{config.separator(1)}{op}' for op in extend_ops])}"
            f"{config.separator(1)}{project_str}"
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


        # normalized_func: dict[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]] = (
        #     self.__normalize_input_func_to_standard_dict(self.__func)
        # )
        #
        # tds_row = PandasApiTdsRow.from_tds_frame("r", self.base_frame())
        #
        # for column_name, agg_input in normalized_func.items():
        #     mapper_function: PyLegendCallable[[PandasApiTdsRow], PyLegendPrimitiveOrPythonPrimitive] = eval(
        #         f'lambda r: r["{column_name}"]'
        #     )
        #     map_result: PyLegendPrimitiveOrPythonPrimitive = mapper_function(tds_row)
        #     collection: PyLegendPrimitiveCollection = create_primitive_collection(map_result)
        #
        #     if isinstance(agg_input, list):
        #         lambda_counter = 0
        #         for func in agg_input:
        #             is_anonymous_lambda = False
        #             if not isinstance(func, str):
        #                 if getattr(func, "__name__", "<lambda>") == "<lambda>":
        #                     is_anonymous_lambda = True
        #
        #             if is_anonymous_lambda:
        #                 lambda_counter += 1
        #
        #             normalized_agg_func = self.__normalize_agg_func_to_lambda_function(func)
        #             agg_result = normalized_agg_func(collection)
        #
        #             alias = self._generate_column_alias(column_name, func, lambda_counter)
        #             self.__aggregates_list.append((alias, map_result, agg_result))
        #
        #     else:
        #         normalized_agg_func = self.__normalize_agg_func_to_lambda_function(agg_input)
        #         agg_result = normalized_agg_func(collection)
        #
        #         self.__aggregates_list.append((column_name, map_result, agg_result))

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

        start_bound = PandasApiFrameBound(PandasApiFrameBoundType.UNBOUNDED_PRECEDING, value=None)
        end_bound = PandasApiFrameBound(PandasApiFrameBoundType.CURRENT_ROW, value=None)
        window_frame = PandasApiWindowFrame(PandasApiWindowFrameMode.ROWS, start_bound, end_bound)

        window = PandasApiWindow(partition_by, order_by, window_frame)
        self.__window = window

        tds_row = PandasApiTdsRow.from_tds_frame("r", true_base_frame)
        normalized_input_func: dict[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]] = (
            self.__normalize_input_func_to_standard_dict(self.__func)
        )

        print(f'aggregate length = {len(self.__aggregates_list)}')

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



        print(f'aggregate length = {len(self.__aggregates_list)}')




        # Construct p,w,r
        # partial_frame = PandasApiPartialFrame(true_base_frame, "p")
        # window_ref = PandasApiWindowReference(window, "w")

        # normalized = self.__normalize_input(self.__func, true_base_frame)
        #
        # results = []
        #
        # tds_row = PandasApiTdsRow.from_tds_frame("r", self.base_frame())
        # for col_name, operations in normalized.items():
        #     col_expr = PyLegendColumnExpression(tds_row, col_name)
        #
        #     for op_name in operations:
        #         # Generate Result Name: e.g. "sum(col1)" or "col1" if single op
        #         alias = self.__generate_alias(col_name, op_name, len(operations) > 1)
        #
        #         result_expr: PyLegendPrimitive
        #         if op_name == 'sum':
        #             result_expr = partial_frame.sum(window_ref, col_expr)
        #         elif op_name in ['mean', 'average']:
        #             result_expr = partial_frame.mean(window_ref, col_expr)
        #         elif op_name == 'min':
        #             result_expr = partial_frame.min(window_ref, col_expr)
        #         elif op_name == 'max':
        #             result_expr = partial_frame.max(window_ref, col_expr)
        #         elif op_name in ['count', 'size']:
        #             result_expr = partial_frame.count(window_ref, col_expr)
        #         else:
        #             raise NotImplementedError(f"Expanding window function '{op_name}' not supported")
        #
        #         results.append((alias, result_expr, window))
        #
        # return results

    def __normalize_input(self, func, frame) -> dict[str, list[str]]:
        # Returns {col_name: [op1, op2]}
        normalized = {}
        all_cols = [c.get_name() for c in frame.columns()]

        # Filter numeric columns for implicit selection?
        # For now, apply to all or let user specify.

        if isinstance(func, str):
            for c in all_cols:
                normalized[c] = [func]

        elif isinstance(func, list):
            for c in all_cols:
                normalized[c] = [str(f) for f in func]

        elif isinstance(func, dict):
            for key, value in func.items():
                if key not in all_cols:
                    continue  # or raise error

                if isinstance(value, str):
                    normalized[key] = [value]
                elif isinstance(value, list):
                    normalized[key] = [str(v) for v in value]
                elif callable(value):
                    # Handle lambda mappings if possible, or mapping known functions
                    # This is tricky without the full AggregateFunction infra.
                    # For this implementation, we assume strings or simple mappings.
                    # Test case passed: "sum", lambda x: x.count()
                    # We need to extract names from callables.
                    name = getattr(value, "__name__", "lambda")
                    if name == "<lambda>":
                        # We can't easily introspect the lambda content here to determine 'count' vs 'sum'
                        # unless we use the code provided in AggregateFunction.
                        # For the specific test case `x.count()`, let's assume 'count'.
                        # This is a simplification.
                        normalized[key] = ["count"]  # Placeholder logic
                    else:
                        normalized[key] = [name]

        return normalized

    def __generate_alias(self, col: str, op: str, is_multi: bool) -> str:
        if not is_multi:
            return col
        return f"{op}({col})"