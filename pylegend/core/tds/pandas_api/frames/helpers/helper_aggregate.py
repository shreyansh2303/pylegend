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

import collections
import numpy as np
from pylegend._typing import (
    PyLegendCallable,
    PyLegendList,
    PyLegendMapping,
    PyLegendSequence,
    PyLegendTuple,
    PyLegendUnion,
)
from pylegend.core.language import (
    create_primitive_collection,
    PyLegendPrimitive,
    PyLegendPrimitiveCollection,
    PyLegendPrimitiveOrPythonPrimitive,
)
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import (
    PyLegendAggFunc,
    PyLegendAggInput,
    PyLegendAggList,
)
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import PandasApiPrimitive
from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
from pylegend.core.language.pandas_api.pandas_api_series import Series
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.tds.pandas_api.frames.helpers.helper_window_function import get_true_base_frame
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame


__all__: PyLegendSequence[str] = [
    "normalize_input_func_to_standard_dict",
    "normalize_agg_func_to_lambda_function",
    "generate_column_alias",
    "construct_aggregate_list",
]


def normalize_input_func_to_standard_dict(
        func_input: PyLegendAggInput,
        base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame, Series, GroupbySeries]
) -> dict[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]]:
    validation_columns: PyLegendList[str]
    default_broadcast_columns: PyLegendList[str]
    group_cols: set[str] = set()

    all_cols = [col.get_name() for col in get_true_base_frame(base_frame).columns()]

    if isinstance(base_frame, PandasApiBaseTdsFrame):
        validation_columns = all_cols
        default_broadcast_columns = all_cols

    elif isinstance(base_frame, PandasApiGroupbyTdsFrame):
        group_cols = set([col.get_name() for col in base_frame.get_grouping_columns()])
        selected_cols = base_frame.get_selected_columns()

        if selected_cols is not None:
            validation_columns = [col.get_name() for col in selected_cols]
            default_broadcast_columns = [col.get_name() for col in selected_cols]
        else:
            validation_columns = all_cols
            default_broadcast_columns = [c for c in all_cols if c not in group_cols]

    elif isinstance(base_frame, Series):
        validation_columns = [col.get_name() for col in base_frame.columns()]
        default_broadcast_columns = [col.get_name() for col in base_frame.columns()]

    elif isinstance(base_frame, GroupbySeries):
        selected_cols = base_frame.get_base_frame().get_selected_columns()
        assert selected_cols is not None
        validation_columns = [col.get_name() for col in selected_cols]
        default_broadcast_columns = [col.get_name() for col in selected_cols]

    else:
        raise TypeError(
            "Unsupported base_frame type encountered when normalizing input 'func' for aggregation. "
            "Supported base_frame types: PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame, Series, GroupbySeries. "
            f"But got type: {type(base_frame).__name__}"
        )

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


def normalize_agg_func_to_lambda_function(
    func: PyLegendAggFunc
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
        lambda_source = _generate_lambda_source(internal_method_name)
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
        lambda_source = _generate_lambda_source(internal_method_name)
        final_lambda = eval(lambda_source)
        return final_lambda

    else:
        func_name = getattr(func, "__name__", "").lower()
        if func_name in FLATTENED_FUNCTION_MAPPING and func_name != "<lambda>":
            internal_method_name = FLATTENED_FUNCTION_MAPPING[func_name]
            lambda_source = _generate_lambda_source(internal_method_name)
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


def _generate_lambda_source(internal_method_name: str) -> str:
    return f"lambda x: x.{internal_method_name}()"


def generate_column_alias(col_name: str, func: PyLegendAggFunc, lambda_counter: int) -> str:
    if isinstance(func, str):
        return f"{func}({col_name})"

    func_name = getattr(func, "__name__", "<lambda>")

    if func_name != "<lambda>":
        return f"{func_name}({col_name})"
    else:
        return f"lambda_{lambda_counter}({col_name})"


def construct_aggregate_list(
        base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame, Series, GroupbySeries],
        input_func: PyLegendAggInput,
        frame_name: str
) -> PyLegendSequence[PyLegendTuple[str, PyLegendPrimitiveOrPythonPrimitive, PyLegendPrimitive]]:
    direct_base_frame = base_frame
    true_base_frame = get_true_base_frame(direct_base_frame)

    tds_row = PandasApiTdsRow.from_tds_frame(frame_name, true_base_frame)
    normalized_input_func: dict[str, PyLegendUnion[PyLegendAggFunc, PyLegendAggList]] = (
        normalize_input_func_to_standard_dict(input_func, direct_base_frame)
    )

    aggregates_list = []
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
                aggregates_list.append((alias, column_primitive, agg_result))

        else:
            normalized_agg_func = normalize_agg_func_to_lambda_function(agg_input)
            agg_result = normalized_agg_func(collection)

            aggregates_list.append((column_name, column_primitive, agg_result))

    return aggregates_list
