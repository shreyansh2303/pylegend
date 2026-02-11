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

from abc import (
    ABC,
    abstractmethod
)
import copy
from typing import overload
from typing_extensions import (
    Generic
)
from pylegend._typing import (
    PyLegendList,
    PyLegendOptional,
    PyLegendSequence,
    PyLegendTypeVar,
    PyLegendUnion,
)
from pylegend.core.language import PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import PyLegendAggInput
from pylegend.core.language.pandas_api.pandas_api_custom_expressions import (
    PandasApiWindow,
    PandasApiWindowFrame,
    PandasApiWindowFrameMode,
    PandasApiFrameBoundType,
    PandasApiFrameBound,
    PandasApiSortInfo,
    PandasApiSortDirection
)
from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
from pylegend.core.language.pandas_api.pandas_api_series import Series
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame


__all__: PyLegendSequence[str] = [
    "PandasApiWindowTdsFrame",
    "PandasApiExpandingTdsFrame",
]


T = PyLegendTypeVar("T")


class PandasApiWindowTdsFrame(ABC, Generic[T]):
    @abstractmethod
    def base_frame(self) -> T:
        pass  # pragma: no cover

    @abstractmethod
    def construct_window(self, sorting_column_names: PyLegendOptional[PyLegendList[str]]) -> PandasApiWindow:
        pass  # pragma: no cover

    @overload
    def aggregate(
            self: "PandasApiWindowTdsFrame[Series]",
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> Series:
        ...

    @overload
    def aggregate(
            self: "PandasApiWindowTdsFrame[GroupbySeries]",
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> GroupbySeries:
        ...

    @overload
    def aggregate(
            self: "PandasApiWindowTdsFrame[PandasApiBaseTdsFrame]",
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> PandasApiBaseTdsFrame:
        ...

    @overload
    def aggregate(
            self: "PandasApiWindowTdsFrame[PandasApiGroupbyTdsFrame]",
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> PandasApiBaseTdsFrame:
        ...

    def aggregate(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> PyLegendUnion[Series, GroupbySeries, PandasApiBaseTdsFrame]:
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.window_aggregate_function import (
            PandasApiWindowAggregateFunction
        )

        direct_base_frame = self.base_frame()

        if isinstance(direct_base_frame, Series):
            new_series = copy.copy(direct_base_frame)
            new_series._filtered_frame = PandasApiAppliedFunctionTdsFrame(
                PandasApiWindowAggregateFunction(
                    self, func, axis, *args, **kwargs
                )
            )
            return new_series

        elif isinstance(direct_base_frame, GroupbySeries):
            new_gb_series = copy.copy(direct_base_frame)
            new_gb_series.applied_function_frame = PandasApiAppliedFunctionTdsFrame(
                PandasApiWindowAggregateFunction(
                    self, func, axis, *args, **kwargs
                )
            )
            return new_gb_series

        else:
            return PandasApiAppliedFunctionTdsFrame(
                PandasApiWindowAggregateFunction(
                    self, func, axis, *args, **kwargs
                )
            )

    agg = aggregate


class PandasApiExpandingTdsFrame(PandasApiWindowTdsFrame[T], Generic[T]):
    _base_frame: T
    _min_periods: int
    _axis: PyLegendUnion[int, str]
    _method: PyLegendOptional[str]

    def __init__(
            self,
            base_frame: T,
            min_periods: int = 1,
            axis: PyLegendUnion[int, str] = 0,
            method: PyLegendOptional[str] = None
    ) -> None:
        self._base_frame = base_frame
        self._min_periods = min_periods
        self._axis = axis
        self._method = method
        self._validate()

    def base_frame(self) -> T:
        return self._base_frame

    def construct_window(self, sorting_column_names: PyLegendOptional[PyLegendList[str]]) -> PandasApiWindow:
        partition_by: PyLegendOptional[PyLegendList[str]] = None
        if isinstance(self._base_frame, PandasApiGroupbyTdsFrame):
            partition_by = [col.get_name() for col in self._base_frame.get_grouping_columns()]
        elif isinstance(self._base_frame, GroupbySeries):
            partition_by = [col.get_name() for col in self._base_frame.get_base_frame().get_grouping_columns()]

        order_by = (
            None if sorting_column_names is None or len(sorting_column_names) == 0 else
            [PandasApiSortInfo(col, PandasApiSortDirection.ASC) for col in sorting_column_names]
        )

        start_bound = PandasApiFrameBound(PandasApiFrameBoundType.UNBOUNDED_PRECEDING)
        end_bound = PandasApiFrameBound(PandasApiFrameBoundType.CURRENT_ROW)
        window_frame = PandasApiWindowFrame(PandasApiWindowFrameMode.ROWS, start_bound, end_bound)

        return PandasApiWindow(partition_by, order_by, window_frame)

    def _validate(self) -> None:
        if self._min_periods != 1:
            raise NotImplementedError(
                "The expanding function is only supported for min_periods=1, "
                f"but got: min_periods={self._min_periods!r}"
            )

        if self._axis not in [0, "index"]:
            raise NotImplementedError(
                'The expanding function is only supported for axis=0 or axis="index", '
                f"but got: axis={self._axis!r}"
            )

        if self._method is not None:
            raise NotImplementedError(
                "The expanding function does not support the 'method' parameter, "
                f"but got: method={self._method!r}"
            )


class PandasApiRollingTdsFrame(PandasApiWindowTdsFrame[T], Generic[T]):
    _base_frame: T
    _min_periods: int
    _axis: PyLegendUnion[int, str]
    _method: PyLegendOptional[str]

    def __init__(
            self,
            base_frame: T,
            min_periods: int = 1,
            axis: PyLegendUnion[int, str] = 0,
            method: PyLegendOptional[str] = None
    ) -> None:
        self._base_frame = base_frame
        self._min_periods = min_periods
        self._axis = axis
        self._method = method
        self._validate()

    def base_frame(self) -> T:
        return self._base_frame

    def construct_window(self, sorting_column_names: PyLegendOptional[PyLegendList[str]]) -> PandasApiWindow:
        partition_by: PyLegendOptional[PyLegendList[str]] = None
        if isinstance(self._base_frame, PandasApiGroupbyTdsFrame):
            partition_by = [col.get_name() for col in self._base_frame.get_grouping_columns()]
        elif isinstance(self._base_frame, GroupbySeries):
            partition_by = [col.get_name() for col in self._base_frame.get_base_frame().get_grouping_columns()]

        order_by = (
            None if sorting_column_names is None or len(sorting_column_names) == 0 else
            [PandasApiSortInfo(col, PandasApiSortDirection.ASC) for col in sorting_column_names]
        )

        start_bound = PandasApiFrameBound(PandasApiFrameBoundType.UNBOUNDED_PRECEDING)
        end_bound = PandasApiFrameBound(PandasApiFrameBoundType.CURRENT_ROW)
        window_frame = PandasApiWindowFrame(PandasApiWindowFrameMode.ROWS, start_bound, end_bound)

        return PandasApiWindow(partition_by, order_by, window_frame)

    def _validate(self) -> None:
        if self._min_periods != 1:
            raise NotImplementedError(
                "The expanding function is only supported for min_periods=1, "
                f"but got: min_periods={self._min_periods!r}"
            )

        if self._axis not in [0, "index"]:
            raise NotImplementedError(
                'The expanding function is only supported for axis=0 or axis="index", '
                f"but got: axis={self._axis!r}"
            )

        if self._method is not None:
            raise NotImplementedError(
                "The expanding function does not support the 'method' parameter, "
                f"but got: method={self._method!r}"
            )
