from pylegend.core.language.pandas_api.pandas_api_aggregate_specification import PyLegendAggInput
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.language import PyLegendPrimitiveOrPythonPrimitive
from pylegend._typing import (
    PyLegendOptional,
    PyLegendUnion,
)

class PandasApiExpandingTdsFrame:
    _base_frame: PandasApiBaseTdsFrame
    _min_periods: int
    _axis: PyLegendUnion[int, str]
    _method: PyLegendOptional[str]

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            min_periods: int = 1,
            axis: PyLegendUnion[int, str] = 0,
            method: PyLegendOptional[str] = None
    ) -> None:
        self._base_frame = base_frame
        self._min_periods = min_periods
        self._axis = axis
        self._method = method
        self._validate()

    def base_frame(self):
        return self._base_frame

    def aggregate(
            self,
            func: PyLegendAggInput,
            axis: PyLegendUnion[int, str] = 0,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiBaseTdsFrame":
        from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import (
            PandasApiAppliedFunctionTdsFrame
        )
        from pylegend.core.tds.pandas_api.frames.functions.pandas_api_window_aggregate_function import (
            PandasApiWindowAggregateFunction
        )
        return PandasApiAppliedFunctionTdsFrame(
            PandasApiWindowAggregateFunction(
                base_frame=self, func=func, axis=axis, *args, **kwargs
            )
        )

    def agg(
            self,
            func: PyLegendAggInput,
            *args: PyLegendPrimitiveOrPythonPrimitive,
            **kwargs: PyLegendPrimitiveOrPythonPrimitive
    ) -> "PandasApiBaseTdsFrame":
        return self.aggregate(func, *args, **kwargs)

    def _validate(self) -> None:
        if self._min_periods != 1:
            raise NotImplementedError(
                "The expanding function is only supported for min_periods=1, "
                f"but got: min_periods={self._min_periods}"
            )

        if self._axis not in [0, "index"]:
            raise NotImplementedError(
                'The expanding function is only supported for axis=0 or axis="index", '
                f"but got: axis={self._axis}"
            )

        if self._method is not None:
            raise NotImplementedError(
                "The expanding function does not support the 'method' parameter, "
                f"but got: method={self._method!r}"
            )
