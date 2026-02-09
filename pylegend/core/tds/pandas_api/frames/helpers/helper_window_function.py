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
    PyLegendUnion,
)
from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
from pylegend.core.language.pandas_api.pandas_api_series import Series
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame import PandasApiGroupbyTdsFrame


def get_true_base_frame(
        direct_base_frame: PyLegendUnion[PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame, Series, GroupbySeries]
) -> PandasApiBaseTdsFrame:
    if isinstance(direct_base_frame, PandasApiBaseTdsFrame):
        return direct_base_frame

    elif isinstance(direct_base_frame, PandasApiGroupbyTdsFrame):
        return direct_base_frame.base_frame()

    elif isinstance(direct_base_frame, GroupbySeries):
        groupby_base_frame = direct_base_frame.get_base_frame()
        return groupby_base_frame.base_frame()

    elif isinstance(direct_base_frame, Series):
        series_base_frame = direct_base_frame.get_base_frame()
        assert isinstance(series_base_frame, PandasApiBaseTdsFrame)
        return series_base_frame

    else:
        raise TypeError(
            "True base frame can only be obtained if the direct base frame is either "
            "PandasApiBaseTdsFrame, PandasApiGroupbyTdsFrame, Series, or GroupbySeries. "
            f"But got: {type(direct_base_frame).__name__}"
        )
