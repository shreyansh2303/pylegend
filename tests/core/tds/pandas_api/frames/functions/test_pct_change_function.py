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

from textwrap import dedent

import pandas as pd
import pytest

from pylegend._typing import PyLegendDict, PyLegendUnion
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_relation_trade_service_frame_pandas_api


TEST_PURE: bool = True
USE_LEGEND_ENGINE: bool = True


class TestErrorsOnBaseFrame:

    def test_invalid_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.pct_change(axis=1)

        expected_msg = "The 'axis' argument of the pct_change function must be 0 or 'index', but got: axis=1"
        assert v.value.args[0] == expected_msg

    def test_unsupported_periods_argument(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.pct_change(periods=2)

        expected_msg = (
            "The 'periods' argument of the pct_change function is only supported for values [1, -1], but got: periods=2"
        )
        assert v.value.args[0] == expected_msg

        with pytest.raises(NotImplementedError) as v:
            frame.pct_change(periods=0)

        expected_msg = (
            "The 'periods' argument of the pct_change function is only supported for values [1, -1], but got: periods=0"
        )
        assert v.value.args[0] == expected_msg

        with pytest.raises(NotImplementedError) as v:
            frame.pct_change(periods=-3)

        expected_msg = (
            "The 'periods' argument of the pct_change function is only supported for values [1, -1], but got: periods=-3")
        assert v.value.args[0] == expected_msg

    def test_unsupported_freq_argument(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.pct_change(periods=1, freq="D")

        expected_msg = (
            "The 'freq' argument of the pct_change function is not supported, but got: freq='D'"
        )
        assert v.value.args[0] == expected_msg

    def test_additional_kwargs(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.pct_change(periods=1, fill_value=0, suffix='_sfx')

        expected_msg = (
            "Passing additional keyword arguments to the pct_change function is not supported, "
            "but got: fill_value=0, suffix='_sfx'"
        )
        assert v.value.args[0] == expected_msg
