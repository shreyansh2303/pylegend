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
from tests.test_helpers.test_legend_service_frames import simple_relation_person_service_frame_pandas_api, simple_relation_trade_service_frame_pandas_api


TEST_PURE: bool = True
USE_LEGEND_ENGINE: bool = False


class TestErrorsOnBaseFrame:
    def test_invalid_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.diff(axis=1)

        expected_msg = "The 'axis' argument of the diff function must be 0 or 'index', but got: axis=1"
        assert v.value.args[0] == expected_msg

    def test_unsupported_periods_argument(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.diff(periods=0)

        expected_msg = (
            "The 'periods' argument of the diff function is only supported for values [1, -1], but got: periods=0")
        assert v.value.args[0] == expected_msg

        with pytest.raises(NotImplementedError) as v:
            frame.diff(periods=3)

        expected_msg = (
            "The 'periods' argument of the diff function is only supported for values [1, -1], but got: periods=3")
        assert v.value.args[0] == expected_msg

        with pytest.raises(NotImplementedError) as v:
            frame.diff(periods=-3)

        expected_msg = (
            "The 'periods' argument of the diff function is only supported for values [1, -1], but got: periods=-3")
        assert v.value.args[0] == expected_msg


class TestUsageOnBaseFrame:
    if USE_LEGEND_ENGINE:
        @pytest.fixture(autouse=True)
        def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
            self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_no_arguments(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.diff()

        expected = '''
            SELECT
                ("root"."col1" - "root"."col1__pylegend_internal_column_name__") AS "col1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        lag("root"."col1", 1) OVER (ORDER BY "root"."__pylegend_internal_column_name__") AS "col1__pylegend_internal_column_name__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                0 AS "__pylegend_internal_column_name__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_internal_column_name__:{r|0})
              ->extend(over([ascending(~__pylegend_internal_column_name__)]), ~col1__pylegend_internal_column_name__:{p,w,r | toOne($r.col1) - toOne($p->lag($r).col1)})
              ->project(~[col1:p|$p.col1__pylegend_internal_column_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected
    
    def test_negative_periods_argument(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.diff(periods=-1)

        expected = '''
            SELECT
                ("root"."col1" - "root"."col1__pylegend_internal_column_name__") AS "col1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        lead("root"."col1", 1) OVER (ORDER BY "root"."__pylegend_internal_column_name__") AS "col1__pylegend_internal_column_name__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                0 AS "__pylegend_internal_column_name__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_internal_column_name__:{r|0})
              ->extend(over([ascending(~__pylegend_internal_column_name__)]), ~col1__pylegend_internal_column_name__:{p,w,r | toOne($r.col1) - toOne($p->lead($r).col1)})
              ->project(~[col1:p|$p.col1__pylegend_internal_column_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected


@pytest.fixture(scope="class")
def pandas_df_simple_person() -> pd.DataFrame:
    rows = [
        {"values": [1, "2014-12-01", 25, "2014-12-02T21:00:00.000000000+0000", "Firm X", "Account 1"]},
        {"values": [2, "2014-12-01", 320, "2014-12-02T21:00:00.000000000+0000", "Firm X", "Account 2"]},
        {"values": [3, "2014-12-01", 11, "2014-12-02T21:00:00.000000000+0000", "Firm A", "Account 1"]},
        {"values": [4, "2014-12-02", 23, "2014-12-03T21:00:00.000000000+0000", "Firm A", "Account 2"]},
        {"values": [5, "2014-12-02", 32, "2014-12-03T21:00:00.000000000+0000", "Firm A", "Account 1"]},
        {"values": [6, "2014-12-03", 27, "2014-12-04T21:00:00.000000000+0000", "Firm C", "Account 1"]},
        {"values": [7, "2014-12-03", 44, "2014-12-04T15:22:23.123456789+0000", "Firm C", "Account 1"]},
        {"values": [8, "2014-12-04", 22, "2014-12-05T21:00:00.000000000+0000", "Firm C", "Account 2"]},
        {"values": [9, "2014-12-04", 45, "2014-12-05T21:00:00.000000000+0000", "Firm C", "Account 2"]},
        {"values": [10, "2014-12-04", 38, None, "Firm C", "Account 2"]},
        {"values": [11, "2014-12-05", 5, None, None, None]},
    ]

    return pd.DataFrame(
        [row["values"] for row in rows],
        columns=["Id", "Date", "Quantity", "Settlement Date Time", "Product/Name", "Account/Name"],
    )


def assert_frame_equal(left: pd.DataFrame, right: pd.DataFrame) -> None:
    pd.testing.assert_frame_equal(
        left=left,
        right=right,
        check_dtype=False,
        check_exact=False,
        check_like=True
    )


class TestEndToEndUsageOnBaseFrame:

    def test_no_arguments(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_trade_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_output = frame[["Id", "Quantity"]].diff().execute_frame_to_pandas_df()
        pandas_output = pandas_df_simple_person[["Id", "Quantity"]].diff()

        assert_frame_equal(pylegend_output, pandas_output)

    def test_negative_periods(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_trade_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_output = frame.diff(periods=-1).execute_frame_to_pandas_df()
        pandas_output = pandas_df_simple_person.diff(periods=-1)

        assert_frame_equal(pylegend_output, pandas_output)
