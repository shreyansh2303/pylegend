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

    def test_non_numeric_columns(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.datetime_column("col3"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.pct_change(periods=-1)

        expected_msg = (
            f"The pct_change function can only be applied to the following column types: "
            f"['Integer', 'Float', 'Number'], but got the following invalid columns: "
            f"['TdsColumn(Name: col2, Type: String)', 'TdsColumn(Name: col3, Type: DateTime)']"
        )
        assert v.value.args[0] == expected_msg


class TestUsageOnBaseFrame:

    if USE_LEGEND_ENGINE:
        @pytest.fixture(autouse=True)
        def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
            self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_no_arguments(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.pct_change()

        expected = '''
            SELECT
                ((1.0 * ("root"."col1" - "root"."col1__pylegend_internal_column_name__")) / "root"."col1__pylegend_internal_column_name__") AS "col1"
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
              ->extend(over([ascending(~__pylegend_internal_column_name__)]), ~col1__pylegend_internal_column_name__:{p,w,r | (toOne($r.col1) - toOne($p->lag($r).col1)) / toOne($p->lag($r).col1)})
              ->project(~[col1:p|$p.col1__pylegend_internal_column_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_pct_change_negative_periods_argument(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.pct_change(periods=-1)

        expected = '''
            SELECT
                ((1.0 * ("root"."col1" - "root"."col1__pylegend_internal_column_name__")) / "root"."col1__pylegend_internal_column_name__") AS "col1"
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
              ->extend(over([ascending(~__pylegend_internal_column_name__)]), ~col1__pylegend_internal_column_name__:{p,w,r | (toOne($r.col1) - toOne($p->lead($r).col1)) / toOne($p->lead($r).col1)})
              ->project(~[col1:p|$p.col1__pylegend_internal_column_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected


class TestUsageOnGroupbyFrame:

    if USE_LEGEND_ENGINE:
        @pytest.fixture(autouse=True)
        def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
            self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_positive_periods_argument(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("group_col"),
                   PrimitiveTdsColumn.integer_column("val_col"),
                   PrimitiveTdsColumn.integer_column("random_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col").pct_change(periods=1)

        expected = '''
            SELECT
                ((1.0 * ("root"."val_col" - "root"."val_col__pylegend_internal_column_name__")) / "root"."val_col__pylegend_internal_column_name__") AS "val_col",
                ((1.0 * ("root"."random_col" - "root"."random_col__pylegend_internal_column_name__")) / "root"."random_col__pylegend_internal_column_name__") AS "random_col"
            FROM
                (
                    SELECT
                        "root".group_col AS "group_col",
                        "root".val_col AS "val_col",
                        "root".random_col AS "random_col",
                        lag("root"."val_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__pylegend_internal_column_name__") AS "val_col__pylegend_internal_column_name__",
                        lag("root"."random_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__pylegend_internal_column_name__") AS "random_col__pylegend_internal_column_name__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col",
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
              ->extend(over(~[group_col], [ascending(~__pylegend_internal_column_name__)]), ~val_col__pylegend_internal_column_name__:{p,w,r | (toOne($r.val_col) - toOne($p->lag($r).val_col)) / toOne($p->lag($r).val_col)})
              ->extend(over(~[group_col], [ascending(~__pylegend_internal_column_name__)]), ~random_col__pylegend_internal_column_name__:{p,w,r | (toOne($r.random_col) - toOne($p->lag($r).random_col)) / toOne($p->lag($r).random_col)})
              ->project(~[val_col:p|$p.val_col__pylegend_internal_column_name__, random_col:p|$p.random_col__pylegend_internal_column_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_negative_periods_argument_with_selection(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("group_col"),
                   PrimitiveTdsColumn.integer_column("val_col"),
                   PrimitiveTdsColumn.integer_column("random_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col")["val_col"].pct_change(periods=-1)

        expected = '''
            SELECT
                ((1.0 * ("root"."val_col" - "root"."val_col__pylegend_internal_column_name__")) / "root"."val_col__pylegend_internal_column_name__") AS "val_col"
            FROM
                (
                    SELECT
                        "root".group_col AS "group_col",
                        "root".val_col AS "val_col",
                        "root".random_col AS "random_col",
                        lead("root"."val_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__pylegend_internal_column_name__") AS "val_col__pylegend_internal_column_name__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col",
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
              ->extend(over(~[group_col], [ascending(~__pylegend_internal_column_name__)]), ~val_col__pylegend_internal_column_name__:{p,w,r | (toOne($r.val_col) - toOne($p->lead($r).val_col)) / toOne($p->lead($r).val_col)})
              ->project(~[val_col:p|$p.val_col__pylegend_internal_column_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_pct_change_self_selection_of_grouping_column(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("group_col"),
                   PrimitiveTdsColumn.integer_column("group_col_2"),
                   PrimitiveTdsColumn.integer_column("val_col"),
                   PrimitiveTdsColumn.integer_column("random_col"),
                   PrimitiveTdsColumn.integer_column("random_col_2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby(["group_col", "group_col_2"])[["group_col", "val_col", "random_col"]].pct_change(periods=1)

        expected = '''
            SELECT
                ((1.0 * ("root"."group_col" - "root"."group_col__pylegend_internal_column_name__")) / "root"."group_col__pylegend_internal_column_name__") AS "group_col",
                ((1.0 * ("root"."val_col" - "root"."val_col__pylegend_internal_column_name__")) / "root"."val_col__pylegend_internal_column_name__") AS "val_col",
                ((1.0 * ("root"."random_col" - "root"."random_col__pylegend_internal_column_name__")) / "root"."random_col__pylegend_internal_column_name__") AS "random_col"
            FROM
                (
                    SELECT
                        "root".group_col AS "group_col",
                        "root".group_col_2 AS "group_col_2",
                        "root".val_col AS "val_col",
                        "root".random_col AS "random_col",
                        "root".random_col_2 AS "random_col_2",
                        lag("root"."group_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."__pylegend_internal_column_name__") AS "group_col__pylegend_internal_column_name__",
                        lag("root"."val_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."__pylegend_internal_column_name__") AS "val_col__pylegend_internal_column_name__",
                        lag("root"."random_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."__pylegend_internal_column_name__") AS "random_col__pylegend_internal_column_name__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".group_col_2 AS "group_col_2",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col",
                                "root".random_col_2 AS "random_col_2",
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
              ->extend(over(~[group_col, group_col_2], [ascending(~__pylegend_internal_column_name__)]), ~group_col__pylegend_internal_column_name__:{p,w,r | (toOne($r.group_col) - toOne($p->lag($r).group_col)) / toOne($p->lag($r).group_col)})
              ->extend(over(~[group_col, group_col_2], [ascending(~__pylegend_internal_column_name__)]), ~val_col__pylegend_internal_column_name__:{p,w,r | (toOne($r.val_col) - toOne($p->lag($r).val_col)) / toOne($p->lag($r).val_col)})
              ->extend(over(~[group_col, group_col_2], [ascending(~__pylegend_internal_column_name__)]), ~random_col__pylegend_internal_column_name__:{p,w,r | (toOne($r.random_col) - toOne($p->lag($r).random_col)) / toOne($p->lag($r).random_col)})
              ->project(~[group_col:p|$p.group_col__pylegend_internal_column_name__, val_col:p|$p.val_col__pylegend_internal_column_name__, random_col:p|$p.random_col__pylegend_internal_column_name__])
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

    @pytest.mark.skip(reason="Legend server doesn't execute this SQL")
    def test_pct_change_no_arguments(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_trade_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_output = frame[["Id", "Quantity"]].pct_change().execute_frame_to_pandas_df()
        pandas_output = pandas_df_simple_person[["Id", "Quantity"]].pct_change()

        assert_frame_equal(pylegend_output, pandas_output)

    @pytest.mark.skip(reason="Legend server doesn't execute this SQL")
    def test_pct_change_negative_periods(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_trade_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_output = frame[["Id", "Quantity"]].pct_change(periods=-1).execute_frame_to_pandas_df()
        pandas_output = pandas_df_simple_person[["Id", "Quantity"]].pct_change(periods=-1)

        assert_frame_equal(pylegend_output, pandas_output)


class TestEndToEndUsageOnGroupbyFrame:

    @pytest.mark.skip(reason="Legend server doesn't execute this SQL")
    def test_groupby_pct_change_positive_periods(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_trade_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_output = frame.groupby("Product/Name")[["Id", "Quantity"]].pct_change().execute_frame_to_pandas_df()
        pandas_output = pandas_df_simple_person.groupby("Product/Name")[["Id", "Quantity"]].pct_change()

        assert_frame_equal(pylegend_output, pandas_output)

    @pytest.mark.skip(reason="Legend server doesn't execute this SQL")
    def test_groupby_pct_change_negative_periods(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_trade_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_output = frame.groupby("Product/Name")[["Id", "Quantity"]].pct_change(periods=-1).execute_frame_to_pandas_df()
        pandas_output = pandas_df_simple_person.groupby("Product/Name")[["Id", "Quantity"]].pct_change(periods=-1)

        assert_frame_equal(pylegend_output, pandas_output)
