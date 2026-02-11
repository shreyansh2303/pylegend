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

import numpy as np
import pytest

from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame


class TestErrors:
    def test_invalid_parameters(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.expanding(min_periods=2)

        assert v.value.args[0] == "The expanding function is only supported for min_periods=1, but got: min_periods=2"

        with pytest.raises(NotImplementedError) as v:
            frame.expanding(axis=1)

        assert v.value.args[0] == 'The expanding function is only supported for axis=0 or axis="index", but got: axis=1'

        with pytest.raises(NotImplementedError) as v:
            frame.expanding(method='single')

        assert v.value.args[0] == "The expanding function does not support the 'method' parameter, but got: method='single'"


class TestUsageOnBaseFrame:
    def test_simple_sum(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.expanding().agg("sum")

        expected_sql = '''
            SELECT
                SUM("root"."col1") OVER (ORDER BY "root"."__internal_pylegend_column__" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col1",
                SUM("root"."col2") OVER (ORDER BY "root"."__internal_pylegend_column__" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        0 AS "__internal_pylegend_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over([ascending(~__internal_pylegend_column__)], rows(unbounded(), 0)), ~[
                col1__internal_pylegend_column__:{p,w,r | $r.col1}:{c | $c->sum()},
                col2__internal_pylegend_column__:{p,w,r | $r.col2}:{c | $c->sum()}
              ])
              ->project(~[
                col1:p|$p.col1__internal_pylegend_column__,
                col2:p|$p.col2__internal_pylegend_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure

    def test_complex_aggregation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.expanding().agg({
            "col1": ["sum", lambda x: x.count()],
            "col2": np.min
        })

        expected_sql = '''
            SELECT
                SUM("root"."col1") OVER (ORDER BY "root"."__internal_pylegend_column__" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "sum(col1)",
                COUNT("root"."col1") OVER (ORDER BY "root"."__internal_pylegend_column__" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "lambda_1(col1)",
                MIN("root"."col2") OVER (ORDER BY "root"."__internal_pylegend_column__" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        0 AS "__internal_pylegend_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over([ascending(~__internal_pylegend_column__)], rows(unbounded(), 0)), ~[
                'sum(col1)__internal_pylegend_column__':{p,w,r | $r.col1}:{c | $c->sum()},
                'lambda_1(col1)__internal_pylegend_column__':{p,w,r | $r.col1}:{c | $c->count()},
                col2__internal_pylegend_column__:{p,w,r | $r.col2}:{c | $c->min()}
              ])
              ->project(~[
                'sum(col1)':p|$p.'sum(col1)__internal_pylegend_column__',
                'lambda_1(col1)':p|$p.'lambda_1(col1)__internal_pylegend_column__',
                col2:p|$p.col2__internal_pylegend_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure


class TestUsageOnGroupbyFrame:
    def test_simple_sum(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grouping_col"),
            PrimitiveTdsColumn.integer_column("value_col"),
            PrimitiveTdsColumn.float_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("grouping_col").expanding().agg("sum")

        expected_sql = '''
            SELECT
                SUM("root"."value_col") OVER (PARTITION BY "root"."grouping_col" ORDER BY "root"."__internal_pylegend_column__" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "value_col",
                SUM("root"."random_col") OVER (PARTITION BY "root"."grouping_col" ORDER BY "root"."__internal_pylegend_column__" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "random_col"
            FROM
                (
                    SELECT
                        "root".grouping_col AS "grouping_col",
                        "root".value_col AS "value_col",
                        "root".random_col AS "random_col",
                        0 AS "__internal_pylegend_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[grouping_col], [ascending(~__internal_pylegend_column__)], rows(unbounded(), 0)), ~[
                value_col__internal_pylegend_column__:{p,w,r | $r.value_col}:{c | $c->sum()},
                random_col__internal_pylegend_column__:{p,w,r | $r.random_col}:{c | $c->sum()}
              ])
              ->project(~[
                value_col:p|$p.value_col__internal_pylegend_column__,
                random_col:p|$p.random_col__internal_pylegend_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure

    def test_complex_aggregation(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grouping_col"),
            PrimitiveTdsColumn.integer_column("value_col"),
            PrimitiveTdsColumn.float_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("grouping_col").expanding().agg({
            "value_col": ["sum", lambda x: x.count()],
            "random_col": np.min
        })

        expected_sql = '''
            SELECT
                SUM("root"."value_col") OVER (PARTITION BY "root"."grouping_col" ORDER BY "root"."__internal_pylegend_column__" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "sum(value_col)",
                COUNT("root"."value_col") OVER (PARTITION BY "root"."grouping_col" ORDER BY "root"."__internal_pylegend_column__" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "lambda_1(value_col)",
                MIN("root"."random_col") OVER (PARTITION BY "root"."grouping_col" ORDER BY "root"."__internal_pylegend_column__" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "random_col"
            FROM
                (
                    SELECT
                        "root".grouping_col AS "grouping_col",
                        "root".value_col AS "value_col",
                        "root".random_col AS "random_col",
                        0 AS "__internal_pylegend_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__internal_pylegend_column__:{r|0})
              ->extend(over(~[grouping_col], [ascending(~__internal_pylegend_column__)], rows(unbounded(), 0)), ~[
                'sum(value_col)__internal_pylegend_column__':{p,w,r | $r.value_col}:{c | $c->sum()},
                'lambda_1(value_col)__internal_pylegend_column__':{p,w,r | $r.value_col}:{c | $c->count()},
                random_col__internal_pylegend_column__:{p,w,r | $r.random_col}:{c | $c->min()}
              ])
              ->project(~[
                'sum(value_col)':p|$p.'sum(value_col)__internal_pylegend_column__',
                'lambda_1(value_col)':p|$p.'lambda_1(value_col)__internal_pylegend_column__',
                random_col:p|$p.random_col__internal_pylegend_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure

    def test_sum_on_groupby_series(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grouping_col"),
            PrimitiveTdsColumn.integer_column("value_col"),
            PrimitiveTdsColumn.float_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["sum_values"] = frame.groupby("grouping_col")["value_col"].expanding().agg("sum")

        expected_sql = '''
            SELECT
                "root".grouping_col AS "grouping_col",
                "root".value_col AS "value_col",
                "root".random_col AS "random_col",
                SUM("root".value_col) OVER (PARTITION BY "root".grouping_col ORDER BY 0 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "sum_values"
            FROM
                test_schema.test_table AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[grouping_col], [], rows(unbounded(), 0)), ~value_col__internal_pylegend_column__:{p,w,r | $r.value_col}:{c | $c->sum()})
              ->project(~[grouping_col:c|$c.grouping_col, value_col:c|$c.value_col, random_col:c|$c.random_col, sum_values:c|$c.value_col__internal_pylegend_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
