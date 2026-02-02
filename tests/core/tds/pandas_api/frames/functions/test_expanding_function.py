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

import pytest
from pandas.tests.window.moments.conftest import min_periods

from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame


class TestErrors:
    def test_invalid_parameters(self):
        columns = [PrimitiveTdsColumn.string_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.expanding(min_periods=2)

        assert v.value.args[0] == "The expanding function is only supported for min_periods=1, but got: min_periods=2"

        with pytest.raises(NotImplementedError) as v:
            frame.expanding(axis=1)

        assert v.value.args[0] == "The expanding function is only supported for axis=0, but got: axis=1"

        with pytest.raises(NotImplementedError) as v:
            frame.expanding(method='single')

        assert v.value.args[0] == "The expanding function does not support the 'single' parameter, but got: method='single'"


class TestUsageOnBaseFrame:
    def test_simple_sum(self):
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.expanding().sum()

        expected_sql = '''
            SELECT
                SUM("root"."col1") OVER (ORDER BY "__internal_pylegend_column__" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "col1"
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
              ->extend(__internal_pylegend_column__:)
              ->extend(over([ascending(~col1)]), ~col1__internal_pylegend_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[
                col1:p|$p.col1__internal_pylegend_column__
              ])
        '''
