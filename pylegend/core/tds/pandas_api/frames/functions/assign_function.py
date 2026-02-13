# Copyright 2023 Goldman Sachs
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
import base64
import json
import re
from datetime import date, datetime

from pylegend._typing import (
    PyLegendList,
    PyLegendSequence,
    PyLegendDict,
    PyLegendCallable,
    PyLegendUnion,
)
from pylegend.core.language import (
    PyLegendPrimitive,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendNumber,
    PyLegendBoolean,
    PyLegendString,
    PyLegendDate,
    PyLegendDateTime
)
from pylegend.core.language.pandas_api.pandas_api_series import Series
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.helpers import escape_column_name, generate_pure_lambda
from pylegend.core.language.shared.literal_expressions import convert_literal_to_literal_expression
from pylegend.core.language.shared.pure_expression import PureExpression
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    SingleColumn,
)
from pylegend.core.tds.pandas_api.frames.functions.rank_function import RankFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_applied_function_tds_frame import PandasApiAppliedFunction
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.sql_query_helpers import copy_query, create_sub_query
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig


class AssignFunction(PandasApiAppliedFunction):
    __base_frame: PandasApiBaseTdsFrame
    __col_definitions: PyLegendDict[
        str,
        PyLegendCallable[[PandasApiTdsRow], PyLegendUnion[int, float, bool, str, date, datetime, PyLegendPrimitive]],
    ]

    @classmethod
    def name(cls) -> str:
        return "assign"  # pragma: no cover

    def __init__(
            self,
            base_frame: PandasApiBaseTdsFrame,
            col_definitions: PyLegendDict[
                str,
                PyLegendCallable[
                    [PandasApiTdsRow], PyLegendUnion[int, float, bool, str, date, datetime, PyLegendPrimitive]],
            ]
    ) -> None:
        self.__base_frame = base_frame
        self.__col_definitions = col_definitions

    def to_sql(self, config: FrameToSqlConfig) -> QuerySpecification:
        db_extension = config.sql_to_string_generator().get_db_extension()
        base_query = self.__base_frame.to_sql_query_object(config)
        should_create_sub_query = (len(base_query.groupBy) > 0) or base_query.select.distinct

        new_query = (
            create_sub_query(base_query, config, "root") if should_create_sub_query else
            copy_query(base_query)
        )

        base_cols = {c.get_name() for c in self.__base_frame.columns()}
        tds_row = PandasApiTdsRow.from_tds_frame("c", self.__base_frame)
        for col, func in self.__col_definitions.items():
            res = func(tds_row)
            res_expr = res if isinstance(res, PyLegendPrimitive) else convert_literal_to_literal_expression(res)
            new_col_expr = res_expr.to_sql_expression(
                {"c": base_query},
                config
            )

            alias = db_extension.quote_identifier(col)
            if col in base_cols:
                for i, si in enumerate(new_query.select.selectItems):
                    if isinstance(si, SingleColumn) and si.alias == alias:
                        new_query.select.selectItems[i] = SingleColumn(alias=alias, expression=new_col_expr)

            else:
                new_query.select.selectItems.append(SingleColumn(alias=alias, expression=new_col_expr))
        return new_query

    def _process_pure_expr_to_extend(self, pure_expr: str, new_col_name: str) -> str:
        if "__RankFunction__" in pure_expr:
            regex_pattern = r"__RankFunction__\(([^)]+)\)"
            matches = list(re.finditer(regex_pattern, pure_expr))
            if len(matches) > 1:
                raise NotImplementedError(
                    "Cannot combine multiple Series objects together in a single statement. "
                    "For instance, instead of frame['new_col'] = frame['c1'].rank() + frame['c2'].rank(), do "
                    "frame['new_col'] = frame['c1'].rank(); frame['new_col'] += frame['c2'].rank()"
                )
            elif len(matches) == 1:
                match = matches[0]
                full_match_string = match.group(0)
                base64_payload = match.group(1)
                decoded_payload = json.loads(base64.b64decode(base64_payload).decode('utf-8'))
                window = decoded_payload["window"]
                function = decoded_payload["function"]
                replaced_pure_expr = pure_expr.replace(full_match_string, function)
                final_pure_lambda = generate_pure_lambda("p,w,r", replaced_pure_expr)
                return f"->extend({window}, ~{new_col_name}:{final_pure_lambda})"
            else:
                raise RuntimeError("RankFunction not found in pure expression")
        else:
            return f"->extend({new_col_name}:{generate_pure_lambda("c", pure_expr)})"

    def to_pure(self, config: FrameToPureConfig) -> str:
        col_name_suffix = "__INTERNAL_PYLEGEND_COLUMN__"

        tds_row = PandasApiTdsRow.from_tds_frame("c", self.__base_frame)
        base_cols = set(c.get_name() for c in self.__base_frame.columns())

        extends: PyLegendList[str] = []
        projects: PyLegendList[str] = []
        for col_name, func in self.__col_definitions.items():
            res = func(tds_row)
            res_expr = res if isinstance(res, PyLegendPrimitive) else convert_literal_to_literal_expression(res)
            pure_expr = res_expr.to_pure_expression(config)

            if col_name not in base_cols:
                extend = self._process_pure_expr_to_extend(pure_expr, escape_column_name(col_name))
                extends.append(extend)
            else:
                extend = self._process_pure_expr_to_extend(pure_expr, escape_column_name(col_name+col_name_suffix))
                project_lambda = generate_pure_lambda(
                    "c", f"$c.{escape_column_name(col_name+col_name_suffix)}"
                )
                project = f"{escape_column_name(col_name)}:{project_lambda}"

                extends.append(extend)
                projects.append(project)

        combined_extend = ""
        if len(extends) > 0:
            combined_extend = (
                config.separator(1) + config.separator(1).join(extends)
            )

        combined_project = ""
        if len(projects) > 0:
            combined_project = (
                config.separator(1) + "->project(~[" +
                config.separator(2) + config.separator(2).join(projects) +
                config.separator(1) + "])"
            )

        return self.__base_frame.to_pure(config) + combined_extend + combined_project

    def base_frame(self) -> PandasApiBaseTdsFrame:
        return self.__base_frame

    def tds_frame_parameters(self) -> PyLegendList["PandasApiBaseTdsFrame"]:
        return []

    def calculate_columns(self) -> PyLegendSequence["TdsColumn"]:
        new_cols = [c.copy() for c in self.__base_frame.columns()]
        base_cols = {c.get_name() for c in self.__base_frame.columns()}
        tds_row = PandasApiTdsRow.from_tds_frame("frame", self.__base_frame)
        for col, func in self.__col_definitions.items():
            if col not in base_cols:
                res = func(tds_row)
                if isinstance(res, (int, PyLegendInteger)):
                    new_cols.append(PrimitiveTdsColumn.integer_column(col))
                elif isinstance(res, (float, PyLegendFloat)):
                    new_cols.append(PrimitiveTdsColumn.float_column(col))
                elif isinstance(res, PyLegendNumber):
                    new_cols.append(PrimitiveTdsColumn.number_column(col))  # pragma: no cover
                elif isinstance(res, (bool, PyLegendBoolean)):
                    new_cols.append(
                        PrimitiveTdsColumn.boolean_column(col)
                    )  # pragma: no cover (Boolean column not supported in PURE)
                elif isinstance(res, (str, PyLegendString)):
                    new_cols.append(PrimitiveTdsColumn.string_column(col))
                elif isinstance(res, (datetime, PyLegendDateTime)):
                    new_cols.append(PrimitiveTdsColumn.datetime_column(col))
                elif isinstance(res, (date, PyLegendDate)):
                    new_cols.append(PrimitiveTdsColumn.date_column(col))
                else:
                    raise RuntimeError("Type not supported")
        return new_cols

    def validate(self) -> bool:
        tds_row = PandasApiTdsRow.from_tds_frame("frame", self.__base_frame)
        for col, f in self.__col_definitions.items():
            f(tds_row)
        return True
