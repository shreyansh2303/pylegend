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
    PyLegendSequence,
)
from pylegend.core.language import (
    PyLegendBoolean,
    PyLegendDate,
    PyLegendDateTime,
    PyLegendFloat,
    PyLegendInteger,
    PyLegendNumber,
    PyLegendPrimitive,
    PyLegendStrictDate,
    PyLegendString,
)
from pylegend.core.tds.tds_column import (
    PrimitiveTdsColumn,
    TdsColumn,
)


__all__: PyLegendSequence[str] = [
    "infer_column_from_expression",
]


def infer_column_from_expression(name: str, expr: PyLegendPrimitive) -> TdsColumn:
    if isinstance(expr, PyLegendInteger):
        return PrimitiveTdsColumn.integer_column(name)
    elif isinstance(expr, PyLegendFloat):
        return PrimitiveTdsColumn.float_column(name)
    elif isinstance(expr, PyLegendNumber):
        return PrimitiveTdsColumn.number_column(name)
    elif isinstance(expr, PyLegendString):
        return PrimitiveTdsColumn.string_column(name)
    elif isinstance(expr, PyLegendBoolean):
        return PrimitiveTdsColumn.boolean_column(name)  # pragma: no cover
    elif isinstance(expr, PyLegendDate):
        return PrimitiveTdsColumn.date_column(name)
    elif isinstance(expr, PyLegendDateTime):
        return PrimitiveTdsColumn.datetime_column(name)
    elif isinstance(expr, PyLegendStrictDate):
        return PrimitiveTdsColumn.strictdate_column(name)
    else:
        raise TypeError(f"Could not infer TdsColumn type for aggregation result type: {type(expr)}")  # pragma: no cover
