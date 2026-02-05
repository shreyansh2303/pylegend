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
