import io

import pyarrow as pa
from datasette import hookimpl
from datasette.utils.asgi import Response
from datasette.views.base import DatasetteError

CONTENT_TYPE = "application/vnd.apache.arrow.stream"

# SQLite values are dynamically typed but every Arrow column has one type, so
# each column gets the narrowest of these types that can hold all of the
# values it is known to contain. Later types can represent earlier ones.
INTEGER, FLOAT, STRING, BINARY = range(4)
ARROW_TYPES = {
    INTEGER: pa.int64(),
    FLOAT: pa.float64(),
    STRING: pa.string(),
    BINARY: pa.binary(),
}
PYTHON_KINDS = {int: INTEGER, float: FLOAT, str: STRING, bytes: BINARY}


def kind_for_declared_type(declared_type):
    """
    The kind of value a column declared with this type will hold, following
    SQLite's type affinity rules, or None if it could hold anything.
    """
    declared_type = (declared_type or "").upper()
    if "INT" in declared_type:
        return INTEGER
    if any(text in declared_type for text in ("CHAR", "CLOB", "TEXT")):
        return STRING
    if any(real in declared_type for real in ("REAL", "FLOA", "DOUB")):
        return FLOAT
    # BLOB and NUMERIC affinity columns store values as they were inserted
    return None


def kind_for_values(values, declared_kind=None):
    kinds = {PYTHON_KINDS[type(value)] for value in values if value is not None}
    if declared_kind is not None:
        kinds.add(declared_kind)
    # A column with no values and no declared type becomes a string column
    return max(kinds, default=STRING)


def coerce(value, kind, column):
    "Convert value to the type of its column, if that loses no information"
    if value is None:
        return None
    value_kind = PYTHON_KINDS[type(value)]
    if value_kind == kind:
        return value
    if value_kind < kind:
        if kind == FLOAT:
            return float(value)
        if kind == STRING:
            return str(value)
        return (value if value_kind == STRING else str(value)).encode("utf-8")
    raise ValueError(
        "Column {!r} is {} in this Arrow stream but has a {} value: {!r}".format(
            column, ARROW_TYPES[kind], type(value).__name__, value
        )
    )


def to_array(values, kind, column):
    arrow_type = ARROW_TYPES[kind]
    try:
        return pa.array(values, type=arrow_type)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        # Some values are not the column's type: convert them one at a time
        return pa.array(
            [coerce(value, kind, column) for value in values], type=arrow_type
        )


def split_labels(rows, columns, expanded_columns):
    """
    Turn rows into a list of values for each output column.

    ?_labels=on replaces foreign key values with {"value": ..., "label": ...}
    These become two columns, as in CSV: column and column_label.
    """
    by_column = list(zip(*rows)) if rows else [()] * len(columns)
    names = []
    values = []
    for column, cells in zip(columns, by_column):
        names.append(column)
        if column not in expanded_columns:
            values.append(list(cells))
            continue
        names.append(f"{column}_label")
        values.append(
            [cell["value"] if isinstance(cell, dict) else cell for cell in cells]
        )
        values.append(
            [
                str(cell["label"]) if isinstance(cell, dict) else None
                for cell in cells
            ]
        )
    return names, values


async def declared_kinds(datasette, database, table):
    "Returns {column name: kind} for the columns of a table with a declared type"
    if not table:
        return {}
    details = await datasette.get_database(database).table_column_details(table)
    kinds = {}
    for detail in details:
        kind = kind_for_declared_type(detail.type)
        if kind is not None:
            kinds[detail.name] = kind
    return kinds


async def render_arrows(datasette, columns, rows, row_stream, database, table, page, error):
    if error:
        raise DatasetteError(error, title="Invalid SQL", status=400)

    expanded_columns = set(page.get("expanded_columns") or [])
    declared = await declared_kinds(datasette, database, table)

    # The schema has to be sent before any rows, so it is based on the
    # declared column types plus the values in the first page of rows
    names, first_values = split_labels(rows, columns, expanded_columns)
    kinds = [
        kind_for_values(values, declared.get(name))
        for name, values in zip(names, first_values)
    ]
    schema = pa.schema([pa.field(name, ARROW_TYPES[kind]) for name, kind in zip(names, kinds)])

    async def body():
        buffer = io.BytesIO()
        writer = pa.ipc.new_stream(buffer, schema)

        def written():
            value = buffer.getvalue()
            buffer.seek(0)
            buffer.truncate()
            return value

        # One record batch for each page of rows
        async for page_rows in row_stream.pages():
            if not page_rows:
                continue
            _, page_values = split_labels(page_rows, columns, expanded_columns)
            arrays = [
                to_array(values, kind, name)
                for values, kind, name in zip(page_values, kinds, names)
            ]
            writer.write_batch(pa.record_batch(arrays, schema=schema))
            # The first chunk starts with the schema
            yield written()
        # Writes the end-of-stream marker - and the schema, if there were no rows
        writer.close()
        yield written()

    return Response.stream(body(), content_type=CONTENT_TYPE)


@hookimpl
def register_output_renderer():
    return {
        "extension": "arrows",
        "render": render_arrows,
        "extras": ["expanded_columns"],
        "label": "Arrow",
        "description": "Apache Arrow IPC stream, for DuckDB, pandas, Polars and other tools",
        "is_binary": True,
    }
