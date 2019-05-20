"""Helper methods for BigQuery schemas"""
from numpy import uint8, int8, uint16, int16, uint32, int32


# https://www.numpy.org/devdocs/user/basics.types.html
NUMPY_INT_RANGE = {
    "int8": (-128, 127),
    "int16": (-32768, 32767),
    "int32": (-2147483648, 2147483647),
    "int64": (-9223372036854775808, 9223372036854775807),
    "uint8": (0, 255),
    "uint16": (0, 65535),
    "uint32": (0, 4294967295),
    "uint64": (0, 18446744073709551615),
}


def generate_bq_schema(dataframe, default_type="STRING"):
    """Given a passed dataframe, generate the associated Google BigQuery schema.

    Arguments:
        dataframe (pandas.DataFrame): D
    default_type : string
        The default big query type in case the type of the column
        does not exist in the schema.
    """

    # If you update this mapping, also update the table at
    # `docs/source/writing.rst`.
    type_mapping = {
        "i": "INTEGER",
        "b": "BOOLEAN",
        "f": "FLOAT",
        "O": "STRING",
        "S": "STRING",
        "U": "STRING",
        "M": "TIMESTAMP",
    }

    fields = []
    for column_name, dtype in dataframe.dtypes.iteritems():
        fields.append(
            {
                "name": column_name,
                "type": type_mapping.get(dtype.kind, default_type),
            }
        )

    return {"fields": fields}


def update_schema(schema_old, schema_new):
    """
    Given an old BigQuery schema, update it with a new one.

    Where a field name is the same, the new will replace the old. Any
    new fields not present in the old schema will be added.

    Arguments:
        schema_old: the old schema to update
        schema_new: the new schema which will overwrite/extend the old
    """
    old_fields = schema_old["fields"]
    new_fields = schema_new["fields"]
    output_fields = list(old_fields)

    field_indices = {field["name"]: i for i, field in enumerate(output_fields)}

    for field in new_fields:
        name = field["name"]
        if name in field_indices:
            # replace old field with new field of same name
            output_fields[field_indices[name]] = field
        else:
            # add new field
            output_fields.append(field)

    return {"fields": output_fields}


def select_columns_by_type(schema, bq_type):
    """
    Select columns from schema with type==bq_type

    We only downcast non-repeated INTEGER and STRING columns.
    """
    return [
        field["name"]
        for field in schema
        if field["type"] == bq_type and field["mode"] != "REPEATED"
    ]


def generate_sql(table_reference_string, schema):
    """
    Parameters
    ----------
    table : str
        Google Bigquery full table reference 'project_id.dataset_id.table_id'
   
    schema : dict
        Schema as returned by pandas_gbq.gbq.GbqConnector.schema()

    Returns
    -------
    str
        Bigquery standard-SQL statement for querying table statistics.

    Generates StandardSQL for reflection/inspection of
    - MIN,MAX,COUNTIF(IS NULL) for INTEGERS
    - COUNT(DISTINCT) for STRINGS
    """
    BQ_TYPES = ("INTEGER", "STRING")
    col_by_type = {
        bq_type: select_columns_by_type(schema, bq_type)
        for bq_type in BQ_TYPES
    }
    select_clause_integers = ", ".join(
        [
            "MIN({column}) AS {column}_min, \
                    MAX({column}) AS {column}_max, \
                    COUNTIF({column} is NULL) AS {column}_countifnull".format(
                column=column
            )
            for column in col_by_type["INTEGER"]
        ]
    )
    select_clause_strings = ", ".join(
        [
            "COUNT(DISTINCT {column}) AS {column}_countdistinct, \
            COUNT({column}) AS {column}_count, \
            SAFE_DIVIDE(COUNT(DISTINCT {column}), \
            COUNT({column})) AS {column}_fractiondistinct".format(
                column=column
            )
            for column in col_by_type["STRING"]
        ]
    )

    select_clause = ", ".join(
        filter(None, [select_clause_integers, select_clause_strings])
    )

    if select_clause:
        return " ".join(
            [
                "SELECT",
                select_clause,
                f" FROM `{table_reference_string}`",
            ]
        )

    else:
        # no non-repeated INT64 or STRING columns
        # TO DO: handle exception more gracefully
        pass


def _determine_int_type(min, max, nullcount):
    """
    Determine optimal np.int type based on (min, max) value.
    """
    if nullcount != 0:
        # return new pandas 0.24 Int64 with since we have nulls
        return "Int64"
    if min >= 0:
        if max <= NUMPY_INT_RANGE["uint8"][1]:
            return uint8
        elif max <= NUMPY_INT_RANGE["uint16"][1]:
            return uint16
        elif max <= NUMPY_INT_RANGE["uint32"][1]:
            return uint32
        else:
            return "Int64"
    else:
        if (
            min >= NUMPY_INT_RANGE["int8"][0]
            and max <= NUMPY_INT_RANGE["int8"][1]
        ):
            return int8
        if (
            min >= NUMPY_INT_RANGE["int16"][0]
            and max <= NUMPY_INT_RANGE["int16"][1]
        ):
            return int16
        if (
            min >= NUMPY_INT_RANGE["int32"][0]
            and max <= NUMPY_INT_RANGE["int32"][1]
        ):
            return int32
        else:
            return "Int64"


def _determine_string_type(fraction_unique, threshold=0.5):
    """
    """
    return 'category' if fraction_unique < threshold else "object"
