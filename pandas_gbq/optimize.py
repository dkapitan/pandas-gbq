import logging
import warnings
from pandas_gbq.gbq import GbqConnector, read_gbq
import numpy as np
import pandas as pd


# https://www.numpy.org/devdocs/user/basics.types.html
NUMPY_INT_RANGE = {
    'int8': (-128, 127),
    'int16': (-32768, 32767),
    'int32': (-2147483648, 2147483647),
    'int64': (-9223372036854775808, 9223372036854775807),
    'uint8': (0, 255),
    'uint16': (0, 65535),
    'uint32': (0, 4294967295),
    'uint64': (0, 18446744073709551615),
}


def _determine_int_type(min, max, nullcount):
    """
    Determine optimal np.int type based on (min, max) value.
    """
    if nullcount != 0:
        # return new pandas 0.24 Int64 with since we have nulls
        return 'Int64'
    if min >= 0:
        if max <= NUMPY_INT_RANGE['uint8'][1]:
            return np.uint8
        elif max <= NUMPY_INT_RANGE['uint16'][1]:
            return np.uint16
        elif max <= NUMPY_INT_RANGE['uint32'][1]:
            return np.uint32
        else:
            return np.uint64
    else:
        if (min >= NUMPY_INT_RANGE['int8'][0] and
                max <= NUMPY_INT_RANGE['int8'][1]):
            return np.int8
        if (min >= NUMPY_INT_RANGE['int16'][0] and
                max <= NUMPY_INT_RANGE['int16'][1]):
            return np.int16
        if (min >= NUMPY_INT_RANGE['int32'][0] and
                max <= NUMPY_INT_RANGE['int32'][1]):
            return np.int32
        else:
            return 'Int64'


def _select_columns_by_type(schema, bq_type):
    """
    Select columns from schema with type==bq_type
    """
    return [field['name']
            for field in schema
            for key, value in field.items()
            if key == 'type' and value == bq_type
            ]


def _generate_sql(schema):
    """
    Generates StandardSQL for reflection/inspection of
    - MIN,MAX,COUNTIF(IS NULL) for INTEGERS
    - COUNT(DISTINCT) for STRINGS
    """
    BQ_TYPES = ('INTEGER', 'STRING')
    col_by_type = {bq_type: _select_columns_by_type(schema, bq_type)
                   for bq_type in BQ_TYPES}
    select_clause_integers = (
        ', '.join(['MIN({column}) AS {column}_min, \
                    MAX({column}) AS {column}_max, \
                    COUNTIF({column} is NULL) AS {column}_countifnull'
                   .format(column=column)
                   for column in col_by_type['INTEGER']]))
    select_clause_strings = (
        ', '.join(['COUNT(DISTINCT {column}) AS {column}_distinct, \
            COUNT({column}) AS {column}_rowcount'
                   .format(column=column)
                   for column in col_by_type['STRING']]))
    return ('SELECT ' +
            select_clause_integers +
            ' , ' +
            select_clause_strings +
            f' FROM `{project_id}.{dataset_id}.{table_id}`'
            )


def _get_table_stats(project_id, dataset_id, table_id, schema):
    return (read_gbq(_generate_sql(schema),
                     project_id=project_id,
                     dialect='standard')
            .transpose()
            .reset_index()
            .assign(name=lambda df: df['index'].apply(
                    lambda x: '_'.join(x.split('_')[:-1])),
                    key=lambda df: df['index'].apply(
                    lambda x: (x.split('_')[-1])))
            .drop('index', axis=1)
            .set_index('name')
            .rename(columns={0: 'value'})
            .reindex(columns=['key', 'value'])
            .join(pd.DataFrame(schema).set_index('name'))
            )


# sandbox
project_id = 'mediquest-sandbox'
dataset_id = 'dkapitan'
table_id = 'ldf_episode'

conn = GbqConnector(project_id)
schema = conn.schema(dataset_id=dataset_id,
                     table_id=table_id)

table_stats = _get_table_stats(project_id, dataset_id, table_id, schema)
print(table_stats)

