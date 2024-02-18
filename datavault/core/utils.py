import logging
from inspect import currentframe
from typing import Any, Dict, List

from sqlalchemy import Engine, column, table
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger(__name__)


def insert_on_conflict_do_nothing(
    schema_name: str,
    table_name: str,
    columns: List[str],
    data: List[Dict[str, Any]],
    index_elements: List[str],
    engine: Engine,
):
    """
    Args:
        schema: Schema name
        table: Table name
        columns: Table column names
        data: List of dictionaries, example: [{'col1': 1, 'col2': 2}, {'col1': 3, 'col2': 4}]
        index_elements: List of primary column names
    """
    frame = currentframe().f_code.co_name
    column_clause = [column(c) for c in columns]
    table_clause = table(table_name, *column_clause, schema=schema_name)

    sql = (
        insert(table_clause)
        .values(data)
        .on_conflict_do_nothing(index_elements=index_elements)
    )
    result = engine.execute(sql)
    logger.info(f"{frame} | Rows inserted: {result.rowcount}")

    return result.rowcount


def insert_on_conflict_do_nothing_df(
    table, conn, keys, data_iter, index_elements
) -> int:
    """
    Args:
        table: pandas.io.sql.SQLTable
        conn: sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
        keys: Table column names
        data_iter: Iterable that iterates the values to be inserted
        primary_indexes: List of primary column names
    """
    data = [dict(zip(keys, row)) for row in data_iter]

    sql = (
        insert(table.table)
        .values(data)
        .on_conflict_do_nothing(index_elements=index_elements)
    )
    result = conn.execute(sql)

    return result.rowcount
