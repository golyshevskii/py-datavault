import logging
from inspect import currentframe
from typing import Any, Dict, List

from pandas import DataFrame
from sqlalchemy import create_engine

from ..core.utils import insert_on_conflict_do_nothing_df

logger = logging.getLogger(__name__)


class HubManager:
    def __init__(
        self,
        hub_schema: str,
        hub_name: str,
        hub_key_column: str,
        db_conn_str: str,
    ):
        """
        Args:
            hub_schema: HUB schema name
            hub_name: HUB table name
            hub_key_column: Key column name wich will be used for hash surrogate key in the HUB (usually it is bisness key)
            db_conn_str: Database connection string
        """
        self.hub_schema = hub_schema
        self.hub_name = hub_name
        self.hub_key_column = hub_key_column
        self.db_conn_str = db_conn_str
        self.db_engine = create_engine(db_conn_str)

    def create_hub(self):
        pass

    def update_hub(self, hub_data: List[Dict[str, Any]] | DataFrame):
        """
        Args:
            hub_data: List of dictionaries or Pandas DataFrame HUB data
        """
        frame = currentframe().f_code.co_name
        logger.debug(f"{frame} | START")

        if isinstance(hub_data, DataFrame):
            rows = hub_data.to_sql(
                schema=self.hub_schema,
                name=self.hub_name,
                con=self.db_engine,
                if_exists="append",
                index=False,
                method=lambda table, conn, keys, data_iter: insert_on_conflict_do_nothing_df(
                    table, conn, keys, data_iter, [self.hub_key_column]
                ),
            )
            logger.info(f"{frame} | Rows inserted: {rows}")
            logger.debug(f"{frame} | END")
            return

        logger.debug(f"{frame} | END")
        return
