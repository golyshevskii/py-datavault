class Hub:
    def __init__(
        self,
        hub_schema: str,
        hub_name: str,
        hub_key_column: str,
        db_conn_str: str,
    ):
        self.hub_schema = hub_schema
        self.hub_name = hub_name
        self.hub_key_column = hub_key_column
        self.db_conn_str = db_conn_str
