from typing import Dict
import pandas as pd
from pandas_etl.base_reader import ReaderBase
from pandas_etl.databases.mysql.base import MySQLBase


class MySQLReader(ReaderBase, MySQLBase):
    def __init__(self, read_config: Dict):
        super().__init__(read_config)

    def __repr__(self) -> str:
        return "MySQLReader"

    def read(self) -> pd.DataFrame:
        df = pd.read_sql_query(
            f"SELECT * FROM {self.config['table_name']}", con=self.engine
        )
        return df
