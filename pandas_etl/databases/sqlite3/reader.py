from typing import Dict
import sqlite3
import pandas as pd
from pandas_etl.base_reader import ReaderBase
from pandas_etl.utils.config_validator import ConfigValidator


class SQLite3Reader(ReaderBase):
    def __init__(self, read_config: Dict):
        self.read_config = read_config
        self._required_config_keys = ["path", "table_name"]
        ConfigValidator().validate_required_keys(
            self.read_config, self._required_config_keys
        )

    def __repr__(self) -> str:
        return "SQLite3Reader"

    def read(self) -> pd.DataFrame:
        conn = sqlite3.connect(self.read_config["path"])
        df = pd.read_sql_query(f"SELECT * FROM {self.read_config['table_name']}", conn)
        return df
