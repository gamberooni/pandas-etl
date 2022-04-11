from typing import Dict
import sqlite3
import pandas as pd
from pandas_etl.base_writer import WriterBase
from pandas_etl.utils.config_validator import ConfigValidator


class SQLite3Writer(WriterBase):
    def __init__(self, write_config: Dict):
        self.write_config = write_config
        self._required_config_keys = ["path", "table_name"]
        ConfigValidator().validate_required_keys(
            self.write_config, self._required_config_keys
        )

    def __repr__(self) -> str:
        return "SQLite3Writer"

    def write(self, df: pd.DataFrame) -> None:
        conn = sqlite3.connect(self.write_config["path"])
        df.to_sql(
            self.write_config["table_name"], conn, if_exists="replace", index=False
        )
