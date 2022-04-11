from typing import Dict
import pandas as pd
from pandas_etl.databases.mysql.base import MySQLBase
from pandas_etl.base_writer import WriterBase


class MySQLWriter(WriterBase, MySQLBase):
    def __init__(self, write_config: Dict):
        super().__init__(write_config)

    def __repr__(self) -> str:
        return "MySQLWriter"

    def write(self, df: pd.DataFrame) -> None:
        df.to_sql(
            self.config["table_name"],
            self.engine,
            if_exists="replace",
            index=False,
        )
