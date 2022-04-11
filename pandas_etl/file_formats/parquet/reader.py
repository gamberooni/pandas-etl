from typing import Dict
from pandas_etl.base_reader import ReaderBase
import pandas as pd
from pandas_etl.utils.config_validator import ConfigValidator


class ParquetReader(ReaderBase):
    def __init__(self, read_config: Dict):
        self.read_config = read_config
        self._required_config_keys = ["path"]
        ConfigValidator().validate_required_keys(
            self.read_config, self._required_config_keys
        )

    def __repr__(self):
        return "ParquetReader"

    def read(self) -> pd.DataFrame:
        df = pd.read_parquet(self.read_config["path"])
        return df
