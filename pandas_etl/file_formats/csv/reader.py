from typing import Dict
from pandas_etl.base_reader import ReaderBase
import pandas as pd
from pandas_etl.utils.config_validator import ConfigValidator


class CSVReader(ReaderBase):
    def __init__(self, read_config: Dict):
        self.read_config = read_config
        self._required_config_keys = ["filepath_or_buffer"]
        ConfigValidator().validate_required_keys(
            self.read_config, self._required_config_keys
        )

    def __repr__(self):
        return "CSVReader"

    def read(self) -> pd.DataFrame:
        df = pd.read_csv(**self.read_config)
        return df
