from typing import Dict
from pandas_etl.base_writer import WriterBase
import pandas as pd
from pandas_etl.utils.config_validator import ConfigValidator


class CSVWriter(WriterBase):
    def __init__(self, write_config: Dict):
        self.write_config = write_config
        self._required_config_keys = ["path_or_buf"]
        ConfigValidator().validate_required_keys(
            self.write_config, self._required_config_keys
        )

    def __repr__(self):
        return "CSVWriter"

    def write(self, df: pd.DataFrame) -> None:
        df.to_csv(**self.write_config)
