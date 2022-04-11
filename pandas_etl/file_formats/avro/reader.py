from typing import Dict
from pandas_etl.base_reader import ReaderBase
import pandas as pd
from pandas_etl.utils.config_validator import ConfigValidator
import fastavro


class AvroReader(ReaderBase):
    def __init__(self, read_config: Dict):
        self.read_config = read_config
        self._required_config_keys = ["path"]
        ConfigValidator().validate_required_keys(
            self.read_config, self._required_config_keys
        )

    def __repr__(self):
        return "AvroReader"

    def read(self) -> pd.DataFrame:
        if not fastavro.is_avro(self.read_config["path"]):
            raise Exception("not a valid avro file")

        with open(self.read_config["path"], "rb") as f:
            reader = fastavro.reader(f)
            records = [r for r in reader]
        df = pd.DataFrame.from_records(records)
        return df
