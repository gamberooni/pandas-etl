from typing import Dict
from pandas_etl.base_writer import WriterBase
import pandas as pd
from pandas_etl.utils.config_validator import ConfigValidator
from fastavro import writer, parse_schema
from fastavro.validation import validate_many


class AvroWriter(WriterBase):
    def __init__(self, write_config: Dict):
        self.write_config = write_config
        self._required_config_keys = ["path", "schema"]
        ConfigValidator().validate_required_keys(
            self.write_config, self._required_config_keys
        )

    def __repr__(self):
        return "AvroWriter"

    def write(self, df: pd.DataFrame) -> None:
        parsed_schema = parse_schema(self.write_config["schema"])
        records = df.to_dict("records")
        valid_schema = validate_many(records, parsed_schema, raise_errors=False)
        if not valid_schema:
            raise Exception("contains records that does not correspond to the schema")

        with open(self.write_config["path"], "wb") as f:
            writer(f, parsed_schema, records)
