import pandas as pd
from pandas_etl.base_reader import ReaderBase
from pandas_etl.base_writer import WriterBase
from typing import Callable
import logging


class ETL:
    def __init__(
        self,
        reader: ReaderBase,
        writer: WriterBase,
        transform_fn: Callable[[pd.DataFrame], pd.DataFrame],
    ):
        self.reader = reader
        self.writer = writer
        self.transform_fn = transform_fn

    def read_from_source(self) -> pd.DataFrame:
        df = self.reader.read()
        return df

    def write_to_destination(self, df: pd.DataFrame) -> None:
        self.writer.write(df)

    def execute(self) -> None:
        df = self.read_from_source()
        df_transformed = self.transform_fn(df)
        self.write_to_destination(df_transformed)
        logging.info(
            f"completed etl step with reader '{self.reader}' and writer '{self.writer}'"
        )
        return self.writer.__dict__
