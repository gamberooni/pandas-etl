import pandas as pd
from pandas_etl.databases.postgres.reader import PostgresReader
from pandas_etl.databases.postgres.writer import PostgresWriter
from pandas_etl.databases.mysql.reader import MySQLReader
from pandas_etl.databases.mysql.writer import MySQLWriter
from pandas_etl.databases.sqlite3.writer import SQLite3Writer
from pandas_etl.databases.sqlite3.reader import SQLite3Reader
from pandas_etl.file_formats.avro.reader import AvroReader
from pandas_etl.file_formats.avro.writer import AvroWriter
from pandas_etl.file_formats.csv.reader import CSVReader
from pandas_etl.file_formats.csv.writer import CSVWriter
from pandas_etl.file_formats.json.reader import JSONReader
from pandas_etl.file_formats.json.writer import JSONWriter
from pandas_etl.file_formats.parquet.reader import ParquetReader
from pandas_etl.file_formats.parquet.writer import ParquetWriter
from pandas_etl.etl import ETL
import logging
logging.basicConfig(level=logging.INFO)


def transform_first(df):
    df['order_date'] = pd.to_datetime(df['order_date'])
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    You could call multiple other functions in this 'transform' function, as long as it takes in
    a dataframe and returns a dataframe.
    """
    df = transform_first(df)
    df_final = df.groupby(
        [
            df['order_date'].dt.year.rename('order_year'), 
            df['order_date'].dt.month_name().rename('order_month')
        ]
    )['amount'].sum().reset_index()
    return df_final


def main():
    # --------------
    #   DATABASES   
    # --------------

    sqlite3_reader = SQLite3Reader({
        'path': 'dummy_data.db',
        'table_name': 'dummy_table'
    })

    sqlite3_writer = SQLite3Writer({
        'path': 'transformed.db',
        'table_name': 'dummy_table'
    })

    postgres_reader = PostgresReader({
        'user': 'postgres',
        'dbname': 'orders',
        'password': 'mypassword',
        'table_name': 'orders_dummy'
    })

    postgres_writer = PostgresWriter({
        'user': 'postgres',
        'dbname': 'orders',
        'password': 'mypassword',
        'table_name': 'orders_transformed'
    })

    mysql_reader = MySQLReader({
        'user': 'mysql',
        'dbname': 'orders',
        'password': 'mypassword',
        'table_name': 'orders_dummy'
    })

    mysql_writer = MySQLWriter({
        'user': 'mysql',
        'dbname': 'orders',
        'password': 'mypassword',
        'table_name': 'orders_transformed'
    })

    # --------------
    #  FILE FORMATS  
    # --------------

    csv_reader = CSVReader({
        'filepath_or_buffer': 'dummy_data.csv',
        'delimiter': ','
    })

    csv_writer = CSVWriter({
        'path_or_buf': 'transformed.csv',
        'index': False
    })

    parquet_reader = ParquetReader({
        'path': 'dummy_data.parquet'
    })

    parquet_writer = ParquetWriter({
        'path': 'transformed.parquet',
        'partition_cols': ['order_year']
    })

    json_reader = JSONReader({
        'path_or_buf': 'dummy_data.json',
        'orient': 'records',
        'lines': True
    })

    json_writer = JSONWriter({
        'path_or_buf': 'transformed.json',
        'orient': 'records',
        'lines': True
    })

    avro_reader = AvroReader({
        'path': 'dummy_data.avro'
    })

    avro_writer = AvroWriter({
        'path': 'transformed.avro',
        'schema': {
            'doc': 'Orders transformed data',
            'name': 'Orders transformed',
            'namespace': 'orders',
            'type': 'record',
            'fields': [
                {'name': 'order_year', 'type': 'int'},
                {'name': 'order_month', 'type': 'string'},
                {'name': 'amount', 'type': 'double'}
            ]
        }
    })

    etl1_writer = ETL(
        reader=parquet_reader,
        writer=mysql_writer,
        transform_fn=transform
    ).execute()

    def filter(df: pd.DataFrame) -> pd.DataFrame:
        return df[df['order_year'] == 2020]

    mysql_reader = MySQLReader(etl1_writer['config'])

    etl2_writer = ETL(
        reader=mysql_reader,
        writer=csv_writer,
        transform_fn=filter
    ).execute()


if __name__ == "__main__":
    main()
