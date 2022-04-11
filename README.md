# etl-lib

Provides abstraction for pandas-based ETL. Intended for small local dataset usages.

## Data Sources

1. File Formats

- avro
- csv
- json
- parquet

2. Databases

- sqlite3
- mysql
- postgres

## Data Transformation

Allows user to define their own transformation function(s) to be parsed into the ETL operation.

## Data Loading

Same as data sources

## Example Usage

1. Data source: csv file
2. Transformation: orders amount aggregation
3. Staging database: sqlite3
4. Transformation: filter results for a specific order year
5. Destination database: postgres

```python
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

csv_reader = CSVReader({
    'filepath_or_buffer': 'dummy_data.csv',
})

sqlite3_writer = SQLite3Writer({
    'path': 'transformed.db',
    'table_name': 'dummy_table'
})

etl1_writer = ETL(
    reader=parquet_reader,
    writer=mysql_writer,
    transform_fn=transform
).execute()

def filter(df: pd.DataFrame) -> pd.DataFrame:
    return df[df['order_year'] == 2020]

sqlite3_reader = SQLite3Reader(etl1_writer['config'])

postgres_writer = PostgresWriter({
    'user': 'postgres',
    'dbname': 'orders',
    'password': 'mypassword',
    'table_name': 'orders_filtered'
})

etl2_writer = ETL(
    reader=sqlite3_reader,
    writer=postgres_writer,
    transform_fn=filter
).execute()
```
