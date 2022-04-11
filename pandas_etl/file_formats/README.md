# File Format Configs

## Reader Configs

1. avro

- path: path of the avro file

2. csv

Implemented using the `pandas.read_csv()` method. Refer to pandas documentation to see what parameters are supported.

3. json

Implemented using the `pandas.read_json()` method. Refer to pandas documentation to see what parameters are supported.

4. parquet

Implemented using the `pandas.read_parquet()` method. Refer to pandas documentation to see what parameters are supported.

## Writer Configs

1. avro

- path: path of the avro file
- schema: schema of the data to be written into the avro file

2. csv

Implemented using the `pandas.DataFrame.to_csv()` method. Refer to pandas documentation to see what parameters are supported.

3. json

Implemented using the `pandas.DataFrame.to_json()` method. Refer to pandas documentation to see what parameters are supported.

4. parquet

Implemented using the `pandas.DataFrame.to_parquet()` method. Refer to pandas documentation to see what parameters are supported.
