from typing import Dict
import pandas as pd
import csv
from io import StringIO
from pandas_etl.databases.postgres.base import PostgresBase
from pandas_etl.base_writer import WriterBase


class PostgresWriter(WriterBase, PostgresBase):
    def __init__(self, write_config: Dict):
        super().__init__(write_config)

    def __repr__(self) -> str:
        return "PostgresWriter"

    def write(self, df: pd.DataFrame) -> None:
        df.to_sql(
            self.config["table_name"],
            self.engine,
            if_exists="replace",
            index=False,
            method=self._psql_insert_copy,
        )

    def _psql_insert_copy(self, table, conn, keys, data_iter):
        """
        Execute SQL statement inserting data

        Parameters
        ----------
        table : pandas.io.sql.SQLTable
        conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
        keys : list of str
            Column names
        data_iter : Iterable that iterates the values to be inserted
        """
        # gets a DBAPI connection that can provide a cursor
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ", ".join('"{}"'.format(k) for k in keys)
            if table.schema:
                table_name = "{}.{}".format(table.schema, table.name)
            else:
                table_name = table.name

            sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
            cur.copy_expert(sql=sql, file=s_buf)
