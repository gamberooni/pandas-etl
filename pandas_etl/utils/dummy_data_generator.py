from datetime import date
from typing import Dict
from faker import Faker
from faker.providers import DynamicProvider
import pandas as pd
from fastavro import writer, parse_schema
import logging


class OrdersDummyDataGenerator:
    """Class for generating fake orders data in multiple file formats."""

    def __init__(self):
        self.fake = Faker()
        self.fake.add_provider(
            DynamicProvider(
                provider_name="logistic_partner",
                elements=["Lalamove", "DHL", "FedEx", "NinjaVan", "PosLaju"],
            )
        )
        self.fake.add_provider(
            DynamicProvider(
                provider_name="nike_product",
                elements=[
                    "Nike Golf",
                    "Nike Pro",
                    "Nike+",
                    "Air Jordan",
                    "Nike Blazers",
                    "Air Force 1",
                    "Nike Dunk",
                    "Air Max",
                    "Nike CR7",
                ],
            )
        )

    def calculate_amount(self) -> float:
        return round(self.quantity * self.unit_price, 2)

    def generate_dummy_data(self) -> Dict:
        """Generate one row of dummy data."""

        self.customer_email = self.fake.ascii_email()
        self.shipping_address = f"{self.fake.street_address()}, {self.fake.city()}, {self.fake.postcode()}, {self.fake.country()}"  # noqa
        self.logistic_partner = self.fake.logistic_partner()
        self.contact_number = self.fake.msisdn()
        self.order_date = self.fake.date_between(start_date=date(2020, 1, 1)).strftime(
            "%Y-%m-%d"
        )
        self.product = self.fake.nike_product()
        self.quantity = self.fake.pyint(min_value=1, max_value=50)
        self.unit_price = self.fake.pyfloat(
            right_digits=2, positive=True, min_value=5, max_value=500
        )
        self.amount = self.calculate_amount()
        self.currency = "USD"
        return self.__dict__.items()

    def generate_dummy_data_df(self, rows: int) -> pd.DataFrame:
        """Generate N number of rows of dummy data and return them in a pandas dataframe."""

        data_dict = [
            {k: v for k, v in self.generate_dummy_data() if k != "fake"}
            for _ in range(rows)
        ]
        df = pd.DataFrame.from_dict(data_dict)
        return df

    def generate_csv(self, filename: str, df: pd.DataFrame) -> None:
        df.to_csv(filename, index=False)
        logging.info(f"generated csv file '{filename}' with dummy data")

    def generate_parquet(self, filename: str, df: pd.DataFrame) -> None:
        df.to_parquet(filename, compression="snappy")
        logging.info(f"generated parquet file '{filename}' with dummy data")

    def generate_json(self, filename: str, df: pd.DataFrame) -> None:
        df.to_json(filename, orient="records", lines=True)
        logging.info(f"generated json file '{filename}' with dummy data")

    def generate_avro(self, filename: str, df: pd.DataFrame) -> None:
        schema = {
            "doc": "Orders dummy data",
            "name": "Orders",
            "namespace": "orders",
            "type": "record",
            "fields": [
                {"name": "customer_email", "type": "string"},
                {"name": "shipping_address", "type": "string"},
                {"name": "logistic_partner", "type": "string"},
                {"name": "contact_number", "type": "string"},
                {"name": "order_date", "type": "string"},
                {"name": "product", "type": "string"},
                {"name": "quantity", "type": "int"},
                {"name": "unit_price", "type": "double"},
                {"name": "amount", "type": "double"},
                {"name": "currency", "type": "string"},
            ],
        }
        parsed_schema = parse_schema(schema)
        records = df.to_dict("records")
        with open(filename, "wb") as f:
            writer(f, parsed_schema, records)
        logging.info(f"generated avro file '{filename}' with dummy data")


def main():
    generator = OrdersDummyDataGenerator()
    df = generator.generate_dummy_data_df(1000)
    generator.generate_csv("dummy_data.csv", df)
    generator.generate_parquet("dummy_data.parquet", df)
    generator.generate_json("dummy_data.json", df)
    generator.generate_avro("dummy_data.avro", df)


if __name__ == "__main__":
    main()
