from pandas_etl.utils.config_validator import ConfigValidator
from sqlalchemy import create_engine


class MySQLBase:
    def __init__(self, config):
        self.config = config
        self._required_config_keys = ["dbname", "user", "table_name", "password"]
        ConfigValidator().validate_required_keys(
            self.config, self._required_config_keys
        )
        ConfigValidator.set_default_if_not_present(
            self.config,
            {
                "host": "localhost",
                "port": 3306,
            },
        )
        engine_str = (
            f"mysql+mysqldb://{self.config['user']}:{self.config['password']}"
            f"@{self.config['host']}:{self.config['port']}/{self.config['dbname']}"
        )
        self.engine = create_engine(engine_str)
