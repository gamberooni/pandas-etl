from typing import List, Dict
import logging


class ConfigValidator:
    @classmethod
    def validate_required_keys(self, config: Dict, required_keys: List[str]):
        for k in required_keys:
            if k not in config:
                raise Exception(f"Key '{k}' is required but not in config")

    @classmethod
    def set_default_if_not_present(self, config: Dict, kv_pair: Dict):
        for k, v in kv_pair.items():
            if k not in config:
                logging.info(
                    f"Key '{k}' not set in config. Setting value of '{k}' to default value '{v}'"
                )
                config[k] = v
