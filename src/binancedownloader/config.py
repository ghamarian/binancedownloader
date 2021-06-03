# Config consts
import configparser
import os
from pathlib import Path

CFG_FL_NAME = "user.cfg"
BINANCE_CFG_SECTION = "binance_user_config"
DB_CFG_SECTION = "database_config"
DATA_CFG_SECTION = "data_config"


class Config:  # pylint: disable=too-few-public-methods
    def __init__(self):
        config = configparser.ConfigParser()
        config_file = Path(__file__).resolve().parents[2] / CFG_FL_NAME

        if not config_file.exists():
            print("No configuration file (user.cfg) found! See README. Assuming default config...")
            config[BINANCE_CFG_SECTION] = {}
        else:
            config.read(config_file)

        self.BINANCE_API_KEY = os.environ.get("API_KEY") or config.get(BINANCE_CFG_SECTION, "api_key")
        self.BINANCE_API_SECRET_KEY = os.environ.get("API_SECRET_KEY") or config.get(BINANCE_CFG_SECTION,
                                                                                     "api_secret_key")

        self.db_url = os.environ.get("DB_URL") or config.get(DB_CFG_SECTION, "db_url")

        self.data_path = Path(config.get(DATA_CFG_SECTION, 'path')) or Path('data')

        supported_coin_list = [
            coin.strip() for coin in os.environ.get("SUPPORTED_COIN_LIST", "").split() if coin.strip()
        ]

        if not supported_coin_list and os.path.exists("supported_coin_list"):
            with open("supported_coin_list") as rfh:
                for line in rfh:
                    line = line.strip()
                    if not line or line.startswith("#") or line in supported_coin_list:
                        continue
                    supported_coin_list.append(line)
        self.SUPPORTED_COIN_LIST = supported_coin_list
