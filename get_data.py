from src.binancedownloader.config import Config
from src.binancedownloader.datamanager import DataManager

N_COINS = 100
if __name__ == '__main__':

    config = Config()
    data_manager = DataManager(config)

    for coin in config.SUPPORTED_COIN_LIST[:N_COINS]:
        data_manager.get_all_binance_in_csv(coin, '1m', True)

    # data_manager.update_all_symbols()
    #
    # df = data_manager.load_from_db_for_all('minutely', '*', ['DOGEUSDT', 'ATOMUSDT', 'ETCUSDT'], '2020-07-01', '2020-08-01')
    # print(df.head())
