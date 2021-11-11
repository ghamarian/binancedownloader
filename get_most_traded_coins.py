from binance.client import Client
from datetime import datetime
from tqdm.notebook import tqdm

api_key = "xxx"
api_secret = "xxx"

client = Client(api_key, api_secret)
exchange_info = client.get_exchange_info()
symbols = [s['symbol'] for s in exchange_info['symbols']]
usdt_symbols = [s for s in symbols if s.endswith('USDT')]
value = {}
for s in tqdm(usdt_symbols):
    klines = client.get_historical_klines(s, Client.KLINE_INTERVAL_1MONTH, '3 month ago UTC')
    value[s] = sum([float(k[5]) * float(k[4]) for k in klines])

sorted_symbols = dict(sorted(value.items(), key = lambda x: x[1], reverse=True))

for k, v in sorted_symbols.items():
    print(k)