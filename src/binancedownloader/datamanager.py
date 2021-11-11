import math
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
from binance.client import Client
from dateutil.parser import parse
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

from src.binancedownloader.config import Config

HOURLY = 'hourly'
MINUTELY = 'minutely'
NUMBER_OF_TICKERS = 28800

OLDEST_DATE_STR = '1 Jan 2020'
OLDEST_DATE = datetime.strptime(OLDEST_DATE_STR, '%d %b %Y')
DATE_MINUTE_FORMAT = "%d %b %Y %H:%M:%S"
bin_sizes = {"1m": 1, "5m": 5, "1h": 60, "1d": 1440}
freqs = {'hourly': 'h', 'minutely': 'min'}
seconds = {'hourly': 3600, 'minutely': 60}


class DataManager:
    def __init__(self, config: Config):
        self.config = config
        self.binance_client = Client(
            config.BINANCE_API_KEY,
            config.BINANCE_API_SECRET_KEY)
        self.url = self.config.db_url
        self.engine = create_engine(self.url)

    def minutes_of_new_data(self, symbol, kline_size, data):
        if len(data) > 0:
            old = parse(data["date"].iloc[-1])
        else:
            old = OLDEST_DATE
        new = pd.to_datetime(self.binance_client.get_klines(symbol=symbol, interval=kline_size)[-1][0], unit='ms')
        return old, new

    def get_batch_of_tickers_in_csv(self, symbol, kline_size, start_date, minutes):
        end_date = start_date + timedelta(minutes=minutes)
        if end_date > datetime.now():
            end_date = datetime.now()
        klines = self.binance_client.get_historical_klines(symbol,
                                                           kline_size,
                                                           start_date.strftime(DATE_MINUTE_FORMAT),
                                                           end_date.strftime(DATE_MINUTE_FORMAT))
        if len(klines) > 0:
            data = pd.DataFrame(klines,
                                columns=['date',
                                         'open',
                                         'high',
                                         'low',
                                         'close',
                                         'volume',
                                         'close_time',
                                         'quote_av',
                                         'trades',
                                         'tb_base_av',
                                         'tb_quote_av',
                                         'ignore'])
            data['date'] = pd.to_datetime(data['date'], unit='ms')
            data['open'] = pd.to_numeric(data['open'])
            data.drop(['close_time',
                       'quote_av',
                       'trades',
                       'tb_base_av',
                       'tb_quote_av',
                       'ignore'], axis='columns', inplace=True)
            data.loc[:, 'currency_code'] = symbol

            return data, end_date
        else:
            return None, end_date

    def get_all_binance_in_csv(self, symbol, kline_size, save=False):
        filename = f'{symbol}-{kline_size}-data.csv'
        filename = self.config.data_path / filename
        if os.path.isfile(filename):
            data_df = pd.read_csv(filename)
        else:
            data_df = pd.DataFrame()
        oldest_point, newest_point = self.minutes_of_new_data(symbol, kline_size, data_df)
        delta_min = (newest_point - oldest_point).total_seconds() / 60
        available_data = math.ceil(delta_min / bin_sizes[kline_size])
        if oldest_point == OLDEST_DATE:
            print(f'Downloading all available {kline_size} data for {symbol}. Be patient..!')
        else:
            print(
                f'Downloading {delta_min} minutes of new data available for {symbol}, '
                f'i.e. {available_data} instances of {kline_size} data.')
        while oldest_point < newest_point:
            minutes = NUMBER_OF_TICKERS * bin_sizes[kline_size]
            end_date = min(oldest_point + timedelta(minutes=minutes), datetime.now())
            print(f'Downloading ({minutes} minutes) from {oldest_point} till {end_date.strftime(DATE_MINUTE_FORMAT)}')
            data, oldest_point = self.get_batch_of_tickers_in_csv(symbol, kline_size, oldest_point, minutes)
            if data is not None:
                data_df = data_df.append(data)

        data_df.set_index('date', inplace=True)
        if save:
            Path(self.config.data_path).mkdir(parents=True, exist_ok=True)
            data_df.to_csv(filename)
        print('All caught up..!')
        return data_df

    def load_from_db_for(self, table, columns, symbol, begin, end):
        query = f'''
              select {columns} from {table} where currency_code = '{symbol}' and date >= '{begin}' and date <= '{end}'; 
              '''
        tmp_df = pd.read_sql_query(query, self.engine, parse_dates=['date'], coerce_float=True)
        if tmp_df.shape[0] == (parse(end) - parse(begin)).total_seconds() / seconds[table]:
            return tmp_df
        else:
            idx = pd.date_range(begin, end, freq=freqs[table])
            tmp_df = tmp_df.set_index('date').reindex(idx, fill_value=None).ffill()
            return tmp_df

    def load_from_db_for_all(self, table, columns, symbols, begin, end):
        symbols = ", ".join([f"'{symbol}'" for symbol in symbols])
        query = f'''
             select {columns} from {table} where currency_code in ({symbols}) and date >= '{begin}' and date <= '{end}'; 
             '''
        tmp_df = pd.read_sql_query(query, self.engine, parse_dates=['date'], coerce_float=True,
                                   index_col='currency_code')

        idx = pd.date_range(begin, end, freq=freqs[table])
        return tmp_df \
            .set_index('date', append=True) \
            .unstack('currency_code') \
            .reindex(idx, fill_value=None) \
            .ffill() \
            .stack() \
            .reset_index() \
            .rename({'level_0': 'date'}, axis=1) \
            .set_index(['date', 'currency_code']) \
            .unstack('currency_code')

    def get_last_time_in_db_for(self, symbol, table):
        query = f'''
        SELECT (date) FROM {table} WHERE currency_code = '{symbol}' ORDER BY date DESC limit 1;
        '''
        with self.engine.connect() as con:
            rs = con.execute(query)
        last_time_in_db = rs.first()
        return last_time_in_db

    def create_table(self, tabel):
        sql_statement = f"""
        create table if not exists {tabel}
            (
                currency_code text      not null,
                date          timestamp not null,
                open          double precision,
                high          double precision,
                low           double precision,
                close         double precision,
                volume        double precision,
                constraint {tabel}_pkey
                    primary key (currency_code, date)
            );
        """
        try:
            conn = psycopg2.connect(self.config.db_url)
            c = conn.cursor()
            c.execute(sql_statement)
            c.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def update_all_symbols(self):
        self.create_table(MINUTELY)
        self.create_table(HOURLY)
        files = self.config.data_path.glob("*.csv")
        files = [file for file in files for coin in self.config.SUPPORTED_COIN_LIST if coin in file.name]
        for csv_filename in files:
            table = MINUTELY if '1m' in csv_filename.name else HOURLY
            symbol = csv_filename.name.split('-')[0]
            df = pd.read_csv(csv_filename, parse_dates=[0])
            print(f'updating {symbol} for {table} table')
            self.update_database(df, symbol, table)

    def update_database(self, df, symbol, table):
        df['currency_code'] = symbol
        try:
            print(f'{symbol} with {df.shape[0]} number of tickers')
            latest_time_in_db = self.get_last_time_in_db_for(symbol, table)
            if latest_time_in_db:
                latest_time_in_db = latest_time_in_db[0].isoformat()
            else:
                latest_time_in_db = OLDEST_DATE_STR
            df.drop_duplicates(subset=['currency_code', 'date'], keep='last') \
                .set_index(['currency_code', 'date']) \
                .query(f'date > "{latest_time_in_db}"') \
                .to_sql(table, self.engine, if_exists='append', index=['currency_code', 'date'])
        except IntegrityError as e:
            print('not possible', e)

    # not used consider removing it TODO (remove)
    def _force_update_table(self, df, table):
        df.drop_duplicates(subset=['currency_code', 'date'], keep='last') \
            .set_index(['currency_code', 'date']) \
            .to_sql('temp_prices', self.engine, if_exists='replace', index=['currency_code', 'date'])
        query = f'''
         update {table} as o
         set
             open = t.open,
             high = t.high,
             low = t.low,
             close = t.close,
             volume = t.volume
         from temp_prices as t
         where o.currency_code = t.currency_code and o.date = t.date;
         '''
        with self.engine.connect() as con:
            con.execute(query)
