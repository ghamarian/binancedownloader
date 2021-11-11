import pandas as pd
import glob
import os

for fname in glob.glob(os.path.join('../data', '*.csv')):
    df = pd.read_csv(fname)
    print(fname)
    print(len(df))
    df['date'] = pd.to_datetime(df.date)
    ddf = (df
           .drop_duplicates('date')
           .set_index('date')
           .asfreq(freq='1Min')
           .interpolate('linear', axis=0)
           .fillna(method='bfill')
           .reset_index()
           )
    print(len(ddf))
    ddf.to_csv(os.path.join('../data_2', fname.split(os.sep)[-1]), index=False)
