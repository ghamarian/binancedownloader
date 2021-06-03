# Get data from binance #

See [the roadmap of the project][roadmap].

## Install dependencies ##

```bash
pip install -U pip  # The newer versions will download wheel files on Mac
pip install --upgrade -r requirements.txt
```

## Copy `user.cfg.example` to `user.cfg` ##

```bash
cp user.cfg.example user.cfg
```
Adapt the content according to your setting

## Download the data as csv run `get_data.py`##

```bash
python get_data.py
```
Coin list to be downloaded are listed in `supported_coin_list` file. By default it gets the data from Jan. 1st 2020. Changing the granuality of the data can be done by changing the parameters of `get_all_binance_in_csv` in `get_data.py`

## [optionally] Store the csv file in postgresql database ##

You can uncomment the code in `get_data.py` to store them in database as well.