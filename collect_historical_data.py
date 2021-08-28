import pandas as pd
import math
import os.path
from binance.client import Client
from datetime import timedelta, datetime
from dateutil import parser
import ssl
import csv

ssl._create_default_https_context = ssl._create_unverified_context



### CONSTANTS
binsizes = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}
batch_size = 750
binance_client = Client()

symbol = input("What symbol do you want data from? (BTCUSDT, BNBBTC...)  ")
timeline = input("What timeline? (1m, 5m, 15m, 1h, 4h OR 1d)  ")



if os.path.isfile(symbol + '-'+ timeline + '-data.csv'):
    print('CSV file already exists, appending new data since it\'s not up to date')
    with open(symbol + '-'+ timeline + '-data.csv', newline='') as csvfile:
        Reader=csv.reader(csvfile)
        Rows=list(Reader)
        Tot_rows=len(Rows)
        text = Rows[(Tot_rows)-1]
        lastdate = text[00]
        if len(lastdate) < 11:
            lastdate=lastdate + ' 00:00:00'
        oldest_point = datetime.strptime(lastdate, '%Y-%m-%d %H:%M:%S')
else:
    print('file does not exist, making new one and creating new date')
    lastdate = '2000-01-01 00:00:00'
    oldest_point = datetime.strptime(lastdate, '%Y-%m-%d %H:%M:%S')


def minutes_of_new_data(symbol, kline_size, data, source):
    if len(data) > 0:  old = parser.parse(data["timestamp"].iloc[-1])
    elif source == "binance": old = datetime.strptime(lastdate, '%Y-%m-%d %H:%M:%S')
    if source == "binance": new = pd.to_datetime(binance_client.get_klines(symbol='BTCUSDT', interval=kline_size)[-1][0], unit='ms')
    return old, new

def get_all_binance(symbol, kline_size, from = 0, save = False):
    filename = '%s-%s-data.csv' % (symbol, kline_size)
    if os.path.isfile(filename): data_df = pd.read_csv(filename)
    else: data_df = pd.DataFrame()
    oldest_point, newest_point = minutes_of_new_data(symbol, kline_size, data_df, source = "binance")
    if from != 0:
        oldest_point = from
    delta_min = (newest_point - oldest_point).total_seconds()/60
    available_data = math.ceil(delta_min/binsizes[kline_size])
    if oldest_point == datetime.strptime(lastdate, '%Y-%m-%d %H:%M:%S'): print('Downloading all available %s data for %s.' % (kline_size, symbol))
    else: print('Downloading %d minutes of new data available for %s, i.e. %d instances of %s data.' % (delta_min, symbol, available_data, kline_size))
    klines = binance_client.get_historical_klines(symbol, kline_size, oldest_point.strftime("%Y-%m-%d %H:%M:%S"), newest_point.strftime("%d %b %Y %H:%M:%S"))
    data = pd.DataFrame(klines, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ])
    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
    if len(data_df) > 0:
        temp_df = pd.DataFrame(data)
        data_df = data_df.append(temp_df)
    else: data_df = data
    data_df.set_index('timestamp', inplace=True)
    if save: data_df.to_csv(filename)
    print('Done')
    return data_df


get_all_binance(symbol, timeline, save = True)
