from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from datetime import datetime
import backtrader as bt
import pandas as pd
import os
import sys

# sys.path = ['libs/backtrader'] + sys.path

def get_data(code:str, start:str='2010-01-01', end:str='2020-03-31') -> pd.DataFrame:
    import tushare as ts
    df = ts.get_k_data(code, autype='qfq', start=start, end=end)
    df.index = pd.to_datetime(df.date)
    df['openinterest'] = 0
    df = df[['open', 'high', 'low', 'close', 'volume', 'openinterest']]
    return df


class TestStrategy(bt.Strategy):

    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close

    def next(self):
        # Simply log the closing price of the series from the reference
        self.log('Close, %.2f' % self.dataclose[0])

        if self.dataclose[0] < self.dataclose[-1]:
            # current close less than previous close

            if self.dataclose[-1] < self.dataclose[-2]:
                # previous close less than the previous close

                # BUY, BUY, BUY!!! (with all possible default parameters)
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                self.buy()


class MyStrategy(bt.Strategy):
    def __init__(self) -> None:
        # super().__init__()
        self.dataclose = self.datas[0].close
    
    def next(self) -> None:
        self.log('Close, %.2f' % self.dataclose[0])
    
    def log(self, txt:str, dt:datetime=None) -> None:
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))
    

if __name__ == '__main__':
    file_600000 = os.path.join('.', 'data', '[600000][浦发银行][股票]_2010-01-01_2020-03-31.csv')
    # df = get_data('600000')
    # df.to_csv(file)

    df = pd.read_csv(file_600000, index_col='date', parse_dates=['date'])

    start_date = datetime(2010, 3, 31)
    end_date = datetime(2020, 3, 31)


    # cerebro
    cerebro = bt.Cerebro()
    
    # strategy
    cerebro.addstrategy(MyStrategy)
    
    # data feed
    data = bt.feeds.PandasData(dataname=df, fromdate=start_date, todate=end_date)
    cerebro.adddata(data)


    cerebro.broker.setcash(100000.0)

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # cerebro.plot(style='candlestick')