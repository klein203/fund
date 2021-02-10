from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from datetime import datetime
import time
import backtrader as bt
import pandas as pd
import os
import sys
import tushare as ts

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.FileHandler("log.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)

logger.addHandler(handler)
logger.addHandler(console)

# sys.path = ['libs/backtrader'] + sys.path


def get_data(code: str, start: str = '2010-01-01', end: str = '2020-03-31') -> pd.DataFrame:
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

    def log(self, txt: str, dt: datetime = None) -> None:
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

def test_backtrader():
    file_600000 = os.path.join('.', 'data', 'custom', '[600000][浦发银行][股票]_2010-01-01_2020-03-31.csv')
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

class TushareProxy(object):
    # INIT_DAY = '1990-12-10'
    INIT_DAY_HYPHEN = '2000-01-01'
    TODAY_HYPHEN = time.strftime('%Y-%m-%d', time.localtime())
    INIT_DAY = '20000101'
    TODAY = time.strftime('%Y%m%d', time.localtime())

    def __init__(self, path) -> None:
        super().__init__()
        self._token = '0a0e3b1fb5f51d01c2ef1b1d654cb4e0685339fb02c5e135aa1b39b2'
        ts.set_token(self._token)
        self.path = path
        self._handler_v1 = ts
        self._handler_v2 = ts.pro_api()

    def _save_file(self, df, path, file, type='csv'):
        if type == 'csv':
            df.to_csv(os.path.join(path, file), encoding='utf_8_sig')

    def fetch_stock_basics(self, saved=True):
        # 名称	类型	必选	描述
        # ts_code	str	N	股票代码
        # list_status	str	N	上市状态： L上市 D退市 P暂停上市，默认L
        # exchange	str	N	交易所 SSE上交所 SZSE深交所 HKEX港交所(未上线)
        # is_hs	str	N	是否沪深港通标的，N否 H沪股通 S深股通
        stock_basics = self._handler_v2.stock_basic(
            exchange='', list_status='L', fields='ts_code, symbol, name, area, industry, list_date')
        if saved:
            self._save_file(stock_basics, self.path, 'stock_basics.csv')
        return stock_basics

    # def fetch_k_data(self, code, saved=True):

    # df = ts.get_k_data('000001', autype='qfq', start='1990-12-10', end='2021-02-09')
    # df.to_csv('000001.csv', encoding='utf_8_sig')

    def fetch_k_data(self, code, start_date, end_date, saved=True):
        # 名称	类型	必选	描述
        # ts_code	str	Y	证券代码
        # api	str	N	pro版api对象，如果初始化了set_token，此参数可以不需要
        # start_date	str	N	开始日期 (格式：YYYYMMDD，提取分钟数据请用2019-09-01 09:00:00这种格式)
        # end_date	str	N	结束日期 (格式：YYYYMMDD)
        # asset	str	Y	资产类别：E股票 I沪深指数 C数字货币 FT期货 FD基金 O期权 CB可转债（v1.2.39），默认E
        # adj	str	N	复权类型(只针对股票)：None未复权 qfq前复权 hfq后复权 , 默认None
        # freq	str	Y	数据频度 ：支持分钟(min)/日(D)/周(W)/月(M)K线，其中1min表示1分钟（类推1/5/15/30/60分钟） ，默认D。对于分钟数据有600积分用户可以试用（请求2次），正式权限请在QQ群私信群主或积分管理员。
        # ma	list	N	均线，支持任意合理int数值。注：均线是动态计算，要设置一定时间范围才能获得相应的均线，比如5日均线，开始和结束日期参数跨度必须要超过5日。目前只支持单一个股票提取均线，即需要输入ts_code参数。
        # factors	list	N	股票因子（asset='E'有效）支持 tor换手率 vr量比
        # adjfactor	str	N	复权因子，在复权数据时，如果此参数为True，返回的数据中则带复权因子，默认为False。 该功能从1.2.33版本开始生效
        df = self._handler_v1.pro_bar(ts_code=code, asset='E', adj='qfq', start_date=start_date, end_date=end_date, ma=[5, 20, 50])
        if saved:
            self._save_file(df, self.path, "%s.csv" % code)
            logger.info("file %s.csv (%s - %s) saved" % (code, start_date, end_date))

    def batch_fetch_k_data(self, start_date, end_date, saved=True,k)

if __name__ == '__main__':
    proxy = TushareProxy(os.path.join('.', 'data', 'tushare'))
    # logger.info(proxy.TODAY)
    stock_basics = proxy.fetch_stock_basics(saved=False)
    for ts_code in stock_basics['ts_code'].values[0:10]:
        proxy.fetch_k_data(ts_code, TushareProxy.INIT_DAY, TushareProxy.TODAY)
        

