import os
import datetime
import akshare as ak
import backtrader as bt

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


class AKData(bt.feeds.GenericCSVData):
    params = (
        ('fromdate', datetime.datetime(2012, 4, 5)),
        ('todate', datetime.datetime(2021, 2, 22)),
        ('nullvalue', 0.0),
        ('dtformat', ('%Y-%m-%d')),

        ('datetime', 1),
        ('time', -1),
        ('open', -1),
        ('high', -1),
        ('low', -1),
        ('close', 2),
        ('volume', -1),
        ('openinterest', -1),
        # ('timeframe', bt.TimeFrame.Weeks)
    )


class TestStrategy(bt.Strategy):
    def __init__(self):
        sma_15d = bt.indicators.SimpleMovingAverage(period=15)
        ema_15d = bt.indicators.ExponentialMovingAverage(period=15)

        close_over_sma_15d = self.data.close > sma_15d
        close_over_ema_15d = self.data.close > ema_15d

        close_under_sma_15d = self.data.close < sma_15d
        close_under_ema_15d = self.data.close < ema_15d

        sma_ema_15d_diff = sma_15d - ema_15d

        self.buy_sig = bt.And(close_over_sma_15d,
                              close_over_ema_15d, sma_ema_15d_diff > 0)
        self.sell_sig = bt.And(close_under_sma_15d,
                               close_under_ema_15d, sma_ema_15d_diff < 0)
        # self.order = None
        super().__init__()
        # for name, value in vars(self).items():
        #     print(name, value)

    def next(self):
        # self.log('Close, %.2f' % self.data.close[0], doprint=True)
        # if self.order:
        #     return

        if self.buy_sig:
            self.log('Close, %.2f' % self.data.close[0], doprint=True)
            # self.order = self.buy()
            self.buy()
            self.log('Buy', doprint=True)
        elif self.sell_sig:
            self.log('Close, %.2f' % self.data.close[0], doprint=True)
            # self.order = self.sell()
            self.sell()
            self.log('Sell', doprint=True)

    def log(self, txt, dt=None, doprint=False):
        if doprint:
            dt = dt or self.datas[0].datetime.date(0)
            logger.info('%s, %s' % (dt.isoformat(), txt))


def fetch_fund_em_open_fund_info(code='163412', path='./data/bs'):
    file = '%s/%s.csv' % (path, code)
    if os.path.exists(file):
        logger.info('%s already exists' % file)
        return

    df = ak.fund_em_open_fund_info(code, '累计净值走势')
    df.columns = ['date_time', 'accumulated_value']
    df.to_csv(file, encoding='utf_8')


if __name__ == '__main__':
    # local_file = os.path.join('.', 'data', 'funds.csv')

    # mgr = FundManager()
    # mgr.init_funds_info(local_file, force_reload=False)

    # start_code = '006377'
    # start_date = '2019-01-01'
    # end_date = '2019-12-31'
    # for code, fund in mgr:
    #     if code >= start_code and fund.is_typeof('混合型'):
    #         logger.debug(code)
    #         mgr.get_fund_history(code, start_date=start_date, end_date=end_date)

    #         fund_file = os.path.join('.', 'data', '[%s][%s][%s]_%s_%s.csv' % (code, fund.type, fund.cn_full_name, start_date, end_date))
    #         mgr.save_history(code, fund_file, type='csv')

    code = '163412'
    path = os.path.join('.', 'data', 'bs')
    fetch_fund_em_open_fund_info()

    cerebro = bt.Cerebro()

    data = AKData(dataname=os.path.join(path, '%s.csv' % code), fromdate=datetime.datetime(2020, 1, 1))
    cerebro.adddata(data)

    cerebro.addstrategy(TestStrategy)

    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(0.005)

    cerebro.addsizer(bt.sizers.FixedReverser, stake=1000)

    logger.info('init %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    logger.info('end %.2f' % cerebro.broker.getvalue())
    # cerebro.plot()

    # print(type(bt.dataseries.OHLCDateTime))
