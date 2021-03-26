import os
import datetime
import akshare as ak
import backtrader as bt
from backtrader.analyzers import DrawDown, SharpeRatio
from backtrader.indicators import SMA, SimpleMovingAverage, EMA, ExponentialMovingAverage
from backtrader.sizers import FixedReverser, FixedSize, AllInSizerInt
from backtrader.feeds import GenericCSVData

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler("log.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)

logger.addHandler(handler)
logger.addHandler(console)


class AKData(GenericCSVData):
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


class TestSignal(bt.Indicator):
    lines = ('buy_sig', 'sell_sig', )
    params = (
        ('maperiod', 30),
    )

    def __init__(self):
        sma_ind = SMA(self.data.close, period=self.p.maperiod, plot=True)
        ema_ind = EMA(self.data.close, period=self.p.maperiod, plot=True)

        close_over_sma = self.data.close > sma_ind
        # close_over_ema = self.data.close > ema_ind

        close_under_sma = self.data.close < sma_ind
        # close_under_ema = self.data.close < ema_ind

        sma_over_ema = sma_ind > ema_ind
        sma_under_ema = sma_ind < ema_ind

        self.l.buy_sig = bt.And(close_over_sma, sma_over_ema)
        self.l.sell_sig = bt.And(close_under_sma, sma_under_ema)


class TestStrategy(bt.Strategy):
    def __init__(self):
        self.buyprice = None
        self.buycomm = None
        self.sellprice = None
        self.sellcomm = None

        test_sig = TestSignal()
        self.buy_sig = test_sig.buy_sig
        self.sell_sig = test_sig.sell_sig
        
        self.order = None
        self.bar_executed = 0

    def next(self):
        self.logdebug('VALUE %.3f, CASH %.3f' % (self.broker.get_value(), self.broker.get_cash()))
        if self.order == None:
            self.logdebug('POSITION SIZE - %d' % self.position.size)
            if self.position.size <= 0:
                if self.buy_sig:
                    self.loginfo('B SIG - PRICE %.3f' % self.data.close[0])
                    self.order = self.buy(exectype=bt.Order.Close)
            else:
                if self.sell_sig:
                    self.loginfo('S SIG - PRICE %.3f' % self.data.close[0])
                    self.order = self.sell(exectype=bt.Order.Close)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        elif order.status in [order.Completed]:
            if order.isbuy():
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
                self.loginfo('BO PLACED - P %.3f' % order.created.price)
                self.loginfo('BO EXECUTED - P %.3f, S %d, C %.3f, COMM %.3f'
                             % (order.executed.price, order.executed.size, order.executed.value, order.executed.comm))
            elif order.issell():
                self.sellprice = order.executed.price
                self.sellcomm = order.executed.comm
                self.loginfo('SO PLACED - P %.3f' % order.created.price)
                self.loginfo('SO EXECUTED - P %.3f, S %.3f, C %.3f, COMM %.3f'
                             % (order.executed.price, order.executed.size, order.executed.value, order.executed.comm))
            
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.logwarning('Order Canceled/Margin/Rejected!')
        else:
            self.logerror('Invalid Order Status Catched!')
        
        self.bar_executed = len(self)
        self.order = None

    def notify_trade(self, trade):
        # self.loginfo('TRADE %.3f, %.3f' % (trade.status, trade.value))
        if trade.isclosed:
            self.loginfo('OPER PF - GP %.3f, NP %.3f' % (trade.pnl, trade.pnlcomm))

    def logdebug(self, txt, dt=None):
        dt = dt or self.data.datetime.date(0)
        logger.debug('[%s] %s' % (dt.isoformat(), txt))

    def loginfo(self, txt, dt=None):
        dt = dt or self.data.datetime.date(0)
        logger.info('[%s] %s' % (dt.isoformat(), txt))

    def logwarning(self, txt, dt=None):
        dt = dt or self.data.datetime.date(0)
        logger.warning('[%s] %s' % (dt.isoformat(), txt))

    def logerror(self, txt, dt=None):
        dt = dt or self.data.datetime.date(0)
        logger.error('[%s] %s' % (dt.isoformat(), txt))


def fetch_fund_em_open_fund_info(code='163412', path='./data/bs'):
    file = '%s/%s.csv' % (path, code)
    if os.path.exists(file):
        logger.info('%s already exists' % file)
        return

    df = ak.fund_em_open_fund_info(code, '累计净值走势')
    df.columns = ['date_time', 'accumulated_value']
    df.to_csv(file, encoding='utf_8')


# if __name__ == '__main__':
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

data = AKData(dataname=os.path.join(path, '%s.csv' % code),
                fromdate=datetime.datetime(2005, 1, 1))
cerebro.adddata(data)

cerebro.addstrategy(TestStrategy)
# cerebro.add_signal(bt.SIGNAL_LONGSHORT, TestSignal, maperiod=15)

cerebro.broker.setcash(10000.0)
cerebro.broker.setcommission(0.001)
# cerebro.broker.fundmode = True

# cerebro.addsizer(FixedSize, stake=1000)
cerebro.addsizer(AllInSizerInt, percents=95)

cerebro.addanalyzer(SharpeRatio, _name='SharpeRatio')
cerebro.addanalyzer(DrawDown, _name='DrawDown')

logger.info('INIT - CASH %.3f' % cerebro.broker.getvalue())
results = cerebro.run()
strat = results[0]
logger.info('SR: %s' % strat.analyzers.SharpeRatio.get_analysis())
logger.info('DW: %s' % strat.analyzers.DrawDown.get_analysis())

logger.info('END - CASH %.3f' % cerebro.broker.getvalue())
logger.info('RATIO %.3f%%' % ((cerebro.broker.getvalue() - cerebro.broker.cash) / cerebro.broker.cash * 100))
# logger.info('end get_fundvalue %.3f' % cerebro.broker.get_fundvalue())
# logger.info('end get_fundshares %.3f' % cerebro.broker.get_fundshares())

# cerebro.plot(volume=True, voloverlay=True)

# print(type(bt.dataseries.OHLCDateTime))
