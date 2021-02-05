import pandas as pd
from pandas.core.arrays.sparse import dtype
import requests
import requests.exceptions
from requests.api import request
import re
from lxml import etree
import pickle
import os
import json

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.FileHandler("log.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)

logger.addHandler(handler)
logger.addHandler(console)


class FundHistoryItem(object):
    HISTORY_COL_INFO = {
        'net_value_date': '净值日期',
        'unit_net_value': '单位净值',
        'accumulative_net_value': '累计净值',
        'daily_growth_rate': '日增长率',
        'purchasing_state': '申购状态',
        'redeming_state': '赎回状态',
        'dividend_paying_and_placing': '分红送配',
    }

    HISTORY_COL_DTYPE = {
        'net_value_date': str,
        'unit_net_value': str,
        'accumulative_net_value': str,
        'daily_growth_rate': str,
        'purchasing_state': str,
        'redeming_state': str,
        'dividend_paying_and_placing': str,
    }

    def __init__(self, item:dict) -> None:
        super().__init__()
        self.net_value_date = item.get('net_value_date')
        self.unit_net_value = item.get('unit_net_value')
        self.accumulative_net_value = item.get('accumulative_net_value')
        self.daily_growth_rate = item.get('daily_growth_rate')
        self.purchasing_state = item.get('purchasing_state')
        self.redeming_state = item.get('redeming_state')
        self.dividend_paying_and_placing = item.get('dividend_paying_and_placing')
    
    def __str__(self) -> None:
        return '[%s][%s][%s][%s]' % (self.net_value_date, self.unit_net_value, self.accumulative_net_value, self.daily_growth_rate)

    def to_list(self) -> list:
        return [self.net_value_date, self.unit_net_value, self.accumulative_net_value, self.daily_growth_rate, self.purchasing_state, self.redeming_state, self.dividend_paying_and_placing]

    @classmethod
    def col_codes(cls) -> list:
        return list(cls.HISTORY_COL_INFO.keys())

    @classmethod
    def col_names(cls) -> list:
        return list(cls.HISTORY_COL_INFO.values())


class FundHistory(object):
    def __init__(self, code:str) -> None:
        super().__init__()
        self.code = code
        self.items = dict()
    
    def __setitem__(self, key:str, value:FundHistoryItem) -> None:
        if key in self.items.keys():
        # if self.has_key(key):
            logger.warn('[%s] Duplicated history @%s' % (self.code, value.net_value_date))
        else:
            self.items[key] = value
    
    def __getitem__(self, key:str) -> FundHistoryItem:
        return self.items.get(key, None)

    def __contains__(self, key:str) -> bool:
        return key in self.items

    def __iter__(self) -> iter:
        return iter(self.items.items())

    def __len__(self) -> int:
        return len(self.items)

    def to_pd(self) -> pd.DataFrame:
        data = {'%s' % dt:his.to_list() for dt, his in self}
        df = pd.DataFrame.from_dict(data, orient='index', columns=FundHistoryItem.HISTORY_COL_INFO.keys())
        return df
    
    def save_history(self, file, type='csv') -> None:
        df = self.to_pd()
        if type == 'csv':
            df.to_csv(file, encoding='utf_8_sig')
        else:
            logger.info('export type [%s] not supported' % type)


class Fund(object):
    FUND_COL_INFO = {
        'code': '编码',
        'py_brief_code': '拼音缩略编码',
        'cn_full_name': '名称',
        'type': '类型',
        'py_full_code': '拼音全称编码',
    }
    
    FUND_COL_DTYPE = {
        'code': str,
        'py_brief_code': str,
        'cn_full_name': str,
        'type': str,
        'py_full_code': str,
    }
    
    def __init__(self, info:dict) -> None:
        super().__init__()
        self.code = info.get('code')
        self.py_brief_code = info.get('py_brief_code')
        self.cn_full_name = info.get('cn_full_name')
        self.type = info.get('type')
        self.py_full_code = info.get('py_full_code')
        self.history = FundHistory(self.code)
    
    @classmethod
    def col_codes(cls) -> list:
        return list(cls.FUND_COL_INFO.keys())

    @classmethod
    def col_names(cls) -> list:
        return list(cls.FUND_COL_INFO.values())

    def __str__(self) -> str:
        return '[%s][%s][%s]' % (self.code, self.cn_full_name, self.type)
    
    def add_history(self, item:FundHistoryItem) -> None:
        self.history[item.net_value_date] = item
    
    def add_histories(self, items:list) -> None:
        for item in items:
            self.add_history(item)

    def to_list(self):
        return [self.code, self.py_brief_code, self.cn_full_name, self.type, self.py_full_code]
    
    def is_typeof(self, type):
        return self.type == type
    
    # def export_history(self, path, type='csv') -> None:
    #     file = os.path.join(path, self.code)
    #     df = self.to_pd()
    #     if type == 'csv':
    #         df.to_csv(file, encoding='utf_8_sig')
    #     else:
    #         logger.info('export type [%s] not supported' % type)



class FundManager(object):
    # 所有基金代码、基金名称简称的集合
    URL_FUNDS_INFO = 'http://fund.eastmoney.com/js/fundcode_search.js'

    # 基金历史净值, per最多40
    URL_FUND_HISTORY = 'http://fund.eastmoney.com/f10/F10DataApi.aspx'

    # 基金净值，公司
    URL_FUND_COMPANY = 'http://fund.eastmoney.com/js/jjjz_gs.js'    # ?dt=1463791574015


    # jzrq 截止日期，dwjz 单位净值，gsz 收益率，gszzl，
    URL_FUND_NET_VALUE = 'http://fundgz.1234567.com.cn/js/001186.js'    # ?rt=1463558676006

    def __init__(self) -> None:
        super().__init__()
        self.funds = dict()
    
    def clear(self) -> None:
        del(self.funds)
        self.funds = dict()
    
    def __len__(self) -> int:
        return len(self.funds)

    def __iter__(self) -> iter:
        return iter(self.funds.items())
    
    def __contains__(self, key:str) -> bool:
        return key in self.funds
    
    def __getitem__(self, key:str) -> Fund:
        return self.funds.get(key, None)
    
    def __setitem__(self, key:str, value:Fund) -> None:
        self.funds[key] = value

    # def dump(self, file:str) -> None:
    #     with open(file, 'wb') as f:
    #         pickle.dump(self.funds, f)
    
    # def load(self, file:str) -> None:
    #     with open(file, 'rb') as f:
    #         self.funds = pickle.load(f)

    def init_funds_info(self, file:str, force_reload:bool=False) -> None:
        self.clear()
        if not os.path.exists(file) or force_reload:
            resp_text = self._crawl_funds_info()
            self.funds = self._parse_funds_info(resp_text)
            self.save_info(file, type='csv')
        else:
            self.load_info(file)
    
    def _crawl_funds_info(self) -> str:
        url = FundManager.URL_FUNDS_INFO
        headers = {
            'Content-Type': 'application/json;charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:22.0) Gecko/20100101 Firefox/22.0',
        }

        resp = None
        try:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
        except requests.HTTPError as err:
            logger.error('requests HTTP error %s' % err)

        return resp.text

    def _parse_funds_info(self, resp_text) -> dict:
        # 匹配每个[]内的
        pattern = re.compile(r'\[\"\d+\",\"\w+\",\"[\u4E00-\u9FA5\(\)\w]+\",\"[\u4E00-\u9FA5\(\)\w]+\",\"\w+\"\]')
        groups = pattern.findall(resp_text)
        
        fund_dict = dict()
        for group in groups:
            fund = Fund(dict(zip(Fund.col_codes(), eval(group))))
            fund_dict[fund.code] = fund
        
        return fund_dict
    
    def get_fund_history(self, fund_code:str, start_date:str='2020-01-01', end_date:str='2020-12-31') -> None:
        cur_page = 1
        page_limit = 1

        while cur_page <= page_limit:
            resp_text = self._crawl_fund_history_items(fund_code, start_date=start_date, end_date=end_date, page=cur_page)
            (page_limit, history_items) = self._parse_fund_history_items(resp_text)
            self.funds[fund_code].add_histories(history_items)
            cur_page += 1

    def _crawl_fund_history_items(self, fund_code:str, start_date:str, end_date:str, page:int) -> str:
        BIZ_TYPE = 'lsjz' # 历史净值
        HISTORY_ITEMS_PER_PAGE = 40

        url = FundManager.URL_FUND_HISTORY
        headers = {
            'Content-Type': 'application/json;charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:22.0) Gecko/20100101 Firefox/22.0',
        }

        payload = {
            'type': BIZ_TYPE,
            'code': fund_code,
            'page': page,
            'sdate': start_date,
            'edate': end_date,
            'per': HISTORY_ITEMS_PER_PAGE,
        }

        resp = None
        try:
            resp = requests.get(url, headers=headers, params=payload)
            resp.raise_for_status()
        except requests.HTTPError as err:
            logger.error('requests HTTP error %s' % err)

        return resp.text
    
    def _parse_fund_history_items(self, resp_text) -> (int, list):
        page_limit = self._parse_page_limit(resp_text)
        if page_limit == 0:
            history_items = []
        else:
            history_items = self._parse_history_items(resp_text)

        return page_limit, history_items
    
    def _parse_page_limit(self, resp_text) -> int:
        pattern = re.compile(r'pages:(\d+),')
        page_limit = pattern.search(resp_text).group(1)
        return int(page_limit)
    
    def _parse_history_items(self, resp_text) -> list:
        pattern = re.compile(r'\"(.*)\"')
        table_content = pattern.search(resp_text).group(1)
        # logger.debug(table_content)

        table_html = etree.HTML(table_content)
        results = []
        trs = table_html.xpath('//table/tbody/tr')
        for tr in trs:
            tds = tr.xpath('.//td')
            td_dt = {
                'net_value_date': tds[0].text,
                'unit_net_value': tds[1].text,
                'accumulative_net_value': tds[2].text,
                'daily_growth_rate': tds[3].text,
                'purchasing_state': tds[4].text,
                'redeming_state': tds[5].text,
                'dividend_paying_and_placing': tds[6].text,
            }
            item = FundHistoryItem(td_dt)
            results.append(item)
        return results
    
    def load_info(self, file:str, type:str='csv') -> None:
        if type == 'csv':
            df = pd.read_csv(file, index_col=0, encoding='utf_8_sig', dtype=Fund.FUND_COL_DTYPE)
            self.clear()
            records = df.to_dict(orient='records')

            for rec in records:
                code = rec['code']
                self.funds[code] = Fund(rec)
        else:
            logger.info('export type [%s] not supported' % type)
    
    def to_pd(self) -> pd.DataFrame:
        dt_data = {'%s' % f.code:f.to_list() for c, f in self}
        # print(dt_data)
        df = pd.DataFrame.from_dict(dt_data, orient='index', columns=Fund.FUND_COL_INFO.keys())
        df = pd.DataFrame.from_records()
        return df

    def save_info(self, file:str, type:str='csv') -> None:
        df = self.to_pd()
        if type == 'csv':
            df.to_csv(file, encoding='utf_8_sig')
        else:
            logger.info('export type [%s] not supported' % type)
    
    def save_history(self, code:str, file:str, type:str='csv') -> None:
        history = self.funds[code].history
        history.save_history(file)


if __name__ == '__main__':
    local_file = os.path.join('.', 'data', 'funds.csv')

    mgr = FundManager()
    mgr.init_funds_info(local_file, force_reload=False)

    start_code = '006377'
    start_date = '2019-01-01'
    end_date = '2019-12-31'
    for code, fund in mgr:
        if code >= start_code and fund.is_typeof('混合型'):
            logger.debug(code)
            mgr.get_fund_history(code, start_date=start_date, end_date=end_date)

            fund_file = os.path.join('.', 'data', '[%s][%s][%s]_%s_%s.csv' % (code, fund.type, fund.cn_full_name, start_date, end_date))
            mgr.save_history(code, fund_file, type='csv')
