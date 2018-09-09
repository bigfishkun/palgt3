#  -*- coding: utf-8 -*-

from pymongo import UpdateOne
from q001.database import DB_CONN
import tushare as ts
from datetime import datetime
import time
from q001.jobqueue.jobqueue import *

"""
从Tushare获取日K数据，保存到本地MongoDB数据库中
"""


class Min5CrawlerMulti:
    def __init__(self):
        self.min5 = DB_CONN['min5']
        self.min5_hfq = DB_CONN['min5_hfq']
        self.min5_qfq = DB_CONN['min5_qfq']

    def crawl_index(self, begin_date=None, end_date=None):
        """
            抓取指数的日线数据，并保存到本地数据库中
            抓取的日期范围从2008-01-01至今
        """
        index_codes = ['000001', '000003', '399001', '399005', '399006']

        # 设置默认的日期范围
        if begin_date is None:
            begin_date = '2008-01-01'
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        for code in index_codes:
            df_daily = ts.get_k_data(code, index=True, start=begin_date, end=end_date)
            self.save_data(code, df_daily, self.min5, {'index': True})

    def save_data(self, code, df_daily, collection, extra_fields=None):
        """
        将从网上抓取的数据保存到本地MongoDB中

        :param code: 股票代码
        :param df_daily: 包含日线数据的DataFrame
        :param collection: 要保存的数据集
        :param extra_fields: 除了K线数据中保存的字段，需要额外保存的字段
        """
        update_requests = []
        for df_index in df_daily.index:
            daily_obj = df_daily.loc[df_index]
            doc = self.daily_obj_2_doc(code, daily_obj)

            if extra_fields is not None:
                doc.update(extra_fields)

            update_requests.append(
                UpdateOne(
                    {'code': doc['code'], 'date': doc['date'], 'index': doc['index']},
                    {'$set': doc},
                    upsert=True
                )
            )

            if len(update_requests) == 1000:
                # 批量写入，提高访问效率
                update_result = collection.bulk_write(update_requests, ordered=False)
                print('保存5分钟数据，代码： %s，插入：%4d条，更新：%4d条' %
                      (code, update_result.upserted_count, update_result.modified_count),
                      flush=True)
                update_requests = []

        # 批量写入，提高访问效率
        if len(update_requests) > 0:
            update_result = collection.bulk_write(update_requests, ordered=False)
            print('保存5分钟数据，代码： %s，插入：%4d条，更新：%4d条' %
                  (code, update_result.upserted_count, update_result.modified_count),
                  flush=True)

    def crawl(self, code, begin_date=None, end_date=None):
        """
        获取所有股票的K线数据（包括前后复权、不复权三种），保存到数据库中
        :param begin_date:
        :param end_date:
        :return:
        """

        # 设置默认的日期范围
        if begin_date is None:
            begin_date = '2008-01-01'

        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        # 抓取不复权的价格
        df_daily = ts.get_k_data(code, ktype='5', autype=None, start=begin_date, end=end_date, retry_count=100, pause=10)
        self.save_data(code, df_daily, self.min5, {'index': False})

    @staticmethod
    def daily_obj_2_doc(code, daily_obj):
        return {
            'code': code,
            'date': daily_obj['date'],
            'close': daily_obj['close'],
            'open': daily_obj['open'],
            'high': daily_obj['high'],
            'low': daily_obj['low'],
            'volume': daily_obj['volume']
        }


def worker(code, begin_date=None, end_date=None):
    print("Worker is working...")
    dc = Min5CrawlerMulti()
    dc.crawl(code, begin_date, end_date)


def collector(meta, result):
    pass


def generateDailyJob():
    # put task parameters into the task jobqueue
    # 把任务加入任务队列
    # 获取所有股票代码
    print("Get all codes...")
    stock_df = ts.get_stock_basics()
    codes = list(stock_df.index)
    print("All codes: " + str(codes))

    queue = Queue()

    for code in codes:
        print(code)
        queue.insertJob("daily", {"code": code, "start": '2017-01-01', "end": '2018-12-31'})

def generateDailyJob(start, end):
    # put task parameters into the task jobqueue
    # 把任务加入任务队列
    # 获取所有股票代码
    print("Get all codes...")
    df_basics = ts.get_stock_basics()
    codes = list(df_basics.index)
    print("All codes: " + str(codes))

    queue = Queue()

    for code in codes:
        doc = dict(df_basics.loc[code])
        time_to_market = None
        try:
            # 将20180101转换为2018-01-01的形式
            time_to_market = datetime \
                .strptime(str(doc['timeToMarket']), '%Y%m%d') \
                .strftime('%Y-%m-%d')
        except:
            print('发生异常，股票代码：%s' % code, flush=True)
            print(doc, flush=True)

        if time_to_market is None or time_to_market < end:
            queue.insertJob("daily", {"code": code, "start": start, "end": end})

def main():
    queue = Queue()
    dc = Min5CrawlerMulti()

    while True:
        startTime = time()
        job = queue.fetchNextFIFOJob()
        print(job)
        if job.get('payload') is None:
            continue

        dc.crawl(job.get('payload').get('param').get('code'), job.get('payload').get('param').get('start'),
                 job.get('payload').get('param').get('end'))

        queue.finishJob(job)

def main5():
    print("Get all codes...")
    df_basics = ts.get_stock_basics()
    codes = list(df_basics.index)
    print("All codes: " + str(codes))

    dc = Min5CrawlerMulti()
    for code in codes:
        dc.crawl(code, "2018-09-04","2018-09-08")

if __name__ == '__main__':
    # generateDailyJob("2015-01-01", "2017-01-01")
    # dc = Min5CrawlerMulti()
    # dc.crawl("000001", "2017-01-01","2018-01-01")
    main5()