#  -*- coding: utf-8 -*-

from pymongo import InsertOne
from database import DB_CONN
import tushare as ts
import datetime

"""
从tushare获取日K数据，保存到本地的MongoDB数据库中
"""


class DailyCrawler:
    def __init__(self):
        self.daily = DB_CONN['daily']
        self.daily_hfq = DB_CONN['daily_hfq']

    def crawl_index(self, begin_date='1990-01-01', end_date=None):
        """
        抓取指数的日线数据，并保存到本地数据数据库中
        抓取的日期范围从2008-01-01至今
        """
        index_codes = ['000001', '000300', '399001', '399005', '399006']

        if end_date is None:
            end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        #查询出所有指数的最后数据的日期
        max_date = self.get_max_date(self.daily,True)
        for code in index_codes:
            #如果数据库中的日期，跟最后日期相同，则取消作业
            daily_max_date = max_date.get(code,begin_date)
            if daily_max_date >= end_date:
                continue
            df_daily = ts.get_k_data(code, index=True, start=daily_max_date, end=end_date)
            self.save_data(code, df_daily, self.daily, {'index': True})

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

            update_requests.append(InsertOne(doc))
            # UpdateOne(
            #     {'code': doc['code'], 'date': doc['date'], 'index': doc['index']},
            #     {'$set': doc},
            #     upsert=False)

        # 批量写入，提高访问效率
        if len(update_requests) > 0:
            print('code:%s,stmts:%4d' % (code,len(update_requests)))
            #print(update_requests)
            update_result = collection.bulk_write(update_requests, ordered=False)

            print('保存日线数据，代码： %s, 插入：%4d条, 更新：%4d条,追加：%4d条' %
                  (code, update_result.inserted_count, update_result.modified_count,update_result.upserted_count),
                  flush=True)

    # 将日期解析成字符串，并且加
    def date_plus(self,str,pdays=1):
        d = datetime.datetime.strptime(str, '%Y-%m-%d') + datetime.timedelta(days=pdays)
        return d.strftime('%Y-%m-%d')

    def get_max_date(self,collection,is_index):
        coursor = collection.aggregate([{'$group': {'_id': {'code': "$code"}, 'count': {'$sum': 1},
                'max_date': {'$max': "$date"},'min_date': {'$min': "$date"}}}])
        max_date = {}
        for x in coursor:
            max_date[x['_id']['code']] = self.date_plus(x['max_date'])
        # print(max_date)
        return max_date

    def crawl(self, begin_date='1990-01-01', end_date=None):
        """
        获取所有股票从2008-01-01至今的K线数据（包括后复权和不复权三种），保存到数据库中
        """

        # 获取所有股票代码
        stock_df = ts.get_stock_basics()
        codes = list(stock_df.index)

        if end_date is None:
            end_date = datetime.datetime.now().strftime('%Y-%m-%d')

        # 一次查出所有个股的最新的不复权数据对应的日期
        daily_date = self.get_max_date(self.daily,False)

        # 一次查出所有个股的最新的后复权数据对应的日期
        daily_hfq_date = self.get_max_date(self.daily_hfq,False)

        for code in codes:
            daily_max_date = daily_date.get(code,begin_date)
            if daily_max_date < end_date:
                print('开始抓取不复权日线数据：code[%s],start:[%s],end:[%s]' % (code, daily_max_date, end_date))
                # 抓取不复权的价格
                df_daily = ts.get_k_data(code, autype=None, start=daily_max_date, end=end_date)
                self.save_data(code, df_daily, self.daily, {'index': False})

            daily_hfq_max_date = daily_hfq_date.get(code, begin_date)
            if daily_hfq_max_date < end_date:
                print('开始抓取后复权日线数据：code[%s],start:[%s],end:[%s]' % (code,daily_hfq_max_date,end_date))
                # 抓取后复权的价格
                df_daily_hfq = ts.get_k_data(code, autype='hfq', start=daily_hfq_max_date, end=end_date)
                self.save_data(code, df_daily_hfq, self.daily_hfq, {'index': False})

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

# db.daily.ensureIndex({"code":1})
# db.daily.ensureIndex({"code":1,"date":1})
# db.daily.remove({date:{$gte:'2018-07-24'}})
# db.daily_hfq.ensureIndex({"code":1})
# db.daily_hfq.ensureIndex({"code":1,"date":1})
# db.daily_hfq.remove({date:{$gte:'2018-07-24'}})

if __name__ == '__main__':
    dc = DailyCrawler()
    dc.crawl_index('1990-01-01')
    dc.crawl('1990-01-01')
