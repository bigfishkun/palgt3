#  -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import tushare as ts
from pymongo import UpdateOne
from database import DB_CONN
from stock_util import get_trading_dates

"""
从tushare获取股票基础数据，保存到本地MongoDB中
"""

def crawl_basic(begin_date=None, end_date=None):
    """
    抓取指定时间范围内的股票基础信息

    :param begin_date: 开始日期
    :param end_date: 结束日期
    :return:
    """

    if begin_date is None:
        begin_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    if end_date is None:
        end_date = begin_date

    all_dates = get_trading_dates(begin_date, end_date)

    for date in all_dates:
        try:
            crawl_basic_at_date(date)
        except:
            print('抓取股票基本信息时出错，日期：%s' % date, flush=True)

def crawl_basic_at_date(date=None):
    """
    从Tushare抓取指定日期的股票基本信息
    :param date: 日期
    :return: 股票基本信息
    """

    # 默认获取上一交易日的数据
    df_basics = ts.get_stock_basics(date)

    # 如果当日没有数据，则不做操作
    if df_basics is None:
        return

    update_requests = []
    codes = set(df_basics.index)

    for code in codes:
        doc = dict(df_basics.loc[code])
        # print(doc)
        try:
            name = str(doc['name'])
            industry = str(doc['industry'])
            area = str(doc['area'])
            pe = float(doc['pe'])
            outstanding = float(doc['outstanding'])
            totals = float(doc['totals'])
            totalAssets = float(doc['totalAssets'])
            liquidAssets = float(doc['liquidAssets'])
            fixedAssets = float(doc['fixedAssets'])
            reserved = float(doc['reserved'])
            reservedPerShare = float(doc['reservedPerShare'])
            esp = float(doc['esp'])
            bvps = float(doc['bvps'])
            pb = float(doc['pb'])
            # 将20180101转换为2018-01-01的形式
            time_to_market = datetime \
                .strptime(str(doc['timeToMarket']), '%Y%m%d') \
                .strftime('%Y-%m-%d')

            # undp = float(doc['undp'])
            # perundp = float(doc['perundp'])
            # rev = float(doc['rev'])
            # profit = float(doc['profit'])
            # gpr = float(doc['gpr'])
            # npr = float(doc['npr'])
            # holders = int(doc['holders'])

            doc.update({
                'code': code,
                'date': date,
                'name': name,
                'industry': industry,
                'area': area,
                'pe': pe,
                'outstanding': outstanding,
                'totals': totals,
                'totalAssets': totalAssets,
                'liquidAssets': liquidAssets,
                'fixedAssets': fixedAssets,
                'reserved': reserved,
                'reservedPerShare': reservedPerShare,
                'esp': esp,
                'bvps': bvps,
                'pb': pb,
                'timeToMarket': time_to_market
                # 'undp': undp,
                # 'perundp': perundp,
                # 'rev': rev,
                # 'profit': profit,
                # 'gpr': gpr,
                # 'npr': npr,
                # 'holders': holders
            })

            update_requests.append(
                UpdateOne(
                    {'code': code, 'date': date},
                    {'$set': doc}, upsert=True
                )
            )

        except:
            print('发生异常，股票代码：%s，日期：%s' % (code, date), flush=True)
            print(doc, flush=True)
    if len(update_requests) > 0:
        update_result = DB_CONN['basic'].bulk_write(update_requests, ordered=False)

        print('抓取股票基本信息，日期：%s, 插入：%4d条，更新：%4d条' %
              (date, update_result.upserted_count, update_result.modified_count), flush=True)


if __name__ == '__main__':
    crawl_basic('2018-04-17', '2018-12-31')