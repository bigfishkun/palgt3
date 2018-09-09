import tushare as ts

ts.set_token('28d628a64a393c98ae3d243171132d7b3a4eaf22ac8fddf16c85e365')
pro = ts.pro_api()
#
# df = pro.daily(ts_code='000008.SZ', start_date='20060701', end_date='20060718')
#
# print(df)
#
# # cal = pro.query('trade_cal', start_date='20070104', end_date='20070104')
# # print(cal)
#
# data = pro.query('stock_basic', exchange_id='', is_hs='H',
#                  fields='symbol,is_hs,name,fullname,enname,exchange_id,curr_type,list_date,list_status,delist_date')
# print(data)
#
# df = ts.get_tick_data('000008',date='2017-01-09')
# print(df)

# from time import time, sleep
# start = time()
# all = ts.get_today_all()
# end = time()
# print(end - start)
# print(all)

# df = pro.query('suspend', ts_code='000008.SZ')#, suspend_date='20180720', resume_date='', fiedls='')
# print(df)
#
# df = pro.query('adj_factor',  trade_date='20180803')
# print(df)

df = pro.query('daily_basic', trade_date='19920721')#, suspend_date='20180720', resume_date='', fiedls='')
print(df)


def test():
    from datetime import datetime, timedelta
    df_basics = ts.get_stock_basics()
    print(len(df_basics))

    codes = set(df_basics.index)
    i = 0
    for code in codes:
        doc = dict(df_basics.loc[code])
        # print(doc)
        time_to_market = None
        try:
        # name = str(doc['name'])
        # industry = str(doc['industry'])
        # area = str(doc['area'])
        # pe = float(doc['pe'])
        # outstanding = float(doc['outstanding'])
        # totals = float(doc['totals'])
        # totalAssets = float(doc['totalAssets'])
        # liquidAssets = float(doc['liquidAssets'])
        # fixedAssets = float(doc['fixedAssets'])
        # reserved = float(doc['reserved'])
        # reservedPerShare = float(doc['reservedPerShare'])
        # esp = float(doc['esp'])
        # bvps = float(doc['bvps'])
        # pb = float(doc['pb'])
        # 将20180101转换为2018-01-01的形式
            time_to_market = datetime \
                .strptime(str(doc['timeToMarket']), '%Y%m%d') \
                .strftime('%Y-%m-%d')
        except:
            print('发生异常，股票代码：%s' % code, flush=True)
            print(doc, flush=True)

        if time_to_market is None or time_to_market < '2007-01-02':
            print(code)
            i = i+1
    print(i)

print('##################')
df = ts.get_k_data('000001', autype=None,start='2018-09-07',end='2018-09-07')
print(df)

df = ts.get_k_data('000001', autype='hfq',start='2018-09-07',end='2018-09-07')
print(df)

df = ts.get_k_data('000001', autype='qfq',start='2018-09-07',end='2018-09-07')
print(df)

df = pro.adj_factor(ts_code='000001.SZ', trade_date='20180907')
print(df)

df = pro.adj_factor(ts_code='000001.SZ')
print(df)