#  -*- coding: utf-8 -*-

from database import *
from datetime import *
import tushare as ts
import jobqueue

def dailyJob():
    print("Get all codes...")
    stock_df = ts.get_stock_basics()
    codes = list(stock_df.index)
    print("All codes: " + str(codes))

    collection = DB_CONN['queue']
    codesInDB = collection.distinct("payload.param.code")
    print(codesInDB)

    print("=====================")
    # queue = jobqueue.Queue()
    for code in codes:
        if code not in codesInDB:
            print(code)

            # queue.insertJob("daily", {"code": code, "start": '2017-01-01', "end": '2018-12-31'})


if __name__ == "__main__":
    dailyJob()