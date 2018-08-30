from __future__ import print_function

from pyalgotrade.feed import csvfeed

feed = csvfeed.Feed("Date", "%Y-%m-%d")
feed.addValuesFromCSV("D:/Develop/project/python/PycharmProjects/python3/pyalgotrade/samples/data/quandl_gold_2.csv")
for dateTime, value in feed:
    print(dateTime, value)
