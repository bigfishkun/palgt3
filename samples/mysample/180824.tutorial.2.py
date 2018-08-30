from pyalgotrade import strategy
from pyalgotrade.barfeed import  quandlfeed
from pyalgotrade.technical import ma, rsi

def safe_round(value, digits):
    if value is not None:
        value = round(value, digits)
    return value

class MyStrategy(strategy.BacktestingStrategy):
    if __name__ == '__main__':
        def __init__(self, feed, instrument):
            super(MyStrategy, self).__init__(feed)
            # We want a 15 period SMA over the closing prices
            self.__rsi = rsi.RSI(feed[instrument].getCloseDataSeries(), 14)
            self.__sma = ma.SMA(self.__rsi, 15)
            self.__instrument = instrument

        def onBars(self, bars):
            bar = bars[self.__instrument]
            self.info("%s %s %s" % (
                bar.getClose(), safe_round(self.__rsi[-1], 2), safe_round(self.__sma[-1], 2)
            ))

# Load the bar feed from the CSV file
feed = quandlfeed.Feed()
feed.addBarsFromCSV("orcl", "D:/Develop/project/python/PycharmProjects/python3/pyalgotrade/samples/data/WIKI-ORCL-2011-quandl.csv")

# Evaluate the strategy with the feed's bars
myStratege = MyStrategy(feed, "orcl")
myStratege.run()
