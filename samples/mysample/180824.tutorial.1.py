from pyalgotrade import strategy
from pyalgotrade.barfeed import quandlfeed

class MyStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, instrument):
        super(MyStrategy, self).__init__(feed)
        self.__instrument = instrument

    def onBars(self, bars):
        bar = bars[self.__instrument]
        self.info(bar.getClose())

# Load the bar feed from the CSV file
feed = quandlfeed.Feed()
feed.addBarsFromCSV("orcl", "D:/Develop/project/python/PycharmProjects/python3/pyalgotrade/samples/data/WIKI-ORCL-2011-quandl.csv")

# Evaluate the strategy with the feed's bars
myStratege = MyStrategy(feed, "orcl")
myStratege.run()