from pyalgotrade import strategy
from pyalgotrade import plotter
from pyalgotrade.stratanalyzer import returns
from pyalgotrade.barfeed import quandlfeed
from pyalgotrade.technical import  ma

class MyStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, instrument, smaPeriod):
        super(MyStrategy, self).__init__(feed, 1000)
        self.__position = None
        self.__instrument = instrument
        # We'll use adjusted close values instead of regular close values
        self.setUseAdjustedValues(True)
        self.__prices = feed[instrument].getPriceDataSeries()
        self.__sma = ma.SMA(self.__prices, smaPeriod)

    def getSMA(self):
        return self.__sma

    def onEnterOk(self, position):
        execInfo = position.getEntryOrder().getExecutionInfo()
        self.info("Buy at $%.2f" % execInfo.getPrice())

    def onEnterCanceled(self, position):
        self.__position = None

    def onExitOk(self, position):
        execInfo = position.getExitOrder().getExecutionInfo()
        self.info("SELL at$%.2f" % execInfo.getPrice())
        self.__position = None

    def onExitCanceled(self, position):
        # If the exit was cancelled, re-submit it
        self.__position.exitMarket()

    def onBars(self, bars):
        # Wait for enough bars to be available to calculate a SMA
        if self.__sma[-1] is None:
            return

        bar = bars[self.__instrument]
        # If a position was not opened, check if we should enter a long position
        if self.__position is None:
            if bar.getPrice() > self.__sma[-1]:
                # Enter a buy market order for 10 shares. The order is good till cancelled
                self.__position = self.enterLong(self.__instrument, 10, True)

        # Check if we have to exit the position
        elif bar.getPrice() < self.__sma[-1] and not self.__position.exitActive():
            self.__position.exitMarket()


if __name__ == '__main__':
    def run_strategy(smaPeriod):
        # Load the bar feed from the CSV file
        feed = quandlfeed.Feed()
        feed.addBarsFromCSV("orcl",
                            "D:/Develop/project/python/PycharmProjects/python3/pyalgotrade/samples/data/WIKI-ORCL-2011-quandl.csv")

        # Evaluate the strategy with the feed's bars
        myStratege = MyStrategy(feed, "orcl", smaPeriod)

        # Attach a returns analyzer to to strategy
        returnsAnalyzer = returns.Returns()
        myStratege.attachAnalyzer(returnsAnalyzer)

        # Attach the plotter to the strategy
        plt = plotter.StrategyPlotter(myStratege)
        # Include the SMA in the instrument's subplot to get it displayed along with the closing prices
        plt.getInstrumentSubplot("orcl").addDataSeries("SMA", myStratege.getSMA())
        # Plot the simple returns on each bar
        #plt.getOrCreateSubplot("returns").addDataSeries("Simple Returns", returnsAnalyzer.getReturns())

        myStratege.run()
        myStratege.info("Final portfolio value: $%.2f" % myStratege.getBroker().getEquity())

        # Plot the strategy
        plt.plot()

run_strategy(30)


