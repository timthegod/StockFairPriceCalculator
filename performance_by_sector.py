from the_enum import TwnSector
from data_processing import USATicketDataProvider, TWNTicketDataProvider, TWNSectorDataProvider
from multiprocessing import Pool
from the_speculator import MainForceAnalyzer

import csv
import numpy as np
import pandas as pd
import datetime


class SectorDataCollector:
    """
    Class to get stock data for certain sector
    """
    def __init__(self, location='TWN', sector='水泥'):

        self.sector = sector
        self.location = location
        self.sdp = self.getLocationSectorDataProvider()
        self.all_sector = self.getAllSector()
        self.sectorStockPerformanceDf = pd.DataFrame()

    def getLocationSectorDataProvider(self):
        if self.location in ['TWN']:
            return eval(self.location + 'SectorDataProvider')(self.location)
        else:
            raise Exception("Change location please.")

    @staticmethod
    def getAllSector():
        return TwnSector.keys()

    def getSectorStockTickets(self):
        """
        :return: List of tickets in the sector
        """
        return self.sdp.getSectorStockList(self.sector)

    def getSectorStockPerformance(self):
        """
        :return: Data Frame of performance of tickets in sector
        """
        return self.sdp.getSectorStockPerformance(self.sector)

    def worsePerformance(self, gap='1 month', percent=10):
        if self.sectorStockPerformanceDf.empty:
            self.sectorStockPerformanceDf = self.getSectorStockPerformance()
        availableGap = ['3 days', '1 week', '2 weeks', '1 month']
        if gap not in availableGap:
            raise Exception('Please select {}'.format('{}, '.format(s) for s in availableGap))

        threshold = np.percentile([x for x in self.sectorStockPerformanceDf[gap] if not np.isnan(x)], percent)

        return self.sectorStockPerformanceDf.loc[self.sectorStockPerformanceDf[gap] <= threshold]

    @staticmethod
    def stocktobuy(parameter):
        """
        :param parameter: 1. ticket 2. date
        """
        print(parameter[0])
        m = MainForceAnalyzer(parameter[0])
        return parameter[0], m.concentrationAnalysis(), m.buyPointAnalysis(parameter[1])

    def shortTermstockToBuy(self, Date=''):
        sectorTickets = self.getSectorStockTickets()
        stockBuyList = []
        with Pool(processes=4) as pool:
            for i in pool.imap_unordered(self.stocktobuy, [(t, d) for t, d in zip(sectorTickets, [Date for _ in range(len(sectorTickets))])]):
                stockBuyList.append(i)
        return stockBuyList


if __name__ == "__main__":
    from performance_by_sector import SectorDataCollector
    s = SectorDataCollector('TWN', '航運')
    print(s.worsePerformance(percent=50))

