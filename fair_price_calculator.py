from the_enum import AvailableLocation
from data_processing import USATicketDataProvider, TWNTicketDataProvider

from multiprocessing import Pool
import numpy as np
import csv
import datetime
from tabulate import tabulate


class FairPriceCalculator:
    """
        Class to calculate fair price of the stock.
    """
    def __init__(self, location, ticket):

        self.ticket = ticket
        self.location = location
        self.tdp = self.getLocationTicketDataProvider()
        self.df = self.tdp.getTicketPERandEPSTable(ticket)
        self.IsEligible = True if len(self.df) >= 4 else False

    def getLocationTicketDataProvider(self):
        if self.location in AvailableLocation:
            return eval(self.location + 'TicketDataProvider')()
        else:
            raise Exception("Change location please.")

    def getAvgPER(self, col_name='PE Ratio'):
        if self.IsEligible:
            q1 = np.percentile([x for x in self.df[col_name] if not np.isnan(x)], 25)
            q3 = np.percentile([x for x in self.df[col_name] if not np.isnan(x)], 75)
            IQR = q3 - q1
            low_boundary = q1 - 1.5*IQR if q1 - 1.5*IQR >= 0 else 0
            high_boundary = q3 + 1.5*IQR
            to_count = []
            for x in self.df[col_name]:
                if not np.isnan(x):
                    if low_boundary <= x <= high_boundary and x != 0:
                        to_count.append(x)
            return np.average(to_count)

    def getAvgOneYearEPS(self, col_name='TTM Net EPS'):
        if self.IsEligible:
            if not all(not np.isnan(x) for x in self.df[col_name][:4]):
                return np.sum(self.df[col_name][1:5])
            else:
                return np.sum(self.df[col_name][0:4])

    def getFairPrice(self):
        if self.IsEligible:
            return self.getAvgPER()*self.getAvgOneYearEPS()

    def buyOrNot(self, p=False):
        """:parameter p: print detail"""
        try:
            price = self.tdp.getPriceDetail(self.ticket)
            fair_price = self.getFairPrice() if self.getFairPrice() else -10000
        except:
            return False, 0, 0, 0, 0
        if p:
            print(self.ticket)
            print('---------------')
            if price['current'] <= fair_price:
                print('Buy: ', price['current'], '<=', fair_price)
            else:
                print('No: ', price['current'], '>', fair_price)

            for k, v in price.items():
                print(k, ":\t", v)

        if price['current'] <= fair_price:
            return True, price['current'], fair_price, self.getAvgPER(), self.tdp.getDividendYield(self.ticket) if self.location == 'USA' else 0
        else:
            return False, price['current'], fair_price, self.getAvgPER(), -100

    def buyOrNotMulti(self, ticket):
        if self.location == 'TWN':
            t = ticket[0]
            tName = ticket[1]
        elif self.location == 'USA':
            t = ticket
            tName = None
        self.__init__(self.location, t)
        if not self.df.empty:
            result = self.buyOrNot()
            return t, tName if tName else '', result[0], result[1], result[2], result[3], result[4]
        else:
            return t, tName if tName else '', False, 0 , 0, 0, -100

    def getAllBuy(self, store=True):
        original_ticket = self.ticket
        store_path = '/Users/timmy/Desktop/投資理財/twn_stock_fair_price/twn_stock_fair_price_{}.csv'.format(datetime.datetime.today().strftime('%Y-%m-%d'))
        if self.location == 'USA' or self.location == 'TWN':
            if self.location == 'TWN':
                tickets = self.tdp.getAvailableTicket(min_daily_transaction = 2)
            else:
                tickets = self.tdp.getAvailableTicket()
            buy = []
            with Pool(processes=4) as pool:
                for i in pool.imap_unordered(self.buyOrNotMulti, tickets):
                    if i[2]:
                        buy.append((i[0], i[1], i[3], i[4], (i[4] - i[3])/i[3], i[5], i[6]))
            # In order to have same instance
            self.__init__(self.location, original_ticket)

            col = ['Num', 'Name', 'Current Price', 'Fair Price', 'gap', 'avg_pe', 'div_yield']

            if store:
                with open(store_path, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(col)
                    for r in buy:
                        writer.writerow(r)
            print(tabulate(buy, col, tablefmt='psql'))
            return buy
        else:
            return "Not yet provided."


if __name__ == "__main__":
    from fair_price_calculator import FairPriceCalculator
    f = FairPriceCalculator('TWN', '2330')
    r = f.getAllBuy()

