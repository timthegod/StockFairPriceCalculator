from data_processing import USATicketDataProvider, TWNTicketDataProvider, TWNSectorDataProvider
from multiprocessing import Pool

import csv
import pandas as pd
import datetime
from tabulate import tabulate


class MainForceAnalyzer:
    """
    Class to analyze how the main force in stock market is buying and selling.
    Get on the upside train and get off in time.
    NOTICE: currently only support TWN
    """
    def __init__(self, ticket, location='TWN'):
        self.ticket = ticket
        self.location = location
        self.tdp = self.getLocationTicketDataProvider()
        self.concentration_df = pd.DataFrame()
        self.buySell_df = pd.DataFrame()
        self.closeDate = self.tdp.getMarketCloseDate()

    def getLocationTicketDataProvider(self):
        if self.location in ['TWN']:
            return eval(self.location + 'TicketDataProvider')()
        else:
            raise Exception("Change location please.")

    def concentrationAnalysis(self, concentrateThreshold=75, foreignThreshold=45):
        """
        Analyze the concentration of the stock.
        To determine if the stock is target or not.
        NOTICE: currently consider the LATEST percentage only.
        :return: True if highly concentrated and foreign fund > 45% else False
        """
        if self.concentration_df.empty:
            self.concentration_df = self.tdp.getWeeklyConcentration(self.ticket)
        if len(self.concentration_df.iloc[0]) >=2:
            return self.concentration_df.iloc[0][1] >= concentrateThreshold and self.concentration_df.iloc[0][2] >= foreignThreshold
        else:
            return False

    @staticmethod
    def s2b(t, y):
        """
        Big sell to big buy.
        :param t: today's total
        :param y: yesterday's total
        :return: True if sell to buy
        """
        if t == 0:
            t = -1
        if y == 0:
            y = -1
        return t > 0 > y

    def buyPointAnalysis(self, Date=''):
        """
        Analyze the buying point.
        :param currentDate: the "current" date.
        :return: (True, power)if main force turns from big sell to big buy else (False, None)
        """
        if self.buySell_df.empty:
            self.buySell_df = self.tdp.get30dayMainForceBuySell(self.ticket)
        if Date:
            if Date in self.closeDate:
                raise Exception('Market close, please change date.')
            else:
                Date = datetime.datetime.strptime(Date, "%Y-%m-%d").date()
        else:
            Date = datetime.datetime.today().date()
            if Date in self.closeDate:
                raise Exception('Market close, please change date.')

        self.buySell_df['buy_point'] = [self.s2b(self.buySell_df.iloc[i][5], self.buySell_df.iloc[i + 1][5]) for i in range(len(self.buySell_df) - 1)] + [False]
        buyOrNot = Date in [d for d in self.buySell_df[self.buySell_df['buy_point']]['date'].to_list()]
        if buyOrNot:
            date_df = self.buySell_df[self.buySell_df.date == Date]
            date_index = self.buySell_df[self.buySell_df.date == Date].index[0]
            power_percent = round(-(date_df.total / self.buySell_df.iloc[date_index + 1].total).values[0]*100, 2)
        else:
            power_percent = None
        return buyOrNot, power_percent

    def SellPointAnalysis(self, boughtDate):
        """
        Given bought date, analyze when to sell
        :param boughtDate: date of purchase
        :return: (True, the date should sell, total holding day) if need to sell else (False, None, total holding day)
        """
        if self.buySell_df.empty:
            self.buySell_df = self.tdp.get30dayMainForceBuySell(self.ticket)

        boughtDate = datetime.datetime.strptime(boughtDate, "%Y-%m-%d").date()
        turnedMinusCount = 0      # initialize
        holdingDay = 0            # initialize
        current_index = self.buySell_df[self.buySell_df.date == boughtDate].index[0]  # initialize
        while turnedMinusCount < 2 and current_index >= 0:
            if self.buySell_df.iloc[current_index].total <= 0:
                turnedMinusCount += 1
            current_index -= 1
            holdingDay += 1
        sellOrNot = turnedMinusCount == 2
        if sellOrNot:
            return True, self.buySell_df.iloc[current_index + 1].date, holdingDay
        else:
            return False, None, holdingDay


def BuyPointAnalysisMulti(info):
    print(info[0], info[1])
    m = MainForceAnalyzer(info[0])
    return info[0], info[1], m.concentrationAnalysis(), m.buyPointAnalysis(info[2])


def getHighVolumnBuyPointAnalysis(min_daily_transaction=2, date=''):
    tdp = TWNTicketDataProvider()
    tickets = tdp.getAvailableTicket(min_daily_transaction=min_daily_transaction)
    info = [[t[0], t[1], date] for t in tickets]
    data = []
    with Pool(processes=4) as pool:
        for r in pool.imap_unordered(BuyPointAnalysisMulti, info):
            data.append(r)
    data = [[i[0], i[1], i[2], i[3][0], i[3][1]] for i in data]

    buy = [d for d in data if d[3]]
    print(tabulate(buy, ['ticket', 'name', '75%Main', 'buy', 'power'], tablefmt="psql"))
    return data


def backTestMainForceSell2Buy(ticket, fund=1000000):
    tdp = TWNTicketDataProvider()
    m = MainForceAnalyzer(ticket)
    df = tdp.getPriceHistoryDataFromHtml(ticket)
    df['average_day_price'] = [(h + l) / 2 for h, l in zip(df['最高'], df['最低'])]

    price_col = 'average_day_price'

    holding = False
    hold_period = 0  # initial
    buy_price = 0  # initial
    buy_date_index = 0  # initial
    fee = 0.0015
    turnSell = 0
    transaction_data = []
    pnl = 1
    for i in range(len(df) - 3, -1, -1):
        if not holding:
            if m.s2b(df.iloc[i + 1]['法人買賣超(張)合計'], df.iloc[i + 2]['法人買賣超(張)合計']):
                buy_price = df.iloc[i][price_col] * (1 + fee)
                buy_date_index = i
                holding = True
                hold_period += 1
        else:
            if turnSell == 2:
                sell_price = df.iloc[i][price_col] * (1 - fee)
                holding = False
                turnSell = 0
                margin = (sell_price - buy_price) / buy_price
                pnl += (pnl * margin)
                transaction_data.append(
                    [df.iloc[buy_date_index]['交易日期'], df.iloc[i]['交易日期'], buy_price, sell_price, margin, hold_period])
                hold_period = 0
            elif turnSell != 2 and df.iloc[i]['法人買賣超(張)合計'] <= 0:
                turnSell += 1
                hold_period += 1
            else:
                hold_period += 1

    long_term_buy = df.iloc[-1][price_col] * (1 + fee)
    long_term_sell = df.iloc[0][price_col] * (1 - fee)
    long_term_pnl = (long_term_sell - long_term_buy) / long_term_buy

    return fund*pnl, fund*(1+long_term_pnl), transaction_data

