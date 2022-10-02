from web_scrap import PageTableHandler
from the_enum import AvailableLocation, URL_dict, Q2M, TwnSector, TwnSectorUrl

from multiprocessing import Pool
from functools import partial
import datetime
import pandas as pd
import os


class TicketDataProvider:
    """
    Abstract class for ticket data provider
    """
    def __init__(self):
        self.parser = PageTableHandler()
        self.PER_base_url = None

    def getPERBaseURL(self):
        raise NotImplementedError

    def getTicketPERandEPSTable(self, ticket):
        raise NotImplementedError

    def getPriceDetail(self, ticket):
        raise NotImplementedError


class SectorDateProvider:
    """
        Abstract class for sector data provider
    """
    def __init__(self, location):
        self.parser = PageTableHandler()
        self.location = location
        self.tdp = self.getLocationTicketDataProvider()

    def getSectorStockList(self, sector):
        raise NotImplementedError

    def getLocationTicketDataProvider(self):
        if self.location in AvailableLocation:
            return eval(self.location + 'TicketDataProvider')()
        else:
            raise Exception("Change location please.")


class USATicketDataProvider(TicketDataProvider):
    """
    Class for USA ticket data provider
    """
    def __init__(self):
        super().__init__()
        self.PER_base_url = self.getPERBaseURL()
        self.ticket_base_url = URL_dict['USA'].TICKET
        self.ticket2nameDict = {}
        self.dividendUrl = URL_dict['USA'].DIV
        self.spUrl = URL_dict['USA'].sp500
        self.nasdaq = URL_dict['USA'].nasdaq
        self.sp500Df = self.getSP500Df()
        self.ticket_url_dict = self.getUrlDict()

    def getPERBaseURL(self):
        return URL_dict['USA'].PRE

    def getUrlDict(self):
        maxIter = 99
        dic = {}
        self.parser.setURL(self.ticket_base_url)
        tree = self.parser.getTRElements()

        for i in range(0, maxIter, 2):
            href = tree.xpath('//tr/td/a')[i].get('href')
            key = href.split('/')[3]
            name = href.split('/')[4]
            if key not in dic:
                self.ticket2nameDict[key] = name
                dic[key] = [self.PER_base_url + key + '/' + name + '/pe-ratio',
                            self.PER_base_url + key + '/' + name + '/stock-price-history']

        for t in self.getSP500Tickets():
            if t not in dic:
                dic[t] = [self.PER_base_url + t + '/' + '/pe-ratio',
                          self.PER_base_url + t + '/' + '/stock-price-history']

        return dic

    def getSP500Df(self):
        self.parser.setURL(self.spUrl)
        tree = self.parser.getTRElements()
        col = ['Symbol', 'Security', 'SEC filings', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date first added', 'CIK', 'Founded']
        ele = tree.xpath('//tr//td')
        data = []
        for i in range(0, 4545, 9):
            data.append([ele[i + j].text_content() for j in range(9)])
        df = pd.DataFrame(data, columns=col)
        df['Symbol'] = [s.split("\n")[0] for s in df['Symbol']]
        return df

    def getSP500Tickets(self):
        return self.sp500Df.Symbol.to_list()

    def getAvailableTicket(self):
        return list(self.ticket_url_dict.keys())

    def getTicketPERandEPSTable(self, ticket):
        print(ticket)
        if ticket in self.ticket_url_dict:
            url = self.ticket_url_dict[ticket][0]
        else:
            raise Exception("Change ticket please.")
        try:
            self.parser.setURL(url)
            tree = self.parser.getTRElements()
            col_name = self.parser.getTableColumnName(tree.xpath('//tr')[1])
            value = []
            stop = False

            for s in self.parser.getTableChildValue(tree.xpath('//tr')[2:]):
                row = []
                for ss in s.split('\r\n\t\t\t\t')[1:5]:
                    row.append(ss[1:])
                    if row[0] == 'Sector':
                        stop = True
                        break
                if stop:
                    break
                if '$' in row[2]:
                    row[2] = row[2].replace('$', '')
                value.append(row)
            df = pd.DataFrame(data=value, columns=col_name)
            df['Stock Price'] = [float(x.replace(',', '')) if x else None for x in df['Stock Price']]
            df['TTM Net EPS'] = [float(x.replace(',', '')) if x else None for x in df['TTM Net EPS']]
            df['PE Ratio'] = [float(x.replace(',', '')) if x else None for x in df['PE Ratio']]
            df['Date'] = [pd.to_datetime(x, format="%Y-%m-%d") for x in df['Date']]
            return df
        except:
            return pd.DataFrame()

    def getDividendYield(self, ticket):
        self.parser.setURL(self.dividendUrl.format(ticket))
        tree = self.parser.getTRElements()
        try:
            y = float(tree.xpath('//strong')[1].text_content().split('%')[0])
        except:
            y = -100
        return y

    def getPriceDetail(self, ticket):
        if ticket in self.ticket_url_dict:
            url = self.ticket_url_dict[ticket][1]
        else:
            raise Exception("Change ticket please.")

        self.parser.setURL(url)
        tree = self.parser.getTRElements()
        strong = tree.xpath('//strong')
        return {'current':          float(strong[0].text_content()),
                'highest':          float(strong[1].text_content()),
                '52_high':          float(strong[3].text_content()),
                '52_low':           float(strong[5].text_content()),
                '52_avg':           float(strong[7].text_content())
                }


class TWNTicketDataProvider(TicketDataProvider):
    """
        Class for TWN ticket data provider
    """
    def __init__(self):
        super().__init__()
        self.PER_base_url = self.getPERBaseURL()
        self.EPS_base_url = URL_dict['TWN'].EPS
        self.ticket_base_url = URL_dict['TWN'].TICKET
        self.ticket_stock_url = URL_dict['TWN'].STOCK
        self.ticket_concentrate_url = URL_dict['TWN'].CONCENTRATE
        self.ticket_chip_url = URL_dict['TWN'].CHIPS
        self.ticket_price_history = URL_dict['TWN'].PAST_PRICE

    def getPERBaseURL(self):
        return URL_dict['TWN'].PRE

    def getAvailableTicket(self, min_daily_transaction = 2):
        tickets = [] # Over NTD 200 million per day
        self.parser.setURL(self.ticket_base_url)
        tree = self.parser.getTRElements()

        ind = 12  # first daily transaction
        daily_transaction = float(tree.xpath('//tr/td')[ind].text_content().replace(',', ''))

        while daily_transaction >= min_daily_transaction:
            tickets.append((tree.xpath('//tr/td')[ind - 12].text_content(),
                            tree.xpath('//tr/td')[ind - 11].text_content().split('\r\n')[1].split(' ')[-1]))
            ind += 13
            daily_transaction = float(tree.xpath('//tr/td')[ind].text_content().replace(',', ''))
        return tickets

    def getTicketPERandEPSTable(self, ticket):
        print(ticket)
        value = []
        # Get EPS data
        try:
            self.parser.setURL(self.EPS_base_url.format(ticket))
            tree = self.parser.getSoupTRElements()
            table = tree.find("table", class_="tb-stock text-center tbBasic")
            eps_tr = table.find_all("tr")
            eps_year_list = [eps_tr[0].text[5:][i:i + 4] for i in range(0, len(eps_tr[0].text[5:]), 4)]
            eps_td = table.find_all("td")

            for i in range(8, 0, -1):
                for j in range(3, -1, -1):
                    val = eps_td[i + 9 * j].text
                    if val != '-':
                        value.append([eps_year_list[i] + Q2M['Q' + str(j + 1)], float(val)])
        except:
            return pd.DataFrame()

        # Get PER data
        try:
            self.parser.setURL(self.PER_base_url.format(ticket))
            tree = self.parser.getSoupTRElements()
            table = tree.find("table", class_="tb-stock tb-outline tbBasic")
            per_data = table.find_all("td")

            eps_latest_year = value[0][0].split('-')[0]
            eps_latest_month = value[0][0].split('-')[1]
            start_storing = False
            ind = 0
            val = 0
            cnt = 0

            for j in range(0, 10, 2):
                for i in range(j, len(per_data), 10):
                    if eps_latest_year == per_data[i].text.split('/')[0]:
                        if eps_latest_month == per_data[i].text.split('/')[1]:
                            start_storing = True
                    if start_storing:
                        val += float(per_data[i + 1].text)
                        cnt += 1
                    if cnt == 3:
                        val /= 3
                        value[ind].append(val)
                        val = 0
                        cnt = 0
                        ind += 1
        except:
            return pd.DataFrame()

        value = [v for v in value if len(v) == 3]
        col_name = ['Date', 'TTM Net EPS', 'PE Ratio']
        df = pd.DataFrame(data=value, columns=col_name)
        df['Date'] = [pd.to_datetime(x, format="%Y-%m-%d") for x in df['Date']]
        df['TTM Net EPS'] = [float(x) if x else None for x in df['TTM Net EPS']]
        df['PE Ratio'] = [float(x) if x else None for x in df['PE Ratio']]
        return df

    def getPriceDetail(self, ticket):
        self.parser.setURL(self.PER_base_url.format(ticket))
        tree = self.parser.getSoupTRElements()
        price = tree.find("span", class_="clr-gr") or tree.find("span", id="CPHB1_Price1_lbTPrice")
        return {
            'current': float(price.text)
        }

    def getMonthPerformance(self, ticket):
        self.parser.setURL(self.ticket_stock_url.format(ticket))
        tree = self.parser.getTRElements()
        data = tree.xpath('//tr//td')

        try:
            three_day = float(data[0].text_content().split(' ')[-1].split('%')[0])
            one_week = float(data[2].text_content().split(' ')[-1].split('%')[0])
            two_week = float(data[4].text_content().split(' ')[-1].split('%')[0])
            one_month = float(data[8].text_content().split(' ')[-1].split('%')[0])
        except:
            three_day = 1000
            one_week = 1000
            two_week = 1000
            one_month = 1000
        return ticket, three_day, one_week, two_week, one_month

    def getWeeklyConcentration(self, ticket):
        self.parser.setURL(self.ticket_concentrate_url.format(ticket))
        tree = self.parser.getTRElements()
        data = tree.xpath('//tr//td')
        columns = ['date', 'concentration', 'foreign', 'big account', 'board']
        data_list = []
        for i in range(0, len(data), 5):
            data_list.append([data[i].text_content(),
                              float(data[i + 1].text_content().split('%')[0]),
                              float(data[i + 2].text_content().split('%')[0]),
                              float(data[i + 3].text_content().split('%')[0]),
                              float(data[i + 4].text_content().split('%')[0])])

        df = pd.DataFrame(data_list, columns=columns)
        df['date'] = [pd.to_datetime(x, format="%Y-%m-%d") for x in df['date']]

        return df

    def get30dayMainForceBuySell(self, ticket):
        self.parser.setURL(self.ticket_chip_url.format(ticket))
        tree = self.parser.getTRElements()
        data = tree.xpath('//tr//td')
        columns = ['date', 'foreign', 'fund trust', 'self_buy', 'self_hedge', 'total']
        data_list = []
        for i in range(0, len(data), 6):
            data_list.append([data[i].text_content(),
                              int(data[i + 1].text_content().replace(',', '')),
                              int(data[i + 2].text_content().replace(',', '')),
                              int(data[i + 3].text_content().replace(',', '')),
                              int(data[i + 4].text_content().replace(',', '')),
                              int(data[i + 5].text_content().replace(',', ''))])
        df = pd.DataFrame(data_list, columns=columns)
        df['date'] = [pd.to_datetime(x, format="%Y-%m-%d").date() for x in df['date']]
        return df

    def fetchHistoryPrice(self, ticket, date):
        self.parser.setURL(self.ticket_price_history.format(date, ticket))
        tree = self.parser.getTRElements()
        element = tree.xpath('//tr//td')
        data = []
        print(date)
        for i in range(2, len(element)-2, 2):
            data.append([element[i].text_content(), float(element[i+1].text_content())])
        return data

    def getPriceHistoryDataFromWeb(self, ticket, years=3):
        """
        Get the price history, currently set for 3 years data.
        """
        file_path = './twn_stock_price_history/{}.csv'.format(ticket)
        if os.path.isfile(file_path):
            return pd.read_csv(file_path)
        else:
            this_year = int(datetime.datetime.today().year)
            this_month = int(datetime.datetime.today().month)
            dateToFetch = ['{}{:02d}01'.format(y, m) for y in range(this_year-years, this_year, 1) for m in range(1, 13)] + ['{}{:02d}01'.format(this_year, m) for m in range(1, this_month, 1)]
            data = []
            df = pd.DataFrame(data, columns=['date', 'close price'])
            func = partial(self.fetchHistoryPrice, ticket)
            for d in dateToFetch:
                c_df = func(d)
                try:
                    df = df.append(c_df)
                except:
                    print(d, " : error.")
            # with Pool(processes=4) as pool:
            #     for i in pool.imap_unordered(self.fetchHistoryPrice, dateToFetch):
            #         c_df = pd.DataFrame(i, columns=['date', 'close price'])
            #         df = df.append(c_df)

            df['date'] = [datetime.datetime.strptime('{}-{}-{}'.format(int(d.split('/')[0])+1911, d.split('/')[1], d.split('/')[2]), "%Y-%m-%d") for d in df['date']]
            df.sort_values(by=['date'], inplace=True)
            df.to_csv(file_path)
            return df

    def getPriceHistoryDataFromHtml(self, ticket):
        """
        Read table from html.
        :return: 1 year data
        """
        file_path = './twn_stock_price_history/{}.html'.format(ticket)
        if os.path.isfile(file_path):
            df = pd.read_html(file_path)[0]
        else:
            raise Exception('Please download from good.info.com.tw')

        cols = []
        for a, b in zip(df.iloc[-2], df.iloc[-1]):
            if a == b:
                cols.append(a)
            else:
                cols.append(a + b)
        df.columns = cols
        df = df[:-2]

        for col in df.columns[1:]:
            # to float and remove ','
            df[col] = [float(s.replace(',', '')) for s in df[col]]
        return df

    def getMarketCloseDate(self):
        self.parser.setURL("https://histock.tw/holiday.aspx?id=TWSE")
        tree = self.parser.getTRElements()
        data = tree.xpath('//tr//td')
        date = []
        for i in range(11, 45, 2):
            date.append(datetime.datetime.strptime(data[i].text_content(), "%Y/%m/%d").date())
        return date


class TWNSectorDataProvider(SectorDateProvider):
    """
        Class for TWN ticket data provider
    """

    def getSectorStockList(self, sector):
        if not sector in TwnSector.keys():
            raise Exception("Change sector please.")

        url = self.getsectorUrl(sector)
        self.parser.setURL(url)
        tree = self.parser.getTRElements()

        ind = 48
        stock_list = []
        stock = tree.xpath('//span')[ind].text_content()
        while stock != '三日':
            stock_list.append(stock)
            ind += 12
            stock = tree.xpath('//span')[ind].text_content()
        return stock_list

    def getsectorUrl(self, sector):
        return TwnSectorUrl.format(TwnSector[sector])

    def getSectorStockPerformance(self, sector):
        tickets = self.getSectorStockList(sector)
        data = []
        with Pool(processes=4) as pool:
            for i in pool.imap_unordered(self.tdp.getMonthPerformance, tickets):
                print(i[0])
                data.append(list(i))
        return pd.DataFrame(data, columns=['ticket', '3 days', '1 week', '2 weeks', '1 month'])




