class USAUrl:
    PRE = "https://www.macrotrends.net/stocks/charts/"
    TICKET = "https://www.macrotrends.net/stocks/research"
    sp500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    nasdaq = "https://en.wikipedia.org/wiki/NASDAQ-100#Components"
    DIV = "https://www.macrotrends.net/stocks/charts/{}/x/dividend-yield-history"


class TWNUrl:
    PRE = "https://histock.tw/stock/financial.aspx?no={}&t=6"
    EPS = "https://histock.tw/stock/{}/%E6%AF%8F%E8%82%A1%E7%9B%88%E9%A4%98"
    TICKET = "https://histock.tw/stock/rank.aspx?m=13&d=1&p=all"
    STOCK = "https://histock.tw/stock/{}"
    CONCENTRATE = "https://histock.tw/stock/large.aspx?no={}"
    CHIPS = "https://histock.tw/stock/chips.aspx?no={}"
    PAST_PRICE = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_AVG?response=html&date={}&stockNo={}"


URL_dict = {
    'USA': USAUrl(),
    'TWN': TWNUrl()
}

Q2M = {
    'Q1': '-03-31',
    'Q2': '-06-30',
    'Q3': '-09-30',
    'Q4': '-12-31',
}

AvailableLocation = ['USA', 'TWN']

TwnSectorUrl = 'https://histock.tw/twclass/A0{}'

TwnSector = {
    '水泥': '11',
    '食品': '12',
    '塑膠': '13',
    '紡織': '14',
    '電機機械': '15',
    '電器電纜': '16',
    '玻璃': '19',
    '造紙': '20',
    '鋼鐵': '21',
    '橡膠': '22',
    '汽車': '23',
    '電子': '09',
    '營建': '32',
    '航運': '33',
    '觀光': '34',
    '金融': '35',
    '貿易百貨': '36',
    '化學': '03',
    '生技醫療': '18',
    '半導體': '24',
    '電腦週邊': '25',
    '光電': '26',
    '通信網路': '27',
    '電零組': '28',
    '電子通路': '29',
    '資訊服務': '30',
    '其他電子': '31',
    '油電燃氣': '37',
    '其他': '38',
    '非電金': '05'
}

