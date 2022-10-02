import requests
import lxml.html
from bs4 import BeautifulSoup
import random


class PageTableHandler:
    def __init__(self):
        self.url = None

    def setURL(self, url):
        self.url = url

    def get_html(self):
        id = ['55.0.2883.95', '75.0.3770.142']
        referer = ['https://www.google.com/', 'https://www.yahoo.com/', 'https://duckduckgo.com/']
        r_id = id[random.randint(0, len(id) - 1)]
        r_referer = referer[random.randint(0, len(referer) - 1)]
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2; X11; Ubuntu; Linux x86_64; rv:52.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{} Safari/537.36'.format(r_id),
            'referer': r_referer
        }
        res = requests.get(self.url, headers=headers)
        res.encoding = 'utf-8'
        return res.text

    def getTRElements(self):
        # Create a handle, page, to handle the contents of the website
        response = requests.get(self.url, stream=True)
        response.raw.decode_content = True

        # Store the contents of the website under doc
        tree = lxml.html.parse(response.raw)

        return tree

    def getSoupTRElements(self):
        return BeautifulSoup(self.get_html(), 'lxml')

    @staticmethod
    def getTableChildValue(currentTree):
        return [c.text_content() for c in currentTree]

    @staticmethod
    def getTableColumnName(firstTree):
        return [c.text_content().split('\n')[0] for c in firstTree]


