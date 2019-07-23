# -*- coding: utf-8 -*-
import scrapy


class XicidailiSpider(scrapy.Spider):
    name = 'xicidaili'
    allowed_domains = ['xicidaili.com']
    start_urls = ['http://xicidaili.com/']

    def __init__(self, page):
        super().__init__()
        self.ips = []
        self.urls = []
        for i in range(page):
            self.urls.append("http://www.xicidaili.com/nn/" + str(i))
        self.header = {"User-Agent": 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0'}
        # self.file=open("ips",'w')
        #self.q = Queue.Queue()


    def parse(self, response):
        pass
