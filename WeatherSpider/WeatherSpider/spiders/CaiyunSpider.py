# -*- coding: utf-8 -*-
import scrapy


class CaiyunspiderSpider(scrapy.Spider):
    name = 'CaiyunSpider'
    allowed_domains = ['caiyunapp.com']
    start_urls = ['http://caiyunapp.com/']

    def parse(self, response):
        pass
