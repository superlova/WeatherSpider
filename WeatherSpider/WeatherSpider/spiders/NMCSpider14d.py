# -*- coding: utf-8 -*-
import scrapy


class Nmcspider14dSpider(scrapy.Spider):
    name = 'NMCSpider14d'
    allowed_domains = ['nmc.cn']
    start_urls = ['http://nmc.cn/']

    def parse(self, response):
        pass
