# -*- coding: utf-8 -*-
import scrapy


class NmcspiderrealSpider(scrapy.Spider):
    name = 'NMCSpiderReal'
    allowed_domains = ['nmc.cn']
    start_urls = ['http://nmc.cn/']

    def parse(self, response):
        pass
