# -*- coding: utf-8 -*-
import scrapy


class Nmcspider2dSpider(scrapy.Spider):
    name = 'NMCSpider2d'
    allowed_domains = ['nmc.cn']
    start_urls = ['http://nmc.cn/']

    def parse(self, response):
        pass
