# -*- coding: utf-8 -*-
import scrapy


class SeniversespiderrealSpider(scrapy.Spider):
    name = 'SeniverseSpiderReal'
    allowed_domains = ['seniverse.com']
    start_urls = ['http://seniverse.com/']

    def parse(self, response):
        pass
