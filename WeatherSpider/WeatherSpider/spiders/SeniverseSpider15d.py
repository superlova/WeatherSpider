# -*- coding: utf-8 -*-
import scrapy


class Seniversespider15dSpider(scrapy.Spider):
    name = 'SeniverseSpider15d'
    allowed_domains = ['seniverse.comm']
    start_urls = ['http://seniverse.comm/']

    def parse(self, response):
        pass
