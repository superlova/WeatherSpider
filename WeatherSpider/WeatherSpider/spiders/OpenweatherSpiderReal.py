# -*- coding: utf-8 -*-
import scrapy


class OpenweatherspiderrealSpider(scrapy.Spider):
    name = 'OpenweatherSpiderReal'
    allowed_domains = ['openweathermap.org']
    start_urls = ['http://openweathermap.org/']

    def parse(self, response):
        pass
