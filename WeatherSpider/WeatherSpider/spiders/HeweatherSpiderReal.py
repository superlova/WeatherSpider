# -*- coding: utf-8 -*-
import scrapy


class HeweatherspiderrealSpider(scrapy.Spider):
    name = 'HeweatherSpiderReal'
    allowed_domains = ['heweather.net']
    start_urls = ['http://heweather.net/']

    def parse(self, response):
        pass
