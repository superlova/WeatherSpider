# -*- coding: utf-8 -*-
import scrapy


class Openweatherspider5dSpider(scrapy.Spider):
    name = 'OpenweatherSpider5d'
    allowed_domains = ['openweathermap.org']
    start_urls = ['http://openweathermap.org/']

    def parse(self, response):
        pass
