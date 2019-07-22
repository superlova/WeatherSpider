# -*- coding: utf-8 -*-
import scrapy


class Heweatherspider3dSpider(scrapy.Spider):
    name = 'HeweatherSpider3d'
    allowed_domains = ['heweather.net']
    start_urls = ['http://heweather.net/']

    def parse(self, response):
        pass
