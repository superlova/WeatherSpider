# -*- coding: utf-8 -*-
import scrapy

from tutorial.items import NMCItemReal
import json
from tutorial.UpdateProxyIPPool import UpdateProxyIPPool

class NMCSpider(scrapy.Spider):
    city_url_dicts = {}

    name = 'NMCSpider'
    allowed_domains = ['www.nmc.cn']
    start_urls = []

    def __init__(self):
        super().__init__()
        # 初始化start_urls
        NMCSpider.init_city_urls()
        NMCSpider.start_urls = NMCSpider.city_url_dicts.values()
        self.pool = UpdateProxyIPPool(4)
        self.pool.main()
        self.pool.savetxt('procies_list.txt')


    def parse(self, response):
        if response.status != 200:
            print('error! code:' , response.status)
            return

        # 首先实例化item对象
        item = NMCItemReal()
        jsonData = json.loads(response.body_as_unicode())
        item['publish_time'] = jsonData['publish_time']
        # item['station_url'] = jsonData['station']['url']
        # item['station_city'] = jsonData['station']['city']
        item['station_code'] = jsonData['station']['code']
        # item['station_province'] = jsonData['station']['province']
        item['weather_temperature'] = jsonData['weather']['temperature']
        # item['weather_temperatureDiff'] = jsonData['weather']['temperatureDiff']
        item['weather_airpressure'] = jsonData['weather']['airpressure']
        item['weather_humidity'] = jsonData['weather']['humidity']
        item['weather_rain'] = jsonData['weather']['rain']
        # item['weather_rcomfort'] = jsonData['weather']['rcomfort']
        # item['weather_icomfort'] = jsonData['weather']['icomfort']
        item['weather_info'] = jsonData['weather']['info']
        # item['weather_feelst'] = jsonData['weather']['feelst'] # 体感温度
        item['wind_direct'] = jsonData['wind']['direct']
        item['wind_power'] = jsonData['wind']['power']

        yield item

    @staticmethod
    def init_city_urls():
        '''初始化city_urls的字典，供start_urls使用'''
        url_real_prefix = "http://www.nmc.cn/f/rest/real/"

        with open('city_list_nmc.txt', 'r', encoding='utf8') as f:
            for line in f:
                v = line.strip().split(' ')
                url = url_real_prefix + v[0]
                NMCSpider.city_url_dicts[v[1]] = url
