# -*- coding: utf-8 -*-
import scrapy
from tutorial.items import NMCItem2D
import json
from tutorial.UpdateProxyIPPool import UpdateProxyIPPool


class Nmcspider2dSpider(scrapy.Spider):
    city_url_dicts = {}

    name = 'NMCSpider2d'
    allowed_domains = ['www.nmc.cn']
    start_urls = []

    def __init__(self):
        super().__init__()
        # 初始化start_urls
        Nmcspider2dSpider.init_city_urls()
        Nmcspider2dSpider.start_urls = Nmcspider2dSpider.city_url_dicts.values()
        self.pool = UpdateProxyIPPool(4)
        self.pool.main()
        self.pool.savetxt('procies_list.txt')



    def parse(self, response):
        if response.status != 200:
            print('error! code:', response.status)
            return

        # 首先实例化item对象
        item = NMCItem2D()
        jsonData = json.loads(response.body_as_unicode())[0]
        item['publish_time'] = jsonData['publish_time']
        # item['station_url'] = jsonData['station']['url']
        # item['station_city'] = jsonData['station']['city']
        item['station_code'] = jsonData['station']['code']
        # item['station_province'] = jsonData['station']['province']
        item['temperature'] = jsonData['temperature']  # publish时的温度

        item['perdict_date_0'] = jsonData['detail'][0]['date']
        item['pd0_day_weather_info'] = jsonData['detail'][0]['day']['weather']['info']
        item['pd0_day_weather_temperature'] = jsonData['detail'][0]['day']['weather']['temperature']
        item['pd0_day_wind_direct'] = jsonData['detail'][0]['day']['wind']['direct']
        item['pd0_day_wind_power'] = jsonData['detail'][0]['day']['wind']['power']
        item['pd0_night_weather_info'] = jsonData['detail'][0]['night']['weather']['info']
        item['pd0_night_weather_temperature'] = jsonData['detail'][0]['night']['weather']['temperature']
        item['pd0_night_wind_direct'] = jsonData['detail'][0]['night']['wind']['direct']
        item['pd0_night_wind_power'] = jsonData['detail'][0]['night']['wind']['power']
        item['perdict_date_1'] = jsonData['detail'][1]['date']
        item['pd1_day_weather_info'] = jsonData['detail'][1]['day']['weather']['info']
        item['pd1_day_weather_temperature'] = jsonData['detail'][1]['day']['weather']['temperature']
        item['pd1_day_wind_direct'] = jsonData['detail'][1]['day']['wind']['direct']
        item['pd1_day_wind_power'] = jsonData['detail'][1]['day']['wind']['power']
        item['pd1_night_weather_info'] = jsonData['detail'][1]['night']['weather']['info']
        item['pd1_night_weather_temperature'] = jsonData['detail'][1]['night']['weather']['temperature']
        item['pd1_night_wind_direct'] = jsonData['detail'][1]['night']['wind']['direct']
        item['pd1_night_wind_power1'] = jsonData['detail'][1]['night']['wind']['power']

        yield item

    @staticmethod
    def init_city_urls():
        '''初始化city_urls的字典，供start_urls使用'''
        url_real_prefix = "http://www.nmc.cn/f/rest/weather/"

        with open('city_list_nmc.txt', 'r', encoding='utf8') as f:
            for line in f:
                v = line.strip().split(' ')
                url = url_real_prefix + v[0]
                Nmcspider2dSpider.city_url_dicts[v[1]] = url
