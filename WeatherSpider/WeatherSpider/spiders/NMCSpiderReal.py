# -*- coding: utf-8 -*-
import scrapy

from WeatherSpider.items import NMCItemReal
import json
import pymysql

class NmcspiderrealSpider(scrapy.Spider):
    name = 'NMCSpiderReal'
    allowed_domains = ['nmc.cn']
    start_urls = []

    def __init__(self):
        
        super().__init__()
        # 初始化start_urls
        self.url = []
        NmcspiderrealSpider.start_urls = NmcspiderrealSpider.get_nmc_city_code()

    def parse(self, response):
        if response.status == 200:
            

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
        else:
            print('error! code:' , response.status)
    
    @staticmethod
    def get_nmc_city_code():
        '''从数据库中到处城市列表'''
        db = pymysql.connect(host='172.21.14.238', user='root', passwd='xn410a', db='zyt_spiders')
        cursor = db.cursor()
        sql = 'select NMC_Code from city_list;'
        cursor.execute(sql)
        
        result = cursor.fetchall()
        url_real_prefix = "http://www.nmc.cn/f/rest/real/{}"
 
        for code in result:
            self.url.append(url_real_prefix.format(code)
        
        cursor.close()
        db.close()
        
        return url