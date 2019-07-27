# -*- coding: utf-8 -*-
import scrapy
from tutorial.items import NMCItem14D
import json
from tutorial.UpdateProxyIPPool import UpdateProxyIPPool

class Nmcspider14dSpider(scrapy.Spider):
    city_url_dicts = {}

    name = 'NMCSpider14d'
    allowed_domains = ['www.nmc.cn']
    start_urls = []

    def __init__(self):
        super().__init__()
        # 初始化start_urls
        Nmcspider14dSpider.init_city_urls()
        Nmcspider14dSpider.start_urls = Nmcspider14dSpider.city_url_dicts.values()
        self.pool = UpdateProxyIPPool(4)
        self.pool.main()
        self.pool.savetxt('procies_list.txt')

    def parse(self, response):
        if response.status != 200:
            print('error! code:', response.status)
            return

        # 首先实例化item对象
        item = NMCItem14D()
        #item2bind = []
        jsonData = json.loads(response.body_as_unicode())

        for jdata in jsonData:

            item.clear()
            item['station_code'] = response.url.split('/')[-1]
            item['min_temp'] = jdata['minTemp']
            item['max_temp'] = jdata['maxTemp']
            item['perdict_date'] = jdata['realTime']

            #item2bind.append(item)

            yield item

    @staticmethod
    def init_city_urls():
        '''初始化city_urls的字典，供start_urls使用'''
        url_real_prefix = "http://www.nmc.cn/f/rest/tempchart/"

        with open('city_list_nmc.txt', 'r', encoding='utf8') as f:
            for line in f:
                v = line.strip().split(' ')
                url = url_real_prefix + v[0]
                Nmcspider14dSpider.city_url_dicts[v[1]] = url
