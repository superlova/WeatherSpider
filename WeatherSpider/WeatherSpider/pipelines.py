# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


class WeatherspiderPipeline(object):

    def __init__(self):
        self.connect = pymysql.connect(
            host="172.21.14.238",
            db="zyt_spiders",
            user="root",
            passwd="xn410a",
            charset='utf8',
            use_unicode=True,
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.connect.cursor()
        self.cursor.execute('create database if not exists zyt_spiders ')
    
    # 建nmc_real_weather表
    def create_table_nmc_real_weather(self):
        sql = '''create table if not exists nmc_real_weather (
                            id int primary key auto_increment not null,
                            record_time datetime not null,
                            publish_time datetime not null,
                            station_code char(5) not null,
                            weather_temperature float,
                            weather_airpressure float,
                            weather_humidity float,
                            weather_rain float,
                            weather_info varchar(10),
                            wind_direct varchar(10),
                            wind_power varchar(10),
                            unique(publish_time, station_code)
                        );'''
        self.cursor.execute(sql)
    
    # 无重复插入nmc_real_weather
    def insert_item_to_nmc_real_weather(self, **item):
        '''输入数据库游标和信息字典。向数据库中插入一项数据'''
        sql = '''insert ignore into `nmc_real_weather`(`record_time`, 
        `publish_time`, `station_code`, 
        `weather_temperature`, `weather_airpressure`, 
        `weather_humidity`, `weather_rain`, `weather_info`, 
        `wind_direct`, `wind_power`) 
        values (now(), '{publish_time}', '{station_code}', 
        '{weather_temperature}', '{weather_airpressure}', 
        '{weather_humidity}', '{weather_rain}', '{weather_info}', 
        '{wind_direct}', '{wind_power}');
        '''
        self.cursor.execute(sql.format(**item))
    
    def process_item(self, item, spider):
        if spider.name == 'NMCSpiderReal':
            self.create_table_nmc_real_weather()
            try:
                self.insert_item_to_nmc_real_weather(**item)
                # print('成功爬取{}城市的数据。'.format(city_code))
                self.connect.commit()
            except:
                self.connect.rollback()
        return item
    
    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()