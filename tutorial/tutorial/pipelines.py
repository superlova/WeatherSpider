# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql

class TutorialPipeline(object):

    def __init__(self):
        self.connect = pymysql.connect(
            host="localhost",
            db="zyt_spiders",
            user="root",
            passwd="xn410a",
            charset='utf8',
            use_unicode=True,
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.connect.cursor()

    # 建表
    def create_table_weather(self):
        self.cursor.execute('create database if not exists zyt_spiders;')
        self.connect.select_db('zyt_spiders')
        self.cursor.execute('show tables;')
        # 接受全部返回结果行
        tables = self.cursor.fetchall()
        findtables = False
        for table in tables:
            if 'nmc_real_weather' in table:
                findtables = True

        if not findtables:
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

    # 建立nmc_2d的表
    def create_table_nmc_2d(self):
        self.cursor.execute('create database if not exists zyt_spiders;')
        self.connect.select_db('zyt_spiders')
        self.cursor.execute('show tables;')
        # 接受全部返回结果行
        tables = self.cursor.fetchall()
        findtables = False
        for table in tables:
            if 'nmc_2d_weather' in table:
                findtables = True

        if not findtables:
            sql = '''create table if not exists nmc_2d_weather (
                        id int primary key auto_increment not null,
                        record_time datetime not null,
                        publish_time datetime not null,
                        station_code char(5) not null,
                        temperature float,
                        perdict_date_0 datetime,
                        pd0_day_weather_info varchar(10),
                        pd0_day_weather_temperature float,
                        pd0_day_wind_direct varchar(10),
                        pd0_day_wind_power varchar(10),
                        pd0_night_weather_info varchar(10),
                        pd0_night_weather_temperature float,
                        pd0_night_wind_direct varchar(10),
                        pd0_night_wind_power varchar(10),
                        perdict_date_1 datetime,
                        pd1_day_weather_info varchar(10),
                        pd1_day_weather_temperature float,
                        pd1_day_wind_direct varchar(10),
                        pd1_day_wind_power varchar(10),
                        pd1_night_weather_info varchar(10),
                        pd1_night_weather_temperature float,
                        pd1_night_wind_direct varchar(10),
                        pd1_night_wind_power1 varchar(10),
                        unique(publish_time, station_code)
                    );'''
            self.cursor.execute(sql)

        # 建立nmc_14d的表
        def create_table_nmc_14d(self):
            self.cursor.execute('create database if not exists zyt_spiders;')
            self.connect.select_db('zyt_spiders')
            self.cursor.execute('show tables;')
            # 接受全部返回结果行
            tables = self.cursor.fetchall()
            findtables = False
            for table in tables:
                if 'nmc_14d_weather' in table:
                    findtables = True

            if not findtables:
                sql = '''create table if not exists nmc_14d_weather (
                            id int primary key auto_increment not null,
                            record_time datetime not null,
                            min_temp float,
                            max_temp float,
                            perdict_date date,
                            station_code char(5)
                        );'''
                self.cursor.execute(sql)

    # 无重复插入
    def insert_item_to_table(self, **item):
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
        # print(sql.format(**item))
        self.cursor.execute(sql.format(**item))

    # nmc 2d
    def insert_NMCItem2D_to_table(self, **item):
        '''输入数据库游标和信息字典。向数据库中插入一项数据'''
        sql = '''insert ignore into `nmc_2d_weather`(
            `record_time`, `publish_time`, `station_code`, `temperature`, `perdict_date_0`, 
            `pd0_day_weather_info`, `pd0_day_weather_temperature`, `pd0_day_wind_direct`, 
            `pd0_day_wind_power`, `pd0_night_weather_info`, `pd0_night_weather_temperature`, 
            `pd0_night_wind_direct`, `pd0_night_wind_power`, `perdict_date_1`, `pd1_day_weather_info`, 
            `pd1_day_weather_temperature`, `pd1_day_wind_direct`, `pd1_day_wind_power`, 
            `pd1_night_weather_info`, `pd1_night_weather_temperature`, `pd1_night_wind_direct`, 
            `pd1_night_wind_power1`) values(now(), '{publish_time}', '{station_code}', '{temperature}', '{perdict_date_0}', 
            '{pd0_day_weather_info}', '{pd0_day_weather_temperature}', '{pd0_day_wind_direct}', 
            '{pd0_day_wind_power}', '{pd0_night_weather_info}', '{pd0_night_weather_temperature}', 
            '{pd0_night_wind_direct}', '{pd0_night_wind_power}', '{perdict_date_1}', '{pd1_day_weather_info}', 
            '{pd1_day_weather_temperature}', '{pd1_day_wind_direct}', '{pd1_day_wind_power}', 
            '{pd1_night_weather_info}', '{pd1_night_weather_temperature}', '{pd1_night_wind_direct}', 
            '{pd1_night_wind_power1}');
            '''
        # print(sql.format(**item))
        self.cursor.execute(sql.format(**item))

    # nmc 14d
    def insert_NMCItem14D_to_table(self, **item):
        '''输入数据库游标和信息字典。向数据库中插入一项数据'''
        sql = '''insert ignore into `nmc_14d_weather`(
            `record_time`, `min_temp`, `max_temp`, `perdict_date`, `station_code`) 
            values(now(), '{min_temp}', '{max_temp}', '{perdict_date}', '{station_code}');
            '''
        # print(sql.format(**item))
        self.cursor.execute(sql.format(**item))

    def process_item(self, item, spider):
        if spider.name == 'NMCSpider':
            self.create_table_weather()
            try:
                self.insert_item_to_table(**item)
                # print('成功爬取{}城市的数据。'.format(city_code))
                self.connect.commit()
            except:
                self.connect.rollback()
        elif spider.name == 'NMCSpider2d':
            self.create_table_nmc_2d()
            try:
                self.insert_NMCItem2D_to_table(**item)
                # print('成功爬取{}城市的数据。'.format(city_code))
                self.connect.commit()
            except:
                self.connect.rollback()
        elif spider.name == 'NMCSpider14d':
            self.create_table_nmc_2d()
            try:
                self.insert_NMCItem14D_to_table(**item[0])
                self.insert_NMCItem14D_to_table(**item[1])
                # print('成功爬取{}城市的数据。'.format(city_code))
                self.connect.commit()
            except:
                self.connect.rollback()
        # else:

        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()
