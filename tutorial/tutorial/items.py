# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class NMCItemReal(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    record_time = scrapy.Field()
    publish_time = scrapy.Field()
    station_code = scrapy.Field()
    weather_temperature = scrapy.Field()
    weather_airpressure = scrapy.Field()
    weather_humidity = scrapy.Field()
    weather_rain = scrapy.Field()
    weather_info = scrapy.Field()
    wind_direct = scrapy.Field()
    wind_power = scrapy.Field()


class NMCItem2D(scrapy.Item):
    record_time = scrapy.Field()
    publish_time = scrapy.Field()
    station_code = scrapy.Field()
    temperature = scrapy.Field()
    perdict_date_0 = scrapy.Field()
    pd0_day_weather_info = scrapy.Field()
    pd0_day_weather_temperature = scrapy.Field()
    pd0_day_wind_direct = scrapy.Field()
    pd0_day_wind_power = scrapy.Field()
    pd0_night_weather_info = scrapy.Field()
    pd0_night_weather_temperature = scrapy.Field()
    pd0_night_wind_direct = scrapy.Field()
    pd0_night_wind_power = scrapy.Field()
    perdict_date_1 = scrapy.Field()
    pd1_day_weather_info = scrapy.Field()
    pd1_day_weather_temperature = scrapy.Field()
    pd1_day_wind_direct = scrapy.Field()
    pd1_day_wind_power = scrapy.Field()
    pd1_night_weather_info = scrapy.Field()
    pd1_night_weather_temperature = scrapy.Field()
    pd1_night_wind_direct = scrapy.Field()
    pd1_night_wind_power1 = scrapy.Field()


class NMCItem14D(scrapy.Item):
    record_time = scrapy.Field()
    min_temp = scrapy.Field()
    max_temp = scrapy.Field()
    perdict_date = scrapy.Field()
    station_code = scrapy.Field()
    # item = []
    # for i in range(14):
    #     item.append([record_time, min_temp, max_temp, perdict_date, station_code])

