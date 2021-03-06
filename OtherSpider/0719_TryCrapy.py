# -*- coding: utf-8 -*-
import requests
import json
import pymysql
import time

'''0.5h运行一次'''
# 心知天气api一分钟只能调用20次

KEY = 'SPKoKjIxGUayMeSyE'  # API key
UID = "PPytfST60CNf-MoBr"  # 用户ID

LOCATION = 'beijing'  # 所查询的位置，可以使用城市拼音、v3 ID、经纬度等
API = 'https://api.seniverse.com/v3/weather/now.json?key=SPKoKjIxGUayMeSyE&language=zh-Hans&unit=c&location={}:{}'  # API URL，可替换为其他 URL
UNIT = 'c'  # 单位
LANGUAGE = 'zh-Hans'  # 查询结果的返回语言


def get_location(cursor):
    sql = 'select City_ID,Latitude,Longitude from city_list;'
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


def fetchWeather(coordTuple):
    result = requests.get(API.format(str(coordTuple[1]), str(coordTuple[2])), timeout=1)
    return result.text

# 建表
def create_table_weather(cursor):

    sql = '''create table if not exists seniverse_real_weather (
            id int primary key auto_increment not null,
            record_time datetime not null,
            location_id char(12),
            temperature float,
            weather_code tinyint,
            last_update char(25),
            unique key (location_id, last_update)
        );'''

    cursor.execute(sql)

# 无重复插入
def insert_item_to_table(cursor, **item):
    '''输入数据库游标和信息字典。向数据库中插入一项数据'''
    sql = '''insert ignore into `seniverse_real_weather`(
    `record_time`, `location_id`, `temperature`, `weather_code`, `last_update`) 
    values(now(), '{location_id}', '{temperature}', '{weather_code}', '{last_update}');
    '''
    #print(sql.format(**item))
    cursor.execute(sql.format(**item))

def get_json_weather_data(cursor, cityTuple):
    '''获取城市编码对应的城市的实时天气'''
    try:
        jsonData = json.loads(fetchWeather(cityTuple))
        item = {}
        item['location_id'] = cityTuple[0]
        # item['location_id'] = jsonData['results'][0]['location']['id']
        item['temperature'] = jsonData['results'][0]['now']['temperature']
        item['weather_code'] = jsonData['results'][0]['now']['code']
        item['last_update'] = jsonData['results'][0]['last_update']
        insert_item_to_table(cursor=cursor, **item)

    except TypeError as e:
        print("获取{}天气状况数据出现URLERROR！".format(cityTuple[0]))
        print("location_id={}, city_coord=({}, {})".format(cityTuple[0], cityTuple[1], cityTuple[2]))

    except Exception as e:
        print("获取{}天气状况数据出现未知异常,错误{}".format(cityTuple[0], e))
        print("location_id={}, city_coord=({}, {})".format(cityTuple[0], cityTuple[1], cityTuple[2]))
        # time.sleep(10)
        # #出现异常则过一段时间重新执行此部分
        # get_json_weather_data(cursor, city_code)


def main():


    db = pymysql.connect(host='localhost', user='root', passwd='xn410a', db='zyt_spiders')
    cursor = db.cursor()

    # 第一次用要打开
    create_table_weather(cursor)
    city_list = get_location(cursor)
    try:
        for city_tuple in city_list:
            get_json_weather_data(cursor, city_tuple)
            print('成功爬取{}城市的数据。'.format(city_tuple[0]))
            time.sleep(3)
            db.commit()
    except:
        db.rollback()
    cursor.close()
    db.close()


if __name__ == '__main__':
    main()