import requests
import json
import pymysql
import time
import datetime
import csv
import pandas as pd
from sqlalchemy import create_engine

# 和风天气：实时天气
'''0.5h运行一次'''

# 定制请求头
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
}

# 可获得实时天气、2天内逐小时天气、15天天气预报
API = 'https://free-api.heweather.net/s6/weather/forecast?key=69cb483f814047f5825211916dab44e0&location={}'

def get_locations(cursor):
    sql = 'select City_ID,Latitude,Longitude from city_list;'
    cursor.execute(sql)
    result = cursor.fetchall()
    return result

def fetchWeather(city_tuple):
    url = API.format(str(city_tuple[1]) + "," + str(city_tuple[2]))
    result = requests.get(url, headers=headers, timeout=5)
    return result.text

# 建表
def create_table_heweather_3d(cursor):
    sql = '''create table if not exists heweather_3d (
                    id int primary key auto_increment not null,
                    record_time datetime not null,
    				update_time datetime,
                    location_code char(11),
                    fore_date date,
                    cond_txt_d varchar(8),
                    cond_txt_n varchar(8),
    				pcpn float,
    				pop float,
    				tmp_max float,
    				tmp_min float,
    				wind_deg int,
    				wind_sc varchar(5),
    				unique key (update_time, location_code, fore_date)
    			);'''
    cursor.execute(sql)

# 无重复插入
def insert_item_to_heweather_3d(cursor, **item):
    '''输入数据库游标和信息字典。向数据库中插入一项数据'''
    sql = '''insert ignore into `heweather_3d`(
        `record_time`, `update_time`, `location_code`, `fore_date`,
    	`cond_txt_d`, `cond_txt_n`, `pcpn`, `pop`, `tmp_max`, `tmp_min`, `wind_deg`, `wind_sc`) 
        values(now(), '{update_time}', '{location_code}', '{fore_date}',
    	'{cond_txt_d}', '{cond_txt_n}', '{pcpn}', '{pop}', '{tmp_max}', '{tmp_min}', '{wind_deg}', '{wind_sc}');
        '''
    # print(sql.format(**item))
    cursor.execute(sql.format(**item))

def get_json_weather_data(cursor, city_tuple):
    '''获取城市编码对应的城市的实时天气'''
    try:
        test = fetchWeather(city_tuple)
        jsonData = json.loads(test)
        item = {}
        item['location_code'] = city_tuple[0]
        item['update_time'] = jsonData['HeWeather6'][0]['update']['loc']  # 更新时间

        for i in range(3):
            item['fore_date'] = jsonData['HeWeather6'][0]['daily_forecast'][i]['date']  # 预测时刻
            item['cond_txt_d'] = jsonData['HeWeather6'][0]['daily_forecast'][i]['cond_txt_d']  # 白天天气描述
            item['cond_txt_n'] = jsonData['HeWeather6'][0]['daily_forecast'][i]['cond_txt_n']  # 晚上天气描述
            item['pcpn'] = jsonData['HeWeather6'][0]['daily_forecast'][i]['pcpn']  # 降水量
            item['pop'] = jsonData['HeWeather6'][0]['daily_forecast'][i]['pop']  # 降水概率
            item['tmp_max'] = jsonData['HeWeather6'][0]['daily_forecast'][i]['tmp_max']  # 最高温度
            item['tmp_min'] = jsonData['HeWeather6'][0]['daily_forecast'][i]['tmp_min']  # 最低温度
            item['wind_deg'] = jsonData['HeWeather6'][0]['daily_forecast'][i]['wind_deg']  # 风向，360度
            item['wind_sc'] = jsonData['HeWeather6'][0]['daily_forecast'][i]['wind_sc']  # 风力，3-4
            insert_item_to_heweather_3d(cursor, **item)

    except TypeError as e:
        print("获取{}天气状况数据出现URLERROR！".format(city_tuple[0]), e)
        print("location_id={}, city_coord=({}, {})".format(cityTuple[0], cityTuple[1], cityTuple[2]))

    except Exception as e:
        print("获取{}天气状况数据出现未知异常，异常如下：{}".format(city_tuple[0]), e)
        print("location_id={}, city_coord=({}, {})".format(cityTuple[0], cityTuple[1], cityTuple[2]))

def main():
    db = pymysql.connect(host='172.21.14.238', user='root', passwd='xn410a', db='zyt_spiders')
    cursor = db.cursor()


    # 第一次用要打开
    create_table_heweather_3d(cursor)
    city_list = get_locations(cursor)
    try:
        for city_tuple in city_list:
            get_json_weather_data(cursor, city_tuple)
            print('成功爬取{}城市的数据。'.format(city_tuple[0]))
            db.commit()
    except:
        db.rollback()
    cursor.close()
    db.close()

if __name__ == '__main__':
    main()