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
API = 'https://free-api.heweather.net/s6/weather/now?key=69cb483f814047f5825211916dab44e0&location={}'

def csv_city_list_to_sql():
    '''将csv表存在数据库中，之后就不必执行该函数了。该函数需要用到pandas/sqlalchemy/pymysql库'''
    # df = pd.read_csv('heweather-city-list-use-big.csv')
    df = pd.read_csv('{}.csv')
    engine = create_engine("mysql+pymysql://root:xn410a@172.21.14.238/zyt_spiders?charset=utf8")
    conn = engine.connect()
    # df.to_sql(name="city_list_big", con=conn, if_exists='replace', index=False)
    df.to_sql(name="city_list_small", con=conn, if_exists='replace', index=False)



def get_locations(cursor):
    sql = 'select City_ID from city_list;'
    cursor.execute(sql)
    result = cursor.fetchall()
    return result

def fetchWeather(city_code):
    url = API.format(city_code)
    result = requests.get(url, headers=headers, timeout=5)
    return result.text

# 建表
def create_table_heweather_real(cursor):
    sql = '''create table if not exists heweather_real (
                    id int primary key auto_increment not null,
                    record_time datetime not null,
    				update_time datetime,
                    location_code char(11),
                    cond_txt varchar(8),
    				pcpn float,
    				tmp float,
    				wind_deg int,
    				wind_sc varchar(5),
    				unique key (update_time, location_code)
    			);'''
    cursor.execute(sql)

# 无重复插入
def insert_item_to_heweather_real(cursor, **item):
    '''输入数据库游标和信息字典。向数据库中插入一项数据'''
    sql = '''insert ignore into `heweather_real`(
        `record_time`, `update_time`, `location_code`, 
    	`cond_txt`, `pcpn`, `tmp`, `wind_deg`, `wind_sc`) 
        values(now(), '{update_time}', '{location_code}', 
    	'{cond_txt}', '{pcpn}', '{tmp}', '{wind_deg}', '{wind_sc}');
        '''
    # print(sql.format(**item))
    cursor.execute(sql.format(**item))

def get_json_weather_data(cursor, city_code):
    '''获取城市编码对应的城市的实时天气'''
    try:
        test = fetchWeather(city_code)
        jsonData = json.loads(test)
        item = {}
        item['location_code'] = city_code
        item['update_time'] = jsonData['HeWeather6'][0]['update']['loc']  # 更新时间
        item['cond_txt'] = jsonData['HeWeather6'][0]['now']['cond_txt']  # 天气描述
        item['pcpn'] = jsonData['HeWeather6'][0]['now']['pcpn']  # 降水量
        item['tmp'] = jsonData['HeWeather6'][0]['now']['tmp']  # 温度
        item['wind_deg'] = jsonData['HeWeather6'][0]['now']['wind_deg']  # 风向，360度
        item['wind_sc'] = jsonData['HeWeather6'][0]['now']['wind_sc']  # 风力，3-4
        insert_item_to_heweather_real(cursor, **item)

    except TypeError as e:
        print("获取{}天气状况数据出现URLERROR！".format(city_code), e)

    except Exception as e:
        print("获取{}天气状况数据出现未知异常，异常如下：{}".format(city_code), e)
        # time.sleep(10)
        # #出现异常则过一段时间重新执行此部分
        # get_json_weather_data(cursor, city_code)


def main():
    db = pymysql.connect(host='172.21.14.238', user='root', passwd='xn410a', db='zyt_spiders')
    cursor = db.cursor()
    result = get_locations(cursor)
    city_list = []
    for i in range(len(result)):
        city_list.append(result[i][0])
    # 第一次用要打开
    create_table_heweather_real(cursor)
    try:
        for city_code in city_list:
            get_json_weather_data(cursor, city_code)
            print('成功爬取{}城市的数据。'.format(city_code))
            db.commit()
    except:
        db.rollback()
    cursor.close()
    db.close()

if __name__ == '__main__':
    main()