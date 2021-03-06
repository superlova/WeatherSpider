# -*- coding: utf-8 -*-
import requests
import json
import pymysql
import time
import datetime
import pytz

# 彩云天气：实时天气
'''0.5h运行一次'''

# 定制请求头
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
}

# 可获得实时天气、2天内逐小时天气、15天天气预报
API = 'https://api.caiyunapp.com/v2/hodyCNI3UPJbGmtg/{}/weather?dailysteps=15'

LOCATION = '121.6544,25.1552'  # 所查询的位置，可以使用城市拼音、v3 ID、经纬度等


# def get_locations():
#     '''初始化city_urls的字典，供start_urls使用'''
#     city_dict = {}
#     with open('caiyun_city_list.txt', 'r', encoding='utf8') as f:
#         for line in f:
#             v = line.strip().split('\t')
#             city_dict[v[0]] = (v[-1], v[-2]) # 经、纬度
#     return city_dict

def get_locations(cursor):
    sql = 'select City_ID,Latitude,Longitude from city_list;'
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


def fetchWeather(city_tuple):
    url = API.format(str(city_tuple[2]) + "," + str(city_tuple[1]))
    result = requests.get(url, headers=headers, timeout=5)
    return result.text


# 建表
def create_table_caiyun_real_weather(cursor):
    sql = '''create table if not exists caiyun_real_weather (
                    id int primary key auto_increment not null,
                    record_time datetime not null,
    				server_time datetime,
                    location_code char(11),
                    temperature float,
    				precip_local_intensity float,
    				wind_direct float,
    				wind_speed float,
    				precip_description varchar(30),
    				weather_description varchar(30),
    				unique key (server_time, location_code)
    			);'''

    cursor.execute(sql)


# 建表
def create_table_caiyun_48h_weather(cursor):
    sql = '''create table if not exists caiyun_48h_weather (
                    id int primary key auto_increment not null,
                    record_time datetime not null,
    				server_time datetime,
    				forecast_time datetime,
                    location_code char(11),
                    temperature float,
    				precip_value float,
    				wind_direct float,
    				wind_speed float,
    				unique key (server_time, forecast_time, location_code)
    			);'''

    cursor.execute(sql)


# 建表
def create_table_caiyun_15d_weather(cursor):
    sql = '''create table if not exists caiyun_15d_weather (
                    id int primary key auto_increment not null,
                    record_time datetime not null,
    				server_time datetime,
    				forecast_time datetime,
                    location_code char(11),
                    temp_max float,
                    temp_min float,
                    temp_avg float,
    				precip_avg float,
    				wind_direct float,
    				wind_speed float,
    				unique key (server_time, forecast_time, location_code)
    			);'''

    cursor.execute(sql)


# 无重复插入
def insert_item_to_caiyun_real_weather(cursor, **item):
    '''输入数据库游标和信息字典。向数据库中插入一项数据'''
    sql = '''insert ignore into `caiyun_real_weather`(
        `record_time`, `server_time`, `location_code`, 
    	`temperature`, `precip_local_intensity`, `wind_direct`, `wind_speed`, `precip_description`, 
    	`weather_description`) 
        values(now(), '{server_time}', '{location_code}', 
    	'{temperature}', '{precip_local_intensity}', '{wind_direct}', '{wind_speed}', '{precip_description}', 
    	'{weather_description}');
        '''
    # print(sql.format(**item))
    cursor.execute(sql.format(**item))


# 无重复插入
def insert_item_to_caiyun_48h_weather(cursor, **item):
    '''输入数据库游标和信息字典。向数据库中插入一项数据'''
    sql = '''insert ignore into `caiyun_48h_weather`(
        `record_time`, `server_time`, `forecast_time`, `location_code`, 
    	`temperature`, `precip_value`, `wind_direct`, `wind_speed`) 
        values(now(), '{server_time}', '{forecast_time}', '{location_code}', 
    	'{temperature}', '{precip_value}', '{wind_direct}', '{wind_speed}');
        '''
    # print(sql.format(**item))
    cursor.execute(sql.format(**item))


# 无重复插入
def insert_item_to_caiyun_15d_weather(cursor, **item):
    '''输入数据库游标和信息字典。向数据库中插入一项数据'''
    sql = '''insert ignore into `caiyun_15d_weather`(
        `record_time`, `server_time`, `forecast_time`, `location_code`, 
    	`temp_max`, `temp_min`, `temp_avg`, `precip_avg`, `wind_direct`, `wind_speed`) 
        values(now(), '{server_time}', '{forecast_time}', '{location_code}', 
    	'{temp_max}', '{temp_min}', '{temp_avg}', '{precip_avg}', '{wind_direct}', '{wind_speed}');
        '''
    # print(sql.format(**item))
    cursor.execute(sql.format(**item))


def get_json_weather_data(cursor, city_tuple):
    '''获取城市编码对应的城市的实时天气'''
    try:
        jsonData = json.loads(fetchWeather(city_tuple))
        item1 = {}
        item2 = {}
        item3 = {}

        server_time = datetime.datetime.fromtimestamp(int(jsonData['server_time']),
                                                      pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')

        item1['server_time'] = server_time  # 服务器时间
        item1['location_code'] = city_tuple[0]  # 查询地址
        item1['temperature'] = jsonData['result']['realtime']['temperature']  # 实时温度
        item1['precip_local_intensity'] = jsonData['result']['realtime']['precipitation']['local'][
            'intensity']  # 本地实时降水强度
        item1['wind_direct'] = jsonData['result']['realtime']['wind']['direction']  # 实时风向
        item1['wind_speed'] = jsonData['result']['realtime']['wind']['speed']  # 实时风力
        item1['precip_description'] = jsonData['result']['forecast_keypoint']  # 降水预报文字描述
        item1['weather_description'] = jsonData['result']['hourly']['description']  # 天气综合文字描述

        insert_item_to_caiyun_real_weather(cursor, **item1)

        item2['server_time'] = server_time  # 服务器时间
        item2['location_code'] = city_tuple[0]  # 查询地址
        for i in range(48):
            item2['forecast_time'] = jsonData['result']['hourly']['temperature'][i]['datetime']  # 温度预报时刻
            item2['precip_value'] = jsonData['result']['hourly']['precipitation'][i]['value']  # 降水量
            item2['wind_direct'] = jsonData['result']['hourly']['wind'][i]['direction']  # 风向
            item2['wind_speed'] = jsonData['result']['hourly']['wind'][i]['speed']  # 风力
            item2['temperature'] = jsonData['result']['hourly']['temperature'][i]['value']  # 温度

            insert_item_to_caiyun_48h_weather(cursor, **item2)

        item3['server_time'] = server_time  # 服务器时间
        item3['location_code'] = city_tuple[0]  # 查询地址

        for i in range(15):
            item3['forecast_time'] = jsonData['result']['daily']['temperature'][i]['date']  # 逐日温度预报时刻
            item3['precip_avg'] = jsonData['result']['daily']['precipitation'][i]['avg']  # 逐日降水预报日均降水量
            item3['wind_direct'] = jsonData['result']['daily']['wind'][i]['avg']['direction']  # 逐日刮风预报平均风向
            item3['wind_speed'] = jsonData['result']['daily']['wind'][i]['avg']['speed']  # 逐日刮风预报平均风力
            item3['temp_max'] = jsonData['result']['daily']['temperature'][i]['max']  # 逐日温度预报最高温
            item3['temp_avg'] = jsonData['result']['daily']['temperature'][i]['avg']  # 逐日温度预报日均温
            item3['temp_min'] = jsonData['result']['daily']['temperature'][i]['min']  # 逐日温度预报最低温

            insert_item_to_caiyun_15d_weather(cursor, **item3)


    except TypeError as e:
        print("获取{}天气状况数据出现URLERROR！".format(city_code), e)
        print("location_id={}, city_coord=({}, {})".format(cityTuple[0], cityTuple[1], cityTuple[2]))

    except Exception as e:
        print("获取{}天气状况数据出现未知异常，异常如下{}：".format(city_code), e)
        print("location_id={}, city_coord=({}, {})".format(cityTuple[0], cityTuple[1], cityTuple[2]))
        # time.sleep(10)
        # #出现异常则过一段时间重新执行此部分
        # get_json_weather_data(cursor, city_code)


def main():
    # jsonData = json.loads(getHTMLText('http://www.nmc.cn/f/rest/tempchart/54765'))
    # for jdata in jsonData:
    #     print(jdata['maxTemp'])

    # raw_list = get_locations()
    # code_list = raw_list.values()
    # print(type(code_list), "|", code_list)
    # loca_dict = get_locations()
    # #loca_values = loca_dict.values()
    # code_list = list(loca_dict.keys())
    # loca_list = list(loca_values)
    # coordinate = str(loca_list[0][0]) + "," + str(loca_list[0][1])
    # print(loca_keys)

    db = pymysql.connect(host='localhost', user='root', passwd='xn410a', db='zyt_spiders')
    cursor = db.cursor()

    # 第一次用要打开
    create_table_caiyun_real_weather(cursor)
    create_table_caiyun_48h_weather(cursor)
    create_table_caiyun_15d_weather(cursor)

    city_list = get_locations(cursor)
    for city_tuple in city_list:
        try:
            get_json_weather_data(cursor, city_tuple)
            print('成功爬取{}城市的数据。'.format(city_tuple[0]))
            db.commit()
        except:
            print('爬取{}城市的数据失败。'.format(city_tuple[0]))
            db.rollback()
    cursor.close()
    db.close()


if __name__ == '__main__':
    main()




