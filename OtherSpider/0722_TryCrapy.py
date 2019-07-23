import datetime
import pytz
import requests
import pymysql
import json

# OpenWeather API
# Weather API data update	< 2 hours
# Threshold: 7,200
# Hourly forecast: 5
# Daily forecast: 0
# Calls 1min: 60
# Current weather API

# 定制请求头
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
}

# 输入纬度、经度，输出实时信息
API = 'http://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&APPID=e3f403e4977492e96d4882b8d8372666&lang=zh_cn&units=metric'

def get_locations(cursor):
    # 从公共城市数据库中取出经纬度
    sql = 'select City_ID,Latitude,Longitude from city_list;'
    cursor.execute(sql)
    result = cursor.fetchall()
    # 返回元组类型的数据
    return result

def fetchWeather(city_tuple):
    '''输入纬度经度，输出json文本'''
    url = API.format(city_tuple[1], city_tuple[2])
    result = requests.get(url, headers=headers, timeout=5)
    return result.text

# 建表
def create_table_openweather_real(cursor):
    sql = '''create table if not exists openweather_real (
                    id int primary key auto_increment not null,
                    record_time datetime not null,
                    update_time datetime,
                    location_code char(11),
                    weather_id char(3),
                    weather_text varchar(20),
    				temp float,
    				temp_max float,
    				temp_min float,
    				pcpn_3h float,
    				wind_deg float,
    				wind_speed float,
    				unique key (update_time, location_code)
    			);'''
    cursor.execute(sql)

# 无重复插入
def insert_item_to_openweather_real(cursor, **item):
    '''输入数据库游标和信息字典。向数据库中插入一项数据'''
    sql = '''insert ignore into `openweather_real`(
        `record_time`, `update_time`, `location_code`, `weather_id`,
    	`weather_text`, `temp`, `temp_max`, `temp_min`, `pcpn_3h`, 
    	`wind_deg`, `wind_speed`) 
        values(now(), '{update_time}', '{location_code}', '{weather_id}',
    	'{weather_text}', '{temp}', '{temp_max}', '{temp_min}', '{pcpn_3h}', '{wind_deg}', '{wind_speed}');
        '''
    # print(sql.format(**item))
    cursor.execute(sql.format(**item))

def get_json_weather_data(cursor, city_tuple):
    '''获取城市编码对应的城市的实时天气'''
    try:
        test = fetchWeather(city_tuple)
        jsonData = json.loads(test)
        item = {}

        update_time = datetime.datetime.fromtimestamp(int(jsonData['dt']),
                                                    pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')

        item['location_code'] = city_tuple[0]
        item['weather_id'] = jsonData['weather'][0]['id']
        item['weather_text'] = jsonData['weather'][0]['description']
        item['temp'] = jsonData['main']['temp']
        item['temp_min'] = jsonData['main']['temp_min']
        item['temp_max'] = jsonData['main']['temp_max']
        item['wind_speed'] = jsonData['wind']['speed']
        if jsonData['wind'].__contains__('deg'):
            item['wind_deg'] = jsonData['wind']['deg']
        else:
            item['wind_deg'] = 0.0
        item['update_time'] = update_time

        item['pcpn_3h'] = 0.0
        if jsonData.__contains__('rain'):
            item['pcpn_3h'] = jsonData['rain']['3h']
        if jsonData.__contains__('snow'):
            item['pcpn_3h'] = jsonData['rain']['3h']
        insert_item_to_openweather_real(cursor, **item)

    except TypeError as e:
        print("获取{}天气状况数据出现URLERROR！".format(city_tuple[0]), e)
        print("location_id={}, city_coord=({}, {})".format(city_tuple[0], city_tuple[1], city_tuple[2]))

    except Exception as e:
        print("获取{}天气状况数据出现未知异常，异常如下：{}".format(city_tuple[0]), e)
        print("location_id={}, city_coord=({}, {})".format(city_tuple[0], city_tuple[1], city_tuple[2]))

def main():
    db = pymysql.connect(host='172.21.14.238', user='root', passwd='xn410a', db='zyt_spiders')
    cursor = db.cursor()


    # 第一次用要打开
    create_table_openweather_real(cursor)
    result = get_locations(cursor)
    try:
        for city_tuple in result:
            get_json_weather_data(cursor, city_tuple)
            print('成功爬取{}城市的数据。'.format(city_tuple[0]))
            db.commit()
    except Exception as e:
        print("获取{}天气状况数据出现未知异常，异常如下：{}".format(city_tuple[0]), e)
        print("location_id={}, city_coord=({}, {})".format(city_tuple[0], city_tuple[1], city_tuple[2]))
        db.rollback()
    cursor.close()
    db.close()

if __name__ == '__main__':
    main()