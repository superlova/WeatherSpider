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
# 5 days/3 hour forecast API

# 定制请求头
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
}

# 输入纬度、经度，输出实时信息
API = 'http://api.openweathermap.org/data/2.5/forecast?lat={}&lon={}&APPID=e3f403e4977492e96d4882b8d8372666&lang=zh_cn&units=metric'

def get_locations(cursor):
    # 从公共城市数据库中取出经纬度
    sql = 'select City_ID,Latitude,Longitude from city_list_big union select City_ID,Latitude,Longitude from city_list_small;'
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
def create_table_openweather_5d(cursor):
    sql = '''create table if not exists openweather_5d (
                    id int primary key auto_increment not null,
                    record_time datetime not null,
    				fore_date datetime,
                    location_code char(11),
                    weather_id char(3),
                    weather_text varchar(20),
    				temp float,
    				temp_max float,
    				temp_min float,
    				pcpn_3h float,
    				wind_deg float,
    				wind_speed float
    			);'''
    cursor.execute(sql)

# 无重复插入
def insert_item_to_openweather_5d(cursor, **item):
    '''输入数据库游标和信息字典。向数据库中插入一项数据'''
    sql = '''insert ignore into `openweather_5d`(
        `record_time`, `fore_date`, `location_code`, `weather_id`,
    	`weather_text`, `temp`, `temp_max`, `temp_min`, `pcpn_3h`, `wind_deg`, `wind_speed`) 
        values(now(), '{fore_date}', '{location_code}', '{weather_id}',
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
        item['location_code'] = city_tuple[0]
        for i in range(40):
            fore_date = datetime.datetime.fromtimestamp(int(jsonData['list'][i]['dt']),
                                                          pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
            item['fore_date'] = fore_date
            item['temp'] = jsonData['list'][i]['main']['temp']
            item['temp_min'] = jsonData['list'][i]['main']['temp_min']
            item['temp_max'] = jsonData['list'][i]['main']['temp_max']
            item['weather_id'] = jsonData['list'][i]['weather'][0]['id']
            item['weather_text'] = jsonData['list'][i]['weather'][0]['description']
            item['wind_speed'] = jsonData['list'][i]['wind']['speed']
            item['wind_deg'] = jsonData['list'][i]['wind']['deg']
            item['pcpn_3h'] = 0.0
            if jsonData['list'][i].__contains__('rain'):
                if jsonData['list'][i]['rain'].__contains__('3h'):
                    item['pcpn_3h'] = jsonData['list'][i]['rain']['3h']
            if jsonData['list'][i].__contains__('snow'):
                if jsonData['list'][i]['snow'].__contains__('3h'):
                    item['pcpn_3h'] = jsonData['list'][i]['snow']['3h']

            insert_item_to_openweather_5d(cursor, **item)


    except TypeError as e:
        print("获取{}天气状况数据出现URLERROR！".format(city_tuple[0]), e)

    except Exception as e:
        print("获取{}天气状况数据出现未知异常，异常如下：{}".format(city_tuple[0]), e)

def main():
    db = pymysql.connect(host='172.21.14.238', user='root', passwd='xn410a', db='zyt_spiders')
    cursor = db.cursor()
    result = get_locations(cursor)

    # 第一次用要打开
    create_table_openweather_5d(cursor)
    try:
        for city_tuple in result:
            get_json_weather_data(cursor, city_tuple)
            print('成功爬取{}城市的数据。'.format(city_tuple[0]))
            db.commit()
    except Exception as e:
        print(e)
        db.rollback()
    cursor.close()
    db.close()

if __name__ == '__main__':
    main()