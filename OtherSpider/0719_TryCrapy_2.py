import requests
import json
import pymysql
import time

'''8h运行一次'''
# 心知天气

KEY = 'SPKoKjIxGUayMeSyE'  # API key
UID = "PPytfST60CNf-MoBr"  # 用户ID

LOCATION = 'beijing'  # 所查询的位置，可以使用城市拼音、v3 ID、经纬度等
API = 'https://api.seniverse.com/v3/weather/daily.json?key=SPKoKjIxGUayMeSyE&language=zh-Hans&unit=c&location={}:{}'  # API URL，可替换为其他 URL
UNIT = 'c'  # 单位
LANGUAGE = 'zh-Hans'  # 查询结果的返回语言

# def get_locations():
#     '''初始化city_urls的字典，供start_urls使用'''
#     city_dict = {}
#     with open('city_list.txt', 'r', encoding='utf8') as f:
#
#         for line in f:
#             v = line.strip().split('\t')
#             city_dict[v[2]] = v[0]
#     return city_dict

def get_location(cursor):
    sql = 'select City_ID,Latitude,Longitude from city_list;'
    cursor.execute(sql)
    result = cursor.fetchall()
    return result

def fetchWeather(city_tuple):
    result = requests.get(API.format(city_tuple[1], city_tuple[2]), timeout=1)
    return result.text

# 建表
def create_table_weather(cursor):
    sql = '''create table if not exists seniverse_15d_weather (
                id int primary key auto_increment not null,
                record_time datetime not null,
                location_id char(12),

                predict_date_0 date,
                weather_code_day_0 tinyint,
    			weather_code_night_0 tinyint,
                maxTemp_0 float,
    			minTemp_0 float,

                predict_date_1 date,
    			weather_code_day_1 tinyint,
                weather_code_night_1 tinyint,
    			maxTemp_1 float,
                minTemp_1 float,

    			predict_date_2 date,
                weather_code_day_2 tinyint,
    			weather_code_night_2 tinyint,
                maxTemp_2 float,
    			minTemp_2 float,

                last_update char(25),
                unique key (location_id, last_update)
            );'''

    cursor.execute(sql)

# 无重复插入
def insert_item_to_table(cursor, **item):
    '''输入数据库游标和信息字典。向数据库中插入一项数据'''
    sql = '''insert ignore into `seniverse_15d_weather`(
        `record_time`, `location_id`, 
    	`predict_date_0`, `weather_code_day_0`, `weather_code_night_0`, `maxTemp_0`, `minTemp_0`, 
    	`predict_date_1`, `weather_code_day_1`, `weather_code_night_1`, `maxTemp_1`, `minTemp_1`, 
    	`predict_date_2`, `weather_code_day_2`, `weather_code_night_2`, `maxTemp_2`, `minTemp_2`,
    	`last_update`) 
        values(now(), '{location_id}', 
    	'{predict_date_0}', '{weather_code_day_0}', '{weather_code_night_0}', '{maxTemp_0}', '{minTemp_0}', 
    	'{predict_date_1}', '{weather_code_day_1}', '{weather_code_night_1}', '{maxTemp_1}', '{minTemp_1}', 
    	'{predict_date_2}', '{weather_code_day_2}', '{weather_code_night_2}', '{maxTemp_2}', '{minTemp_2}', 
    	'{last_update}');
        '''
    #print(sql.format(**item))
    cursor.execute(sql.format(**item))

def get_json_weather_data(cursor, city_tuple):
    '''获取城市编码对应的城市的实时天气'''
    try:
        jsonData = json.loads(fetchWeather(city_tuple))
        item = {}
        item['location_id'] = city_tuple[0]  # location_id
        # item['location_id'] = jsonData['results'][0]['location']['id']  # location_id

        item['predict_date_0'] = jsonData['results'][0]['daily'][0]['date']  # predict_date_0
        item['weather_code_day_0'] = jsonData['results'][0]['daily'][0]['code_day']  # weather_code_day_0
        item['weather_code_night_0'] = jsonData['results'][0]['daily'][0]['code_night']  # weather_code_night_0
        item['maxTemp_0'] = jsonData['results'][0]['daily'][0]['high']  # maxTemp_0
        item['minTemp_0'] = jsonData['results'][0]['daily'][0]['low']  # minTemp_0

        item['predict_date_1'] = jsonData['results'][0]['daily'][1]['date']  # predict_date_1
        item['weather_code_day_1'] = jsonData['results'][0]['daily'][1]['code_day']  # weather_code_day_1
        item['weather_code_night_1'] = jsonData['results'][0]['daily'][1]['code_night']  # weather_code_night_1
        item['maxTemp_1'] = jsonData['results'][0]['daily'][1]['high']  # maxTemp_1
        item['minTemp_1'] = jsonData['results'][0]['daily'][1]['low']  # minTemp_1

        item['predict_date_2'] = jsonData['results'][0]['daily'][2]['date']  # predict_date_2
        item['weather_code_day_2'] = jsonData['results'][0]['daily'][2]['code_day']  # weather_code_day_2
        item['weather_code_night_2'] = jsonData['results'][0]['daily'][2]['code_night']  # weather_code_night_2
        item['maxTemp_2'] = jsonData['results'][0]['daily'][2]['high']  # maxTemp_2
        item['minTemp_2'] = jsonData['results'][0]['daily'][2]['low']  # minTemp_2

        item['last_update'] = jsonData['results'][0]['last_update']  # last_update

        insert_item_to_table(cursor=cursor, **item)

    except TypeError as e:
        print("获取{}天气状况数据出现URLERROR！".format(city_tuple[0]), e)
        print("location_id={}, city_coord=({}, {})".format(city_tuple[0], city_tuple[1], city_tuple[2]))

    except Exception as e:
        print("获取{}天气状况数据出现未知异常。异常如下：".format(city_tuple[0]), e)
        print("location_id={}, city_coord=({}, {})".format(city_tuple[0], city_tuple[1], city_tuple[2]))
        # time.sleep(10)
        # #出现异常则过一段时间重新执行此部分
        # get_json_weather_data(cursor, city_code)


def main():

    # jsonData = json.loads(getHTMLText('http://www.nmc.cn/f/rest/tempchart/54765'))
    # for jdata in jsonData:
    #     print(jdata['maxTemp'])

    # raw_list = get_locations()
    # code_list = raw_list.values()
    #print(type(code_list), "|", code_list)

    db = pymysql.connect(host='172.21.14.238', user='root', passwd='xn410a', db='zyt_spiders')
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

