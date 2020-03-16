import pymysql

def connect_sql():
    conn = pymysql.connect(host='localhost', user='root', password='xn410a', db='zyt_spiders', charset='utf8')
    return conn

def delete_old_items(conn, table_name, time):
    cursor = conn.cursor()
    sql = 'delete from {} where record_time < "{}";'.format(table_name, time)
    #print(sql)
    cursor.execute(sql)
    sql_optimize = 'optimize table {}'.format(table_name)
    cursor.execute(sql_optimize)


if __name__ == "__main__":
    conn = connect_sql()

    table_name_list = ["caiyun_15d_weather",
                       "caiyun_48h_weather",
                       "caiyun_real_weather",
                       "heweather_3d",
                       "heweather_real",
                       "nmc_2d_weather",
                       "nmc_real_weather",
                       "openweather_5d",
                       "openweather_real",
                       "seniverse_15d_weather",
                       "seniverse_real_weather"]
    for table_name in table_name_list:
        delete_old_items(conn=conn, table_name=table_name, time="2019-12-14 00:00:00")
        print(table_name + " is cleaned.")
