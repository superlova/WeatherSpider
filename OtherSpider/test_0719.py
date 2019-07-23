# -*- coding: utf-8 -*-
import pandas as pd
import pymysql
from sqlalchemy import create_engine

def csv_city_list_to_sql(filename):
    '''将csv表存在数据库中，之后就不必执行该函数了。该函数需要用到pandas/sqlalchemy/pymysql库'''
    df = pd.read_csv(filename)
    engine = create_engine("mysql+pymysql://root:xn410a@localhost/zyt_spiders?charset=utf8")
    conn = engine.connect()
    df.to_sql(name="city_list", con=conn, if_exists='replace', index=False)

if __name__ == '__main__':
    csv_city_list_to_sql('city_list.csv')