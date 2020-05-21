#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import sys
import logging
import datetime
import time
import configparser
import inspect
import pymysql
import csv
import redis

# LIBS 路径
SCRIPTPATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LIBSPATH = SCRIPTPATH + '/libs/'
sys.path.insert(0, LIBSPATH)


# 定义
NOW = datetime.datetime.now().strftime('%Y%m%d')
NowDateTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
NOWTIME = int(time.time())
INCPATH = os.path.split(os.path.realpath(__file__))[0]
BASEPATH = os.path.abspath(os.path.join(INCPATH, os.path.pardir))
SCRIPTPATH = os.path.abspath(os.path.join(BASEPATH, os.path.pardir))
LOGPATH = BASEPATH + '/logs/'
DATAPATH = BASEPATH + '/data/'
SQLPATH = DATAPATH + '/sql/'

# 配置Production
MODE = 'Test'
cf = configparser.ConfigParser()
if os.path.isfile(INCPATH+'/config'+MODE+'.conf'):
    cf.read(INCPATH+'/config'+MODE+'.conf')
else:
    cf.read(INCPATH+'/config.conf')

# db
conn = pymysql.connect(
    host=cf.get('db', 'host'),
    port=cf.getint('db', 'port'),
    user=cf.get('db', 'user'),
    password=cf.get('db', 'pass'),
    database=cf.get('db', 'database'),
    charset=cf.get('db', 'charset'))

DB = conn.cursor()
DICT_DB = conn.cursor(cursor=pymysql.cursors.DictCursor)

# redis
redis_pool = redis.ConnectionPool(host=cf.get('redis', 'host'), port=cf.get('redis', 'port'), db=cf.get('redis', 'db'))
red = redis.Redis(connection_pool=redis_pool)


def get_current_function_name():
    return inspect.stack()[1][3]


def logInt(filename):
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename=filename,
                        filemode='a')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


def create_csv(filename,row_header,row_data):
    with open(DATAPATH + filename + '.csv', "w+", newline='',encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(row_header)
        writer.writerows(row_data)
        f.close()