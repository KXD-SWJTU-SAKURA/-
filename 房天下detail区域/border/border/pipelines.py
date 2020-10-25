# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import time

import pymysql
from scrapy.exceptions import DropItem


class BorderPipeline:
    def __init__(self, host, database, user, password, port):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port

    @classmethod
    def from_crawler(cls, crawler):
        return cls(host=crawler.settings.get('MYSQL_HOST'),
                   database=crawler.settings.get('MYSQL_DATABASE'),
                   user=crawler.settings.get('MYSQL_USER'),
                   password=crawler.settings.get('MYSQL_PASSWORD'),
                   port=crawler.settings.get('MYSQL_PORT'),
                   )

    def open_spider(self, spider):
        self.db = pymysql.connect(self.host, self.user, self.password, self.database, charset='utf8', port=self.port)
        self.cursor = self.db.cursor()

    def close_spider(self, spider):
        self.db.close()



    def insert_data(self, item):
        try:
            data = dict(item)
            keys = ', '.join(data.keys())
            values = ', '.join(['% s'] * len(data))
            sql = 'insert into % s (% s) values (% s)' % ('areadistrict_detail_info', keys, values)
            print('sql =', sql)
            print('data =', tuple(data.values()))
            self.cursor.execute(sql, tuple(data.values()))
            self.db.commit()

        except Exception as err:
            print(err)
            log_file = open('log.txt', 'a')
            regDate = time.strftime("%Y-%m-%d %X", time.localtime(time.time()))
            log_file.write(regDate + ":" + str(err) + "------" "\n")
            log_file.close()

    def refresh_data(self, item):
        try:
            sql = "select * from areadistrict_detail_info where county = %s and areaName = %s"
            print('pipiline_sql', sql)
            self.cursor.execute(sql, [item['county'],item['areaName']])
            if self.cursor.fetchone():  # 如果数据库存在这条数据，则不插入
                return DropItem('数据已存在')
            else:
                self.insert_data(item)

        except Exception as err:
            print(err)
            log_file = open('log.txt', 'a')
            regDate = time.strftime("%Y-%m-%d %X", time.localtime(time.time()))
            log_file.write(regDate + ":" + str(err) + "------" "\n")
            log_file.close()

    def process_item(self, item, spider):
        # self.insert_data(item)
        self.refresh_data(item)

        return item


