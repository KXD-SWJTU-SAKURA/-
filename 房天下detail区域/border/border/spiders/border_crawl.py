# -*- coding: utf-8 -*-
import json
import scrapy
from border.items import BorderItem
import pymysql


class BorderCrawlSpider(scrapy.Spider):
    name = 'border_crawl'
    allowed_domains = ['kxd.com']
    start_urls = ['https://map.fang.com/ajaxSearch.html?city=tj']

    def start_requests(self):
        city_list = self.getCityIdDict()
        for city in city_list:
            url = 'https://map.fang.com/ajaxSearch.html?city=' + city['cityID'] + '&type=xf,esf,zf&xfPrice=&esfPrice=&zfPrice=&room=&area=&purpose=0&keyword=&zoom=12&newcodeType=&newcode=&pagingType=&pagingNum=1&x1=121.31568300078244&x2=121.46048984899089&y1=31.100128362057884&y2=31.137535651709584'
            yield scrapy.Request(url, callback=self.districts, dont_filter=True,meta ={'province': city['province'], 'city': city['city'], 'cityID': city['cityID'],})

    def getCityIdDict(self):
        db = pymysql.connect(
            host='47.108.198.155',
            port=3306,
            user='root',
            password='12345678',
            database='area_border_kxd',
            charset='utf8'
        )
        city_list = []
        for i in range(0, 653):
            cur = db.cursor()
            cur.execute("select city_url,city_name,city_short_name from fangtianxia_city_list")
            data0 = cur.fetchall()[i]
            city_dict = {
            'cityID': data0[0],
            'province': data0[1],
            'city': data0[2]
            }
            city_list.append(city_dict)
        print(city_list)
        return city_list



    def districts(self, response):
        province = response.meta['province']
        city = response.meta['city']
        cityID = response.meta['cityID']
        res = json.loads(response.text)
        # print(res)

        #遍历某一城市下的所有区域
        for district in res['data']['dist']:
            county = district['district']
            print(county)
            if len(district['baiducoord'])>=1:
              positions = district['baiducoord'].split(";")
            else:
                continue
            print(positions)
            lngs = []
            lats = []
            for position in positions:
                lngs.append (float(position.split(",")[0]))
                lats.append (float(position.split(",")[1]))
                # print(position)
                # print(lngs)
                # print(lats)

            #获取district的边界经纬度信息
            maxLongitude = max(lngs) + 0.1
            minLongitude = min(lngs) - 0.1
            maxLatitude = max(lats) + 0.1
            minLatitude = min(lats) - 0.1

            #对district进行分区获取该分区下的bizcircle的边界经纬度集合
            # curMaxLng = maxLongitude
            curMinLng = minLongitude
            while(curMinLng < maxLongitude):
                curMaxLat = maxLatitude
                while(curMaxLat > minLatitude):
                    print(curMaxLat)
                    print(curMinLng)
                    yield scrapy.Request('https://map.fang.com/ajaxSearch.html?city=' + cityID + '&type=xf,esf,zf&xfPrice=&esfPrice=&zfPrice=&room=&area=&purpose=0&keyword=&zoom=15&newcodeType=&newcode=&pagingType=&pagingNum=1' +
                                         '&x1=' + str(curMinLng) +
                                         '&x2=' + str(curMinLng + 0.1) +
                                         '&y1=' + str(curMaxLat - 0.1) +
                                         '&y2=' + str(curMaxLat),
                                         callback=self.borders, dont_filter=True,meta ={'province':province,'city':city,'county':county})
                    print(scrapy.Request.url)
                    curMaxLat -= 0.1
                curMinLng += 0.1

    def borders(self, response):
        item = BorderItem()
        item['province'] = response.meta['province']
        item['city'] = response.meta['city']
        item['county'] = response.meta['county']
        data = json.loads(response.text)['data']
        print(data)
        if 'comarea' in data.keys():
            bubbleList = data['comarea']
            for bubble in bubbleList:
                item['border'] = bubble['baiducoord']
                item['areaName'] = bubble['comarea']
                item['longitude'] = bubble['x']
                item['latitude'] = bubble['y']
                yield item
