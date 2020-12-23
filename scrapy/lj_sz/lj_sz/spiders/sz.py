import scrapy
from lj_sz.items import LjSzItem
from bs4 import BeautifulSoup
import sys
import json
import re

class SzSpider(scrapy.Spider):
    name = 'sz'
    allowed_domains = ['sz.lianjia.com']
    # start_urls = ['https://www.baidu.com']
    start_urls = [
        'https://sz.lianjia.com/chengjiao/luohuqu/pg1',
        'https://sz.lianjia.com/chengjiao/futianqu/pg1',
        'https://sz.lianjia.com/chengjiao/nanshanqu/pg1',
        'https://sz.lianjia.com/chengjiao/yantianqu/pg1',
        'https://sz.lianjia.com/chengjiao/baoanqu/pg1',
        'https://sz.lianjia.com/chengjiao/longgangqu/pg1',
        'https://sz.lianjia.com/chengjiao/longhuaqu/pg1',
        'https://sz.lianjia.com/chengjiao/guangmingqu/pg1',
        'https://sz.lianjia.com/chengjiao/pingshanqu/pg1',
        'https://sz.lianjia.com/chengjiao/dapengxinqu/pg1',
    ]
    i = 0

    def parse(self, response):
     
        item =  LjSzItem()
        # print(response.request.url)
        for li in  response.xpath('/html/body/div[5]/div[1]/ul//li'):
            item['region'] = [self.getRegion(response)]
            url =  li.xpath('./div/div[@class="title"]/a/@href').extract_first()
            print(url)
            if url:
                # 请求详情页
                yield scrapy.Request(
                    url,
                    callback=self.detail_parse,
                    meta={"item": item}
                )

        # # 下一页递归爬
        new_links = response.xpath('//div[contains(@page-data, "totalPage")]/@page-data').extract()
        totalPage = json.loads(new_links[0])['totalPage']
        nowPage   = json.loads(new_links[0])['curPage']
        # print('页数情况',totalPage)
        print('当前------------------------------页',nowPage)
        print()
        if nowPage < totalPage :
            now_url = response.request.url
            urlList = now_url.split('/pg')
        #    next_url = 'https://sz.lianjia.com/chengjiao/dapengxinqu/pg' + str(nowPage+1) + '/'
            next_url = urlList[0] + '/pg'  + str(nowPage+1) + '/'
            print('下一页=============================',next_url)

            yield scrapy.Request(next_url,meta={
                'dont_redirect': True,
                'handle_httpstatus_list': [301,302],
                'item':item
            }, callback=self.parse)


    def getRegion(self, response):
        regionList = ['luohuqu','futianqu','nanshanqu','yantianqu','baoanqu','longgangqu','longhuaqu','guangmingqu','pingshanqu','dapengxinqu']
        regionMap = {
            'luohuqu' : '罗湖区',
            'futianqu' : '福田区',
            'nanshanqu': '南山区',
            'yantianqu': '盐田区',
            'baoanqu' : '宝安区',
            'longgangqu': '龙岗区',
            'longhuaqu': '龙华区',
            'guangmingqu': '光明区',
            'pingshanqu': '坪山区',
            'dapengxinqu': '大鹏新区',
        }
        for region in regionList:
            if region in response.request.url:
                return regionMap[region]
        return None
    # 爬取详情页数据
    def detail_parse(self, response):
        # 接收上级已爬取的数据
        item = response.meta['item']   
        #一级内页数据提取 
        originHtml = response.xpath("/html/body/script[11]/text()").extract()[0]
        originHtml = str(originHtml)
        location   = re.findall(r"resblockPosition:'(.*)'", originHtml)
        item['location'] = location
        item['total_price']  = response.xpath('/html/body/section[1]/div[2]/div[2]/div[1]/span/i/text()').extract()
        item['title'] = response.xpath('/html/body/div[4]/div/text()').extract()
        item['unit_price']  = response.xpath('/html/body/section[1]/div[2]/div[2]/div[1]/b/text()').extract()
        item['trade_time']  = response.xpath('/html/body/div[4]/div/span/text()').extract()
        item['url'] = [response.request.url]
        # 二级内页地址爬取
        # yield scrapy.Request(item['url'] + "&123", meta={'item': item}, callback=self.detail_parse2)
        # 有下级页面爬取 注释掉数据返回
        yield item

                    