# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import  pandas as pd
import pymysql

class LjSzPipeline:
    def process_item(self, item, spider):
        scrapyData = []
        # 链接数据库
        conn=pymysql.connect('127.0.0.1','ectouch','ecTouchZS123')
        # 选择数据库
        conn.select_db('ectouch')
        cur=conn.cursor()
        sql="insert into lj_sz_scrapy_temp (city_desc,region, title, trade_time, total_price,total_unit,unit_price,unit_unit,location,url) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)";
        self.getData(item,scrapyData)
        print(scrapyData)
        try:
            # 执行sql语句
            insert=cur.executemany(sql,scrapyData)
            print ('批量插入返回受影响的行数：',insert)
            # 提交到数据库执行
            conn.commit()
        except:
        # 如果发生错误则回滚
            conn.rollback()
            print ('错误')
        conn.close()
        # print("title:",item['title'])
        # print("url:",item['url'])
        # print("total_price:",item['total_price'])
        # print("unit_price：",item['unit_price'])
        # print("trade_time:",item['trade_time'])
        # print("region:",item['region'])
        # print("location:",item['location'])
        print('============='*10)
        return 
    def getData(self, item, scrapyData):
        df = pd.DataFrame({"city_desc": '深圳', "region": item['region'], "title": item["title"],"trade_time":item["trade_time"],
        "total_price":item["total_price"],"total_unit":'万',"unit_price":item['unit_price'],"unit_unit":'元/平',"location":item['location'],"url":item['url']})
        def reshape(r):
            scrapyData.append(tuple(r))
        df.apply(reshape,axis=1)
        return 



# ['承翰半山海 1室1厅 36.35平米', '承翰半山海 1室1厅 36.34平米', '海语山林 2室1厅 70.14平米', '亚迪村 3室2厅 128.79平米', '亚迪村 3室2厅 130.19平米', '承翰陶柏莉 3室2厅 101.44平米', '鑫园广场 2室2厅 81.43平米', '海语山林 3室2厅 86.53平米', '承翰半山海 1室1厅 36.34平米', '金众云山栖 3室2厅 87.23平米', '海语山林 2室2厅 70.95平米', 'PURE33璞岸 2室1厅 69.53平米', '承翰半山海 2室2厅 78.68平米', '海语山林 3室2厅 87.76平米', '亚迪村 3室2厅 129.3平米', '承翰半山海 1室0厅 36.35平米', '海语山林 3室2厅 86.53平米', '海语山林 2室2厅 77.78平米', '承翰陶柏莉 2室2厅 72.78平米', 'KPR佳兆业广场 1室1厅 50平米', '亚迪村 3室2厅 128.65平米', '承翰半山海 2室2厅 77.3平米', '承翰陶柏莉 3室2厅 88.38平米', '亚迪村 3室2厅 129.25平米', '亚迪村 3室2厅 127.5平米', '承翰陶柏莉 3室2厅 88.42平米',
#  '鹏海苑小区 1室1厅 38.44平米', '海语山林 2室2厅 71.84平米', '金众云山栖 3室2厅 81.7平米', '金众云山栖 3室2厅 81.69平米']
# CREATE TABLE `lj_sz_scrapy` (
#   `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
#   `city_desc` varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT '城市',
#   `region` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT '区',
#   `title` varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT '标题',
#   `trade_time` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT '交易时间',
#   `total_price` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '0.00' COMMENT '总价',
#   `total_unit` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT '总价单位',
#   `unit_price` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '0.00' COMMENT '单价',
#   `unit_unit` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT '单价单位',
#   `location` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'location',
#   `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
#   `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#   `x` decimal(10,5) NOT NULL DEFAULT '0.00000' COMMENT 'x',
#   `y` decimal(10,5) NOT NULL DEFAULT '0.00000' COMMENT 'y',
#   PRIMARY KEY (`id`),
#   KEY `title` (`title`) USING BTREE
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;