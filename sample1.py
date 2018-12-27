#!/usr/bin/python3
# -*- encoding: utf-8 -*-
# ------------------------------------------------- #
#       Title        : 爬取微贷成交额               #
#       Version      : v1.0                         #
#       Author       : Elison                       #
#       Email        : Ly99@qq.com                  #
#       Updated Date : 2018-12-1                    #
# ------------------------------------------------- #


import time
import json  # 支持多种解析方式，如json,re,bs4,可自由选择
import pdb
from public.spider import ESpider

"""
案例1：对于简单网页只需要定义初始属性，重写parse方法即可爬取页面
更多用法和功能请查看public/spdier.py源码
"""


class Spider(ESpider):
    def __init__(self):
        self.name = '微贷成交额'  # 爬虫名
        self.urls = [
            'https://www.weidai.com.cn/index/v2/indexPlatformData']  # 爬取的url列表，可存放多个url，依次爬取（后进先出），使用相同的parse方法解析。
        self.table_name = 'trade_amount'  # 推送到数据库的表名，不需要入库可以在settings中关闭数据库选项。

    def parse(self, result):
        "解析页面"
        # 传递进入的result变量是字典形式，包含url和page_source
        url = result['url']
        page_source = result['page_source']
        # pdb.set_trace()      # 一般在这个位置进行调试，使用pdb调试（可以把这行注释去掉进行调试）
        capture_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        item_name = self.name
        data = json.loads(page_source)
        text = data['data']['turnover'].replace('亿元', '')
        trade_amount = text
        item = {'capture_time': capture_time, 'item_name': item_name,
                'trade_amount': trade_amount}  # 这里key的值必须是表字段名一一对应，是否入库失败
        print(item)  # 调试模式下可开启print
        yield item


# main
obj = Spider()
obj.start()  # 爬虫启动入口
