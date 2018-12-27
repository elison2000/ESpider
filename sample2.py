#!/usr/bin/python3
# -*- encoding: utf-8 -*-
# ------------------------------------------------- #
#       Title        : 爬放款明细                   #
#       Version      : v1.1                         #
#       Author       : Elison                       #
#       Email        : Ly99@qq.com                  #
#       Updated Date : 2018-12-23                   #
# ------------------------------------------------- #

import time
import re
from public.spider import ESpider

"""
案例2：有一些网页由于以下原因不能直接通过get\post方式爬取：
1、需要登录授权
2、有防爬虫策略
3、分析其接口难度大

对于此类网页，可以采用模拟浏览器访问页面，获取页面源码的方式爬取，该案例模拟了使用浏览器访问主页获取到缓存后，然后把缓存导入request会话进行获取数据
更多用法和功能请查看public/spdier.py源码
"""


class Spider(ESpider):
    def __init__(self):
        self.name = '你我贷放款明细'  # 爬虫名
        self.urls = []
        self.table_name = 'loan_detail_niwodai'  # 推送到数据库的表名
        self.js_enabled = 'Y'  # 开启js渲染，即是启动浏览器进行访问页面
        self.request_mode = 'post'  # 获取到缓存后改用request爬取，效率更高

        # request头部信息，不定义的话，会使用默认的头部
        self.headers = {"content-type": "application/x-www-form-urlencoded",
                        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'}

        # 使用浏览器访问时，ajax请求是异步进行的，不同的数据在不同时间返回，我们可以使用几种方式探测页面内容，如果探测到页面已加载到我们想要的数据，就结束等待，返回数据。
        self.wait_method = 'find_text_by_regex'  # 这里使用正则表达式进行探测，还有其他探测方式，具体查看public/spdier.py源码
        self.wait_expr = '<a class="link_prolist" href="(.*?)" target="_blank">'  # 正则表达式，匹配成功就结束等待，如果匹配不成功，等待self.timeout秒，timeout时间到后，不管是否页面加载完毕都结束加载。
        self.timeout = 5  # 等待时间，单位秒

    def prepare(self):
        "添加要处理的页面"
        regex = re.compile('<a class="link_prolist" href="(.*?)" target="_blank">', re.S)  # 使用正则表达式解析页面
        self.request('https://member.niwodai.com')  # 先访问主页，获取缓存后，再访问接口，如果跳过这步，直接访问接口是不可行的
        for i in range(10, 0, -1):  # 取前10页
            preurl = 'https://member.niwodai.com/portal/loan/getLoanList.do?pageNo={0}'.format(
                i)  # 进一步通过接口获取需要爬取的url，一共有10页
            page = self.request(preurl)
            text_list = regex.findall(page)  # 获取href内容
            for i in text_list:
                url = 'https://member.niwodai.com' + i  # 把href拼接成完整地址
                self.urls.append(url)  # 添加到url列表

        # 获取缓存后，把浏览器模式关闭，后面的访问使用requests方法
        self.js_enabled = 'N'

        # 这步执行后，程序自动把浏览器的缓存导入request会话，无需手动处理

    def parse(self, result):
        "解析页面"
        page_source = result['page_source']
        regex = re.compile(
            '<i class="fl biao_tips size24 img_icon_new sItem_new_12100 mar_r10" style="margin-top: 2px;"></i>\s+(.*?)\s+.*?class="yunyingact fs_14 ml_10">.*?<em class="fs_14 fc_9 ml_30 block pad_l5 pad_t5">(.*?)</em>.*?<em class="block mar_b5 fc_9">债权总额 </em>\s+<p class="lh36"><em class="fs_30 Numfont">\s+(.*?)\s+</em><em class="fs_16">元</em>',
            re.S)
        text_list = regex.findall(page_source)
        for i in text_list:
            loan_id = i[1].replace('项目ID：', "")
            loan_amount = i[2].replace(",", "")
            comment = i[0]
            capture_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            item = {'capture_time': capture_time, 'subject': self.name, 'loan_id': loan_id,
                    'loan_amount': loan_amount, 'comment': comment}
            # print(item)
            yield item


# main
obj = Spider()
obj.start()  # 爬虫启动入口
