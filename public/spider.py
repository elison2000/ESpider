#!/usr/bin/python3
# -*- encoding: utf-8 -*-
# ------------------------------------------------- #
#       Title        : 定义爬虫父类                 #
#       Version      : v1.2.13                      #
#       Author       : Elison                       #
#       Email        : Ly99@qq.com                  #
#       Updated Date : 2018-12-27                   #
# ------------------------------------------------- #

import os
import re
import time
import traceback
import requests
from requests.cookies import RequestsCookieJar
import urllib.parse
import queue
import threading
from selenium import webdriver
from bs4 import BeautifulSoup
from . import settings
from . import pipeline
from . import postman

_version = 'v1.2.13'


class ESpider:
    """"爬虫父类，通过requests和selenium多种方式爬取数据"""

    def __init__(self):
        """定义属性"""
        self.name = ''
        self.urls = []
        self.js_enabled = 'Y'  # 开启js渲染

    def init(self):
        """初始化
        1、检查和初始化参数、属性等
        2、打开request会话、浏览器
        3、开启线程
        """

        self.logging_lock = threading.Lock()
        # 数据目录
        assert isinstance(settings.DATADIR, str), 'DATADIR参数错误'
        os.chdir(settings.DATADIR)
        today = time.strftime('%Y%m%d', time.localtime())
        if not os.path.exists(today):
            os.mkdir(today)
        os.chdir(today)
        self.logging(1, '数据目录：{0}'.format(settings.DATADIR))

        # 属性
        assert isinstance(self.name, str), 'name属性错误'
        assert isinstance(self.urls, list), 'urls属性错误'

        # js渲染开关
        if hasattr(self, 'js_enabled'):
            assert self.js_enabled in ('Y', 'N'), 'js_enabled属性错误'
            if self.js_enabled == 'Y':
                assert settings.WEBDRIVER_TYPE in ('chrome', 'firefox', 'phantomjs'), 'WEBDRIVER_TYPE参数错误'
                assert isinstance(settings.WEBDRIVER_PATH, str), 'WEBDRIVER_PATH参数错误'

        else:
            self.js_enabled = 'N'

        # 请求模式
        if not hasattr(self, 'request_mode'):
            self.request_mode = 'get'
        assert self.request_mode in ('get', 'post'), 'request_mode属性错误'

        # 告警通知开关
        if hasattr(settings, 'MAIL_ENABLED'):
            self.mail_enabled = settings.MAIL_ENABLED
        else:
            self.mail_enabled = 'N'
        assert self.mail_enabled in ('Y', 'N'), 'MAIL_ENABLED参数错误'

        # 收件人
        if self.mail_enabled == 'Y':
            if not hasattr(self, 'mail_receivers'):
                self.mail_receivers = settings.MAIL_RECEIVERS
            assert isinstance(self.mail_receivers, list), 'mail_receivers属性错误'

        # 文本推送开关
        if hasattr(settings, 'TEXT_ENABLED'):
            self.text_enabled = settings.TEXT_ENABLED
        else:
            self.text_enabled = 'Y'
        assert self.text_enabled in ('Y', 'N'), 'TEXT_ENABLED参数错误'

        # 数据库推送开关
        if hasattr(settings, 'DB_ENABLED'):
            self.db_enabled = settings.DB_ENABLED
        else:
            self.db_enabled = 'N'
        assert self.db_enabled in ('Y', 'N'), 'DB_ENABLED参数错误'

        # 设置headers
        if not hasattr(self, 'headers'):
            self.headers = {'Content-Type': 'text/html; charset=utf-8',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'}

        # 设置字符集
        if not hasattr(self, 'encoding'):
            self.encoding = 'utf-8'

        # 设置延时
        if not hasattr(self, 'timeout'):
            self.timeout = 5

        # 设置浏览器等待属性
        if not hasattr(self, 'wait_method'):
            self.wait_method = None

        if not hasattr(self, 'wait_expr'):
            self.wait_expr = None

        # 设置浏览器HEADLESS模式
        if not hasattr(self, 'webdriver_headless'):
            if hasattr(settings, 'WEBDRIVER_HEADLESS'):
                self.webdriver_headless = settings.WEBDRIVER_HEADLESS
            else:
                self.webdriver_headless = 'Y'  # 默认开启
        assert self.webdriver_headless in ('Y', 'N'), 'WEBDRIVER_HEADLESS参数错误'

        # 增加属性
        self.done_urls = set()
        self.file_id = 0  # html文件序号
        self.queue_list = []
        self.errmsg_list = []

        # 启动线程
        if self.text_enabled == 'Y':
            self.textfile = pipeline.textfile(self.name + '.txt', 'a')
            self.logging(1, '文本文件打开成功')
            self.text_data_queue = queue.Queue(maxsize=1000000)  # 文本存储队列
            self.queue_list.append(self.text_data_queue)
            self.TEXTWriter = threading.Thread(name='TEXTWriter', target=self.write_text)
            self.TEXTWriter.setDaemon(True)
            self.TEXTWriter.start()
            self.logging(1, '{0}进程启动成功'.format(self.TEXTWriter.name))
        else:
            self.logging(1, 'TEXT_ENABLED 【关闭】')
        if self.db_enabled == 'Y':
            assert isinstance(settings.DB_URL, str), 'DB_URL参数错误'
            self.dbconnect = self.open_dbconnect(settings.DB_URL)  # 打开数据库连接
            self.dbconnect.query('select now()')  # 测试数据库连接
            self.logging(1, '数据库连接成功')
            self.db_data_queue = queue.Queue(maxsize=1000000)  # 数据库存储队列
            self.queue_list.append(self.db_data_queue)
            self.DBImporter = threading.Thread(name='DBImporter', target=self.import_table)
            self.DBImporter.setDaemon(True)
            self.DBImporter.start()
            self.logging(1, '{0}进程启动成功'.format(self.DBImporter.name))
        else:
            self.logging(1, 'DB_ENABLED 【关闭】')

        # 打开reqsess
        self.reqsess = requests.session()

        # 打开浏览器
        if self.js_enabled == 'Y':
            self.browser = self.open_browser()  # 打开浏览器

    def prepare(self):
        """准备阶段，爬取一些需要进行预处理的网页，可重构此方法。比如：登录"""
        pass

    def start(self):
        """程序运行入口
        主要有4个阶段：
        1、初始化
        2、预处理，登录、导入缓存
        3、爬取数据
        4、关闭资源
        """

        # 初始化参数、属性、线程、浏览器、数据库连接等
        print("[{0}] [INFO] 启动爬虫【{1}】".format(self.get_now(), self.name))
        print("[{0}] [INFO] 版本：{1}".format(self.get_now(), _version))
        self.init()
        self.logging(1, '程序初始化完成')

        # 预处理阶段，如登录，批量添加url等
        self.prepare()

        # 把浏览器缓存导入request会话
        if hasattr(self, 'browser'):
            self.import_cookies_from_browser()

        # 从urls池中取出url，访问网页
        for result in self.batch_request():
            try:
                items = self.parse(result)  # 解析页面
                for item in items:
                    [que.put(item) for que in self.queue_list]  # 数据推入队列处理
            except Exception as e:
                errmsg = 'parse()方法异常：\n{0}'.format(traceback.format_exc(limit=3))
                self.logging(3, errmsg)
                self.errmsg_list.append(errmsg)  # 错误信息添加到队列，在close阶段统一处理：触发告警或发送邮件

                # 保存异常的页面数据
                try:
                    self.file_id += 1
                    filename = '{0}_{1}.dmp'.format(self.name, self.file_id)
                    with open(filename, 'w') as f:
                        f.write(result['page_source'])
                except Exception:
                    errmsg = '异常页面保存出错：\n{0}'.format(traceback.format_exc(limit=3))
                    self.logging(3, errmsg)

        # 回收资源
        self.close()

    def close(self):
        """回收资源"""

        # 先关闭线程，再关闭数据库连接，顺序不能调
        if hasattr(self, 'DBImporter'):
            [self.db_data_queue.put(None) for i in range(10)]  # 发送线程结束信号
            self.DBImporter.join()  # 等待线程结束
            self.logging(1, '{0}线程已退出'.format(self.DBImporter.name))
        if hasattr(self, 'TEXTWriter'):
            [self.text_data_queue.put(None) for i in range(10)]  # 发送线程结束信号
            self.TEXTWriter.join()  # 等待线程结束
            self.logging(1, '{0}线程已退出'.format(self.TEXTWriter.name))

        # 关闭request会话
        if hasattr(self, 'reqsess'):
            try:
                self.reqsess.close()
                self.logging(1, 'request会话已关闭')
            except Exception as e:
                self.logging(3, '关闭request会话：{0}'.format(e))

        # 关闭浏览器
        if hasattr(self, 'browser'):
            try:
                self.browser.quit()
                self.logging(1, '浏览器已关闭')
            except Exception as e:
                self.logging(3, '关闭浏览器：{0}'.format(e))

        # 关闭数据库连接
        if hasattr(self, 'dbconnect'):
            try:
                self.dbconnect.close()
                self.logging(1, '数据库连接已关闭')
            except Exception as e:
                self.logging(3, '关闭数据库连接：{0}'.format(e))

        # 关闭文件
        if hasattr(self, 'textfile'):
            try:
                self.textfile.close()
                self.logging(1, '文本文件已关闭')
            except Exception as e:
                self.logging(3, '关闭文本文件：{0}'.format(e))

        # 发送告警
        if self.mail_enabled == 'Y' and self.errmsg_list:
            to_list = self.mail_receivers
            subject = self.name + '爬取异常'
            content = subject + '：\n'
            content += '\n'.join(self.errmsg_list[0:5])  # 取前5个错误内容
            self.send_mail(to_list=to_list, subject=subject, content=content)
            self.logging(1, '告警邮件发送成功')

        self.logging(1, '程序结束！')

    def parse(self, result):
        """"解析页面"""
        return result

    def batch_request(self):
        """批量处理url池
        从urls队列取出url获取页面（后进先出），取出的数据分两种情况，需要分别处理：
        1、url
        2、url、payload组成的元组
        """

        while True:
            if self.urls:
                url_info = self.urls.pop()  # 如果urls池不为空，取出最后一个
                if not isinstance(url_info, str):
                    assert isinstance(url_info, tuple), 'url必须是str或tuple类型'
                    assert isinstance(url_info[1], str), 'payload必须是str类型'

                if isinstance(url_info, str):  # url_info是url
                    self.logging(1, 'request页面：{0}'.format(url_info))
                    page_source = self.request(url_info)
                    self.done_urls.add(url_info)  # 把url放进已访问列表
                    if page_source:
                        result = {'url': url_info, 'page_source': page_source}
                        yield result
                elif isinstance(url_info, tuple):  # urlinfo是url和payload
                    url = url_info[0]
                    payload = url_info[1]
                    self.request_mode = 'post'
                    self.logging(1, 'request页面：{0}'.format(url))
                    page_source = self.request(url, data=payload)
                    self.done_urls.add(url_info)  # 把url放进已访问列表
                    if page_source:
                        result = {'url': url_info, 'page_source': page_source}
                        yield result
                else:
                    self.logging(2, 'url类型无效：{0}'.format(url_info))

            else:
                self.logging(1, '所有页面均已处理')
                break

    def request(self, url, *, params=None, data=None):
        """请求页面入口"""
        if self.js_enabled == 'N':
            page_source = self.get_page(url, timeout=self.timeout, request_mode=self.request_mode, headers=self.headers,
                                        params=params, data=data)  # 获取静态页面
            return page_source
        else:
            page_source = self.get_js_page(url, timeout=self.timeout, wait_method=self.wait_method,
                                           wait_expr=self.wait_expr)  # 获取动态页面
            return page_source

    def get_page(self, url, *, request_mode='get', retry=3, timeout=5, headers=None, params=None, data=None):
        """使用get或post方法获取静态页面"""
        assert request_mode in ('get', 'post'), 'request_mode参数错误'
        # 访问页面，如果失败重试retry次
        for i in range(retry):
            # 使用get请求模式
            if request_mode == 'get':
                try:
                    response = self.reqsess.get(url, timeout=timeout, headers=headers, params=params)
                    if response.status_code == 200:
                        page_source = response.content.decode(self.encoding, 'ignore')
                        return page_source
                    else:
                        self.logging(3, 'get()页面失败：{0}'.format(response.url))
                        self.logging(3, '状态码：{0} {1}'.format(response.status_code, response.reason))
                except requests.exceptions.ReadTimeout as e:
                    self.logging(3, 'get()页面超时：{0}'.format(e))
                except Exception:
                    errmsg = 'get()页面异常：\n{0}\n{1}'.format(url, traceback.format_exc(limit=3))
                    self.logging(3, errmsg)
                    if i == retry - 1:  # 只有最后一次异常才告警
                        self.errmsg_list.append(errmsg)  # 错误信息添加到队列，在close阶段统一处理：触发告警或发送邮件

            # 使用post请求模式
            elif request_mode == 'post':
                try:
                    response = self.reqsess.post(url, timeout=timeout, headers=headers, params=params, data=data)
                    if response.status_code == 200:
                        page_source = response.content.decode(self.encoding, 'ignore')
                        return page_source
                    else:
                        self.logging(3, 'post()页面失败：{0} data={1}'.format(response.url, data))
                        self.logging(3, '状态码：{0} {1}'.format(response.status_code, response.reason))
                except requests.exceptions.ReadTimeout as e:
                    self.logging(3, 'post()页面超时：{0}'.format(e))
                except Exception:
                    errmsg = 'post()页面异常：\n{0} data={1}\n{2}'.format(url, data, traceback.format_exc(limit=3))
                    self.logging(3, errmsg)
                    if i == retry - 1:  # 只有最后一次异常才告警
                        self.errmsg_list.append(errmsg)  # 错误信息添加到队列，在close阶段统一处理：触发告警或发送邮件

        self.logging(1, '重试次数达{0}次，退出'.format(retry))

    def get_js_page(self, url, *, timeout=5, wait_seconds=0.5, wait_method=None, wait_expr=None):
        """使用浏览器获取页面"""
        self.browser.get(url)
        self.logging(1, '正在加载页面：{0}'.format(url))

        if wait_method is None:
            self.logging(1, '{0}秒后浏览器返回结果'.format(timeout))
            time.sleep(timeout)
            return self.browser.page_source

        # 等待页面加载完成
        wait_cnt = int(timeout / wait_seconds)
        for cnt in range(wait_cnt):
            if self.wait_condition(wait_method=wait_method, wait_expr=wait_expr):
                self.logging(1, '页面加载成功')
                return self.browser.page_source
            else:
                time.sleep(wait_seconds)
        self.logging(2, '页面加载超时：{0}'.format(url))
        return self.browser.page_source

    def wait_condition(self, *, wait_method, wait_expr):
        """
        wait_method有以下类型：
        find_text_by_regex   #使用正则表达式查找文本，找到则返回true
        find_elmt_by_css  #使用bs4 select查找元素，找到则返回true
        find_text_by_css  #使用bs4 select查找元素是否包含某个文本，包含则返回true
        """
        assert wait_method in ['find_text_by_regex', 'find_elmt_by_css', 'find_text_by_css'], "wait_method参数错误"

        if wait_method == 'find_text_by_regex':
            assert isinstance(wait_expr, str), 'expr参数不是str类型'
            regex = re.compile(wait_expr, re.S)
            text_list = regex.findall(self.browser.page_source)
            if text_list:
                return True

        elif wait_method == 'find_elmt_by_css':
            assert isinstance(wait_expr, str), 'expr参数不是str类型'
            soup = BeautifulSoup(self.browser.page_source, 'lxml')
            elmts = soup.select(wait_expr)
            if elmts:
                return True

        elif wait_method == 'find_text_by_css':
            assert isinstance(wait_expr, tuple), 'expr参数必须是tuple类型'
            assert len(wait_expr) == 2, 'expr参数必须有2个元素'
            assert isinstance(wait_expr[0], str) and isinstance(wait_expr[1], str), 'expr参数2个元素必须是str类型'
            css_str = wait_expr[0]
            text = wait_expr[1]
            soup = BeautifulSoup(self.browser.page_source, 'lxml')
            elmts = soup.select(css_str)
            if elmts:
                elmt_text = elmts[0].get_text()
                if text in elmt_text:
                    return True

    def import_cookies_from_browser(self):
        "把浏览器缓存导入request会话"
        cookie_jar = RequestsCookieJar()
        for i in self.browser.get_cookies():
            cookie_jar.set(i['name'], i['value'])
        self.reqsess.cookies = cookie_jar

    def write_text(self):
        """把数据推送到文本"""
        while True:
            item = self.text_data_queue.get()
            if item is None:
                break
            if not hasattr(self, '__keys'):
                self.__keys = item.keys()
            row = [item[key] for key in self.__keys]
            self.textfile.write(row)

    def import_table(self):
        """把数据推送到数据库"""
        while True:
            item = self.db_data_queue.get()
            if item is None:
                break
            if not hasattr(self, '__keys'):
                self.__keys = item.keys()
            row = [item[key] for key in self.__keys]
            try:
                self.dbconnect.insert(self.table_name, self.__keys, row)
            except Exception:
                errmsg = 'import_table()方法异常：\n{0}\n{1}'.format(traceback.format_exc(limit=3), row)
                self.logging(3, errmsg)

    def logging(self, level, text):
        """打印日志信息（并行时要加锁）"""
        try:
            self.logging_lock.acquire()
            if level == 1:
                print("[{0}] [INFO] {1}".format(self.get_now(), text))
            elif level == 2:
                print("[{0}] [WARN] {1}".format(self.get_now(), text))
            else:
                print("[{0}] [ERROR] {1}".format(self.get_now(), text))
        except Exception as e:
            print("[{0}] [ERROR] 未知错误：{1}".format(self.get_now(), str(e)))
        finally:
            self.logging_lock.release()

    def open_browser(self):
        """打开浏览器"""
        self.logging(1, '打开浏览器')
        if settings.WEBDRIVER_TYPE.lower() == 'chrome':
            browser = webdriver.Chrome(executable_path=settings.WEBDRIVER_PATH,
                                       chrome_options=self.set_chrome())  # 打开浏览器
            self.logging(1, 'chrome浏览器已打开')
            return browser
        elif settings.WEBDRIVER_TYPE.lower() == 'firefox':
            options = webdriver.FirefoxOptions()
            if self.webdriver_headless == 'Y':
                options.add_argument('--headless')  # 关闭浏览器可视化界面
            browser = webdriver.Firefox(options=options, executable_path=settings.WEBDRIVER_PATH)  # 打开浏览器
            self.logging(1, 'firefox浏览器已打开')
            return browser
        elif settings.WEBDRIVER_TYPE.lower() == 'phantomjs':
            browser = webdriver.PhantomJS(executable_path=settings.WEBDRIVER_PATH)
            self.logging(1, 'phantomjs浏览器已打开')
            return browser
        else:
            assert False, 'WEBDRIVER_TYPE参数错误'

    @classmethod
    def save_html(cls, filename, page_source):
        """保存html文件"""
        with open(filename, 'w', encoding='utf-8') as fileobj:
            fileobj.write(page_source)

    @classmethod
    def open_dbconnect(cls, dburl):
        """打开数据库连接"""
        urlobj = urllib.parse.urlparse(dburl)
        conf = {}
        conf['dbtype'] = urlobj.scheme
        conf['host'] = urlobj.hostname
        conf['port'] = urlobj.port
        conf['user'] = urlobj.username
        conf['passwd'] = urlobj.password
        conf['db'] = urlobj.path.split('/')[1]
        assert conf['dbtype'] in ('mysql', 'oracle'), '数据库类型错误'
        if conf['dbtype'] == 'mysql':
            # 推送到mysql
            dbconnect = pipeline.mysql(conf)
        elif conf['dbtype'] == 'oracle':
            dbconnect = pipeline.oracle(conf)
        return dbconnect

    def set_chrome(self):
        """设置浏览器"""
        from selenium.webdriver.chrome.options import Options
        chrome_options = Options()
        chrome_options.add_argument('lang=zh_CN.gbk')
        chrome_options.add_argument('--no-sandbox')  # 解决DevToolsActivePort文件不存在的报错
        chrome_options.add_argument('window-size=1920x3000')  # 指定浏览器分辨率
        chrome_options.add_argument('--disable-gpu')  # 谷歌文档提到需要加上这个属性来规避bug
        chrome_options.add_argument('--hide-scrollbars')  # 隐藏滚动条, 应对一些特殊页面
        chrome_options.add_argument('blink-settings=imagesEnabled=false')  # 不加载图片, 提升速度
        chrome_options.add_argument('--disable-images')  # 关闭图像
        chrome_options.add_argument('-incognito')  # 隐身模式
        chrome_options.add_argument('--disable-java')  # 关闭java
        chrome_options.add_argument('--no-proxy-server')  # 关闭代理
        chrome_options.add_argument('--disable-plugins')  # 关闭插件
        # chrome_options.add_argument('--disable-javascript')  # 关闭js渲染
        chrome_options.binary_location = settings.CHROME_PATH  # 设置浏览器路径
        if self.webdriver_headless == 'Y':
            chrome_options.add_argument('--headless')  # 关闭浏览器可视化界面

        return chrome_options

    @classmethod
    def get_now(cls):
        """获取当前时间"""
        now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        return now

    @classmethod
    def ts_to_time(cls, ts):
        if len(str(ts)) > 10:
            ts = ts // 1000
        time_local = time.localtime(ts)
        return time.strftime("%Y-%m-%d %H:%M:%S", time_local)

    @classmethod
    def dict_to_form_data(cls, payload):
        """格式化为form data"""
        data = urllib.parse.urlencode(payload)
        return data

    @classmethod
    def send_mail(cls, *, to_list, subject, content, cc_list=[]):
        """发送邮件"""
        conf = settings.MAIL_CONF
        postman.send_mail(conf, to_list=to_list, subject=subject, content=content, cc_list=cc_list)


if __name__ == '__main__':
    class Spider(ESpider):

        def __init__(self):
            self.name = '银湖网成交金额'
            self.urls = ['https://www.yinhu.com/main.bl', 'https://www.yinhu.com/main.bl']
            self.js_enabled = 'Y'

        def parse(self, result):
            regex = re.compile('\s.*?元', re.S)
            page_source = result['page_source']
            text_list = regex.findall(page_source)
            print(text_list)
            for i in text_list:
                name = i[0]
                amount = i[1]
                item = {'name': name, 'amount': amount}
                yield item


    # main
    obj = Spider()
    obj.start()
