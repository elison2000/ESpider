#!/usr/bin/python3
# -*- encoding: utf-8 -*-

# 数据目录
DATADIR = 'D:/py3proj/爬虫/data'

# 浏览器设置
WEBDRIVER_TYPE = 'chrome'
# WEBDRIVER_PATH = 'D:/py3proj/plugins/phantomjs.exe'
WEBDRIVER_PATH = 'D:/py3proj/plugins/chromedriver.exe'
CHROME_PATH = 'C:/Users/elison/AppData/Local/Google/Chrome/Application/chrome.exe'

# 邮件设置
MAIL_ENABLED = 'N'
MAIL_CONF = {'host': 'mail.163.com', 'user': 'abc123@163.com', 'passwd': '*******',
             'ssl_enabled': 'Y'}
MAIL_RECEIVERS = ['abc123@yonyou.com']

# 数据推送到文本文件
TEXT_ENABLED = 'Y'

# 数据推送到数据库
DB_ENABLED = 'N'
DB_URL = 'mysql://spider:m123@172.29.3.243:3306/spiderdb'
