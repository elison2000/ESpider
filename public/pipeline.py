#!/usr/bin/python3
# -*- encoding: utf-8 -*-

# from urllib.parse import urlparse
import pymysql
import traceback


class textfile:
    """默认生成hive格式的文本文件：分隔符：\001 换行：\n"""

    def __init__(self, filename, mode, field_separator=u'\001', line_separator=u'\n', encoding='utf-8'):
        self.field_separator = field_separator
        self.line_separator = line_separator
        self.file = open(filename, mode, encoding=encoding)

    def write(self, row):
        """写入行"""
        text = self.field_separator.join(row) + self.line_separator
        self.file.write(text)

    def close(self):
        """关闭文件"""
        self.file.close()


class mysql:
    """数据推送到mysql数据库"""

    def __init__(self, conf):
        host = conf['host']
        port = int(conf['port'])
        db = conf['db']
        user = conf['user']
        passwd = conf['passwd']
        self.conn = pymysql.connect(host=host, port=port, user=user, password=passwd, db=db, charset='utf8mb4')

    def query(self, sql):
        """批量插入数据"""
        cur = self.conn.cursor()
        cur.execute(sql)
        yield cur.fetchone()

    def insert(self, table_name, fieldname_list, row):
        """插入一条数据"""
        cur = self.conn.cursor()
        row = ["'{0}'".format(i) for i in row]
        value_text = ','.join(row)
        fieldname_text = ','.join(fieldname_list)
        sql = 'insert into {0}({1}) values({2})'.format(table_name, fieldname_text, value_text)
        cur.execute(sql)
        self.conn.commit()

    def batch_insert(self, table_name, fieldname_list, rows):
        """批量插入数据，效率更高"""
        cur = self.conn.cursor()
        value_text = ','.join(['%s' for i in fieldname_list])
        fieldname_text = ','.join(fieldname_list)
        sql = 'insert into {0}({1}) values({2})'.format(table_name, fieldname_text, value_text)
        cur.executemany(sql, rows)
        self.conn.commit()

    def close(self):
        """关闭数据库连接"""
        self.conn.close()


class oracle:
    """数据推送到oracle数据库"""
    pass


class mongo:
    """数据推送到mongo数据库"""
    pass


if __name__ == '__main__':
    obj = textfile('123.txt', 'a')
    row = ['sdfdb', '123.01']
    obj.write(row)
    obj.close()


    # dburl = 'mysql://spider:m123@172.29.3.243:3306/spiderdb'
    # urlobj = urlparse(dburl)
    # conf = {}
    # conf['dbtype'] = urlobj.scheme
    # conf['host'] = urlobj.hostname
    # conf['port'] = urlobj.port
    # conf['user'] = urlobj.username
    # conf['passwd'] = urlobj.password
    # conf['db'] = urlobj.path.split('/')[1]
    # db = mysql(conf)
    # table_name = 'ESpider'
    # fieldname_list = ['name','amount']
    # rows = [('测试1','500.12'),('测试2','1033.13')]
    # db.import_tab('ESpider',fieldname_list, rows)
