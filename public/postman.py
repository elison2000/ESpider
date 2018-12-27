#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
#------------------------------------------------#
#         Title        : 发送邮件                #
#         Version      : v1.3                    #
#         Author       : Elison                  #
#         Email        : Ly99@qq.com             #
#         Updated Date : 2018-1-30               #
#------------------------------------------------#


import os
import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class mail:
    def __init__(self, conf):
        self.host = conf['host']
        self.user = conf['user']
        self.passwd = conf['passwd']
        self.ssl_enabled = conf.get('ssl_enabled', 'N')
        self.port = conf.get('port', None)

        #判断端口
        if self.port is None:
            if self.ssl_enabled == 'N':
                self.port = '25'

            else:
                self.port = '465'

        #使用安全或非安全模式连接
        if self.ssl_enabled == 'N':
            self.smtp = smtplib.SMTP(self.host, self.port, timeout=10)
        else:
            self.smtp = smtplib.SMTP_SSL(self.host, self.port, timeout=10)

        #连接邮件服务器
        self.smtp.login(self.user, self.passwd)

        # 构造消息内容
        self.msg = MIMEMultipart()
        self.msg['From'] = self.user

    def add_att(self, filename):
        "增加附件"
        #判断文件是否存在
        if not os.path.isfile(filename):
            print('附件不存在: {0}'.format(filename))
            return -1
        with open(filename, 'rb') as f:
            data = f.read()
            att = MIMEText(data, 'base64', 'utf-8')
            att['Content-Type'] = 'application/octet-stream'
            att.add_header('Content-Disposition', 'attachment',
                           filename=Header(os.path.basename(filename), 'utf-8').encode())
            self.msg.attach(att)
            return 100

    def send(self, *, to_list, subject, content, cc_list=[], type='plain'):
        "发送邮件"

        #判断参数
        assert type in ('plain', 'html'), 'type参数错误'

        self.msg['To'] = ','.join(to_list)  #收件人
        self.msg['Cc'] = ','.join(cc_list)  #抄送人
        self.msg['Subject'] = subject  #主题
        self.msg.attach(MIMEText(content, type, 'utf-8'))  #正文

        #发送
        self.smtp.sendmail(self.user, to_list + cc_list, self.msg.as_string())
        # print('邮件发送成功！')
        return 100

    def close(self):
        #关闭连接
        self.smtp.quit()


def send_mail(conf, *, to_list, subject, content, cc_list=[]):
    "发送邮件"
    obj = mail(conf)
    obj.send(to_list=to_list, subject=subject, content=content, cc_list=cc_list)
    obj.close()


def send_sms():
    "发送短信"
    pass


def send_wechat():
    "发送微信"
    pass


if __name__ == "__main__":
    conf = {'host': '123.103.9.36', 'ssl_enabled': 'Y', 'user': 'yyfax_monitor@yonyou.com',
            'passwd': '****'}
    to_list = ['chenyjc@yonyou.com', ]
    cc_list = ['18520806800@163.com', ]
    subject = '测试邮件'
    content = '这是测试邮件！'
    send_mail(conf, to_list, subject, content, cc_list)
