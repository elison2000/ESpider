# Espider

Espider爬虫轻量级框架


这是轻量级的爬虫框架，主要有4个模块：

1、spider模块，只要实现爬虫功能，支持requests和selenium两种方式

2、settings模块，全局配置

3、pipeline模块，用于推送数据，目前支持推送到mysql\hive格式文本

4、postman模块，当爬取异常会触发邮件告警


注：目前该版本不集成调度模块，调度模块请参考Eclocker程序，这是一个独立的调度程序，目前主要用于备份、ETL等任务。

