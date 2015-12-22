# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_create_crawler.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-12-22 11:11:31
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import logging
import time
import MySQLdb
from pyspark import SparkContext
import urllib2
from bs4 import BeautifulSoup
from operator import add
################################### PART2 CLASS && FUNCTION ###########################
class CreateCrawler(object):
    def __init__(self, database_name, pyspark_sc):
        self.start = time.clock()

        logging.basicConfig(level = logging.INFO,
                  format = '%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s',
                  datefmt = '%y-%m-%d %H:%M:%S',
                  filename = './spider.log',
                  filemode = 'a')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s')
        console.setFormatter(formatter)

        logging.getLogger('').addHandler(console)
        logging.info("START CLASS {class_name}.".format(class_name = CreateCrawler.__name__))

        # connect database
        try:
            self.con = MySQLdb.connect(host='localhost', user='root', passwd='931209', db = database_name, charset='utf8')
            logging.info("Success in connecting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in connecting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))

        # spark configure
        try:
            self.sc = pyspark_sc
            logging.info("Config spark successfully.")
        except Exception as e:
            logging.error("Config spark failed.")
            logging.error(e)



    def __del__(self):
        # close database
        try:
            self.con.close()
            logging.info("Success in quiting MySQL.")
        except MySQLdb.Error, e:
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))

        logging.info("END CLASS {class_name}.".format(class_name = CreateCrawler.__name__))
        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = CreateCrawler.__name__, delta_time = self.end))



    def get_seed_url_list(self, seed_url_list):
        return seed_url_list



    def proxy_setting(self):
        enable_proxy = True
        proxy_handler = urllib2.ProxyHandler({"http" : '180.76.151.210:80'})
        null_proxy_handler = urllib2.ProxyHandler({})
        if enable_proxy:
            opener = urllib2.build_opener(proxy_handler)
        else:
            opener = urllib2.build_opener(null_proxy_handler)
        urllib2.install_opener(opener)



    def enable_urllib2_debug_log(self, http_handle_debug_level, https_handle_debug_level):
        httpHandler = urllib2.HTTPHandler(debuglevel = http_handle_debug_level)
        httpsHandler = urllib2.HTTPSHandler(debuglevel = https_handle_debug_level)
        opener = urllib2.build_opener(httpHandler, httpsHandler)
        urllib2.install_opener(opener)
        #response = urllib2.urlopen('http://www.baidu.com')


    def crawl_from_seed_url_list(self, seed_url_list, crawl_layer_num):
        final_crawl_result_rdd = self.sc.parallelize([])
        cur_crawl_layer_num = 0

        def get_hyperlink_from_a_label(a_label):
            try:
                link_list = a_label['href']
            except Exception as e:
                link_list = None
                logging.error(e)
            return link_list

        while(cur_crawl_layer_num <= crawl_layer_num):
            cur_crawl_layer_num = cur_crawl_layer_num + 1
            seed_url_list_rdd = self.sc.parallelize(seed_url_list)
            seed_url_and_headers_list_rdd = seed_url_list_rdd\
                .map(lambda seed_url: (seed_url,\
                                       { 'User-Agent' : 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)',\
                                         'Referer' : seed_url},\
                                       )\
                     )

            request_rdd = seed_url_and_headers_list_rdd.map(lambda (seed_url, headers_dict): (seed_url, urllib2.Request(url = seed_url,\
                                                                                                                        headers = headers_dict\
                                                                                                                        )\
                                                                                              )\
                                                            )
            response_rdd = request_rdd.map(lambda (seed_url, request): (seed_url, urllib2.urlopen(request, timeout = 10)))
            try:
                cur_crawl_result_rdd = response_rdd\
                    .map(lambda (url, response): (url, response.read()))\
                    .map(lambda (url, html_doc): (url, BeautifulSoup(html_doc, 'lxml')))\
                    .map(lambda (url, soup): (url, soup.html.title.text, soup.text, map(get_hyperlink_from_a_label, soup.find_all('a'))))\
                    .map(lambda (url, title, html_doc, link_list): (url, title, html_doc, link_list, len(link_list)))
            except Exception as e:
                logging.info(e)
                continue
            logging.info("cur_crawl_result_rdd.count():{0}".format(cur_crawl_result_rdd.count()))
            final_crawl_result_rdd = final_crawl_result_rdd.union(cur_crawl_result_rdd)
            logging.info("cur_crawl_layer_num:{0}".format(cur_crawl_layer_num))
            logging.info("final_crawl_result_rdd.count():{0}".format(final_crawl_result_rdd.count()))
            seed_url_list = cur_crawl_result_rdd\
                .map(lambda (url, title, html_doc, link_list, len_list_length): link_list)\
                .reduce(add)
        logging.info("final_crawl_result_rdd.count():{0}".format(final_crawl_result_rdd.count()))
        logging.info("final_crawl_result_rdd.collect():{0}".format(final_crawl_result_rdd.collect()))
        return final_crawl_result_rdd
################################### PART3 CLASS TEST ##################################
# Initialization
pyspark_app_name = "spider"
database_name = "WebDB"
pyspark_sc = SparkContext()
seed_url_list = ['http://www.yuenshome.com']


Crawler = CreateCrawler(database_name = database_name, pyspark_sc = pyspark_sc)
seed_url_list = Crawler.get_seed_url_list(seed_url_list = seed_url_list)
Crawler.proxy_setting()
Crawler.enable_urllib2_debug_log(http_handle_debug_level = 1, https_handle_debug_level = 1)
Crawler.crawl_from_seed_url_list(seed_url_list = seed_url_list, crawl_layer_num = 2)
