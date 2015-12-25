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
import socket
import sys
import math
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
        final_crawl_link_tuple_list = []
        final_crawl_page_rdd = self.sc.parallelize([])
        cur_crawl_layer_num = 0

        def get_title_from_soup(soup):
            try:
                title = soup.html.title.text
            except Exception as e:
                logging.error(e)
                logging.error("Failed to get title.")
                title = None
            return title

        def get_text_from_soup(soup):
            try:
                text = soup.text
            except Exception as e:
                logging.error(e)
                logging.error("Failed to get text.")
                text = None
            return text

        def get_link_list_from_soup(soup):
            a_label_list = soup.find_all('a')
            link_list = []
            for a_idx in xrange(len(a_label_list)):
                a_label = a_label_list[a_idx]
                try:
                    link_list.append(a_label['href'])
                except Exception as e:
                    logging.error(e)
                    logging.error("Failed to get link.")
                    link_list.append(None)
            return link_list

        while(cur_crawl_layer_num < crawl_layer_num):
            cur_crawl_layer_num = cur_crawl_layer_num + 1
            seed_url_list = filter(lambda url: url != None and url[:4] == 'http', seed_url_list)
            seed_url_list = map(lambda url: url.strip(), seed_url_list)
            seed_url_list_rdd = self.sc.parallelize(seed_url_list)
            seed_url_and_headers_list_rdd = seed_url_list_rdd\
                .map(lambda seed_url: (seed_url,\
                                       { 'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36',\
                                         'Referer' : seed_url},\
                                       )\
                     )

            request_rdd = seed_url_and_headers_list_rdd.map(lambda (seed_url, headers_dict): (seed_url, urllib2.Request(url = seed_url,\
                                                                                                                        headers = headers_dict\
                                                                                                                        )\
                                                                                              )\
                                                            )
            global request_rdd_num
            request_rdd_num = request_rdd.count()
            global cur_response, success_response, failure_response
            cur_response = 0
            success_response = 0
            failure_response = 0
            def get_response_and_code(request, timeout):
                global cur_response, success_response, failure_response, request_rdd_num
                cur_response = cur_response + 1
                if (cur_response % 1000 == 0 and cur_response > 998) or (cur_response == request_rdd_num-1):
                    logging.info("============== cur_response:{0} all_response:{1} cur_craw_layer_num:{2} in get_response_and_code ==============".format(cur_response, request_rdd_num, cur_crawl_layer_num))
                    logging.info("cur_response:{cur_response}, finish rate:{rate}".format(cur_response = cur_response, rate = float(cur_response)/request_rdd_num))
                    logging.info("success_rate:{success_response}".format(success_response = success_response / float(cur_response + 0.0001)))
                    logging.info("success_response:{success}, failure_response:{failure}".format(success = success_response, failure = failure_response))

                try:
                    response = urllib2.urlopen(request, timeout = 10)
                    response_code = response.getcode()
                    success_response  = success_response + 1
                except socket.error:
                    failure_response = failure_response + 1
                    errno, errstr = sys.exc_info()[:2]
                    if errno == socket.timeout:
                        logging.error("There was a timeout")
                    else:
                        logging.error("There was some other socket error")
                    response = None
                    response_code = None
                except Exception as e:
                    logging.error(e)
                    response = None
                    response_code = None
                return response, response_code
                # default return in a tuple format

            response_rdd = request_rdd\
                .map(lambda (seed_url, request): (seed_url, get_response_and_code(request, timeout = 10)))\
                .map(lambda (seed_url, (response, response_code)): (seed_url, response, response_code))
            stat_response_rdd = response_rdd\
                .map(lambda (seed_url, response, response_code): (response_code, 1))\
                .reduceByKey(add)
            logging.info("stat_response_rdd.collect():{0}".format(stat_response_rdd.collect()))
            normal_response_rdd = response_rdd\
                .map(lambda (seed_url, response, response_code): (seed_url, response))\
                .filter(lambda (seed_url, response): response != None)

            global cur_read, success_read, failure_read, normal_response_rdd_num
            cur_read = 0
            success_read = 0
            failure_read = 0
            normal_response_rdd_num = normal_response_rdd.count()
            def get_response_read(response):
                global cur_read, success_read, failure_read
                if (cur_read % 1000 == 0 and cur_read > 998) or (cur_read == normal_response_rdd_num-1):
                    logging.info("============== cur_read:{0} all_read:{1} cur_craw_layer_num:{2} in get_response_read ==============".format(cur_read, normal_response_rdd_num, cur_crawl_layer_num))
                    logging.info("cur_read:{cur_read}, finish rate:{rate}".format(cur_read = cur_read, rate = float(cur_read)/normal_response_rdd_num))
                    logging.info("success_rate:{success_read}".format(success_read = success_read / float(cur_read + 0.0001)))
                    logging.info("success_read:{success}, failure_read:{failure}".format(success = success_read, failure = failure_read))

                try:
                    html_doc = response.read()
                    success_read = success_read + 1
                except socket.error:
                    failure_read = failure_read + 1
                    errno, errstr = sys.exc_info()[:2]
                    if errno == socket.timeout:
                        logging.error("There was a timeout")
                    else:
                        logging.error("There was some other socket error")
                    html_doc = None
                return html_doc

            try:
                cur_crawl_page_rdd = normal_response_rdd\
                    .map(lambda (url, response): (url, get_response_read(response = response)))\
                    .filter(lambda (url, html_doc): html_doc != None)\
                    .map(lambda (url, html_doc): (url, BeautifulSoup(html_doc, 'lxml')))\
                    .map(lambda (url, soup): (url, get_title_from_soup(soup), get_text_from_soup(soup), get_link_list_from_soup(soup)))\
                    .filter(lambda (url, title, html_doc, link_list): title != None and html_doc != None and link_list != None)\
                    .map(lambda (url, title, html_doc, link_list): (url, title, html_doc, link_list, len(link_list)))\
                    .map(lambda (url, title, html_doc, link_list, link_list_length): (url, title, html_doc, filter(lambda link_url: link_url != None and link_url[:4] == "http", link_list)))\
                    .map(lambda (url, title, html_doc, link_list): (url, title, html_doc, link_list, len(link_list)))
            except Exception as e:
                logging.info(e)
                continue
            logging.info("cur_crawl_page_rdd.count():{0}".format(cur_crawl_page_rdd.count()))
            final_crawl_page_rdd = final_crawl_page_rdd.union(cur_crawl_page_rdd)
            logging.info("cur_crawl_layer_num:{0}".format(cur_crawl_layer_num))
            logging.info("final_crawl_page_rdd.count():{0}".format(final_crawl_page_rdd.count()))

            cur_crawl_link_tuple_list = cur_crawl_page_rdd\
                .map(lambda (url, title, html_doc, link_list, link_list_length): map(lambda link_url: (url, link_url), link_list))\
                .reduce(add)
            logging.info("len(cur_crawl_link_tuple_list):{0}".format(len(cur_crawl_link_tuple_list)))
            logging.info("cur_crawl_link_tuple_list[:3]:{0}".format(cur_crawl_link_tuple_list[:3]))
            final_crawl_link_tuple_list.extend(cur_crawl_link_tuple_list)

            seed_url_list = cur_crawl_page_rdd\
                .map(lambda (url, title, html_doc, link_list, len_list_length): link_list)\
                .reduce(add)
            '''
            for i in xrange(len(seed_url_list)):
                print i, seed_url_list[i]

            for i in xrange(len(final_crawl_link_tuple_list)):
                print i, final_crawl_link_tuple_list[i]
            '''
        logging.info("final_crawl_page_rdd.count():{0}".format(final_crawl_page_rdd.count()))
        logging.info("len(final_crawl_link_tuple_list):{0}".format(len(final_crawl_link_tuple_list)))
        return final_crawl_page_rdd, final_crawl_link_tuple_list


    def save_page_and_link_result_to_database(self, database_name, link_table_name, page_table_name, final_crawl_page_rdd, final_crawl_link_tuple_list):

        # generate insert sql for page table
        def insert_sql_generator_for_page_table(database_name, page_table_name, page_url, page_title, page_html_doc, page_link_num):
            try:
                sql = "INSERT INTO {database_name}.{table_name}(page_url, page_title, page_html_doc, page_link_num) " \
                      "VALUES " \
                      "('{page_url}', '{page_title}', '{page_html_doc}', {page_link_num})"\
                      .format(database_name = database_name, table_name = page_table_name,\
                              page_url = page_url,\
                              page_title = page_title.encode("utf8").replace('"', ".").replace("'", ".").replace(' ', ''),\
                              page_html_doc = page_html_doc.encode("utf8").replace('"', ".").replace("'", ".").replace(' ', ''),\
                              page_link_num = page_link_num)
            except Exception as e:
                logging.error(e)
                sql = None
            return sql
        
        insert_sql_for_page_table_rdd = final_crawl_page_rdd\
            .map(lambda (url, title, html_doc, link_list, link_list_length): insert_sql_generator_for_page_table(database_name = database_name,\
                                                                                                                 page_table_name = page_table_name,\
                                                                                                                 page_url = url,\
                                                                                                                 page_title = title,\
                                                                                                                 page_html_doc = html_doc,\
                                                                                                                 page_link_num = link_list_length,\
                                                                                                                 )\
                 )\
            .filter(lambda sql: sql != None)
        logging.info("insert_sql_for_page_table_rdd.count():{0}".format(insert_sql_for_page_table_rdd.count()))
        logging.info("insert_sql_for_page_table_rdd.take(3):{0}".format(insert_sql_for_page_table_rdd.take(3)))



        # generate insert sql for link table
        def insert_sql_generator_for_link_table(database_name, link_table_name, page1_url, page2_url):
            try:
                sql = "INSERT INTO {database_name}.{table_name}(page1_url, page2_url) " \
                      "VALUES('{page1_url}', '{page2_url}')" \
                      .format(database_name = database_name, table_name = link_table_name,\
                              page1_url = page1_url, page2_url = page2_url)
            except Exception as e:
                logging.error(e)
                sql = None
            return sql

        insert_sql_for_link_table_list = map(lambda (page1_url, page2_url):\
                                                 insert_sql_generator_for_link_table(database_name = database_name,\
                                                                                     link_table_name = link_table_name,\
                                                                                     page1_url = page1_url,\
                                                                                     page2_url = page2_url,\
                                                                                     ),\
                                             final_crawl_link_tuple_list\
                                             )
        insert_sql_for_link_table_list = filter(lambda sql: sql != None, insert_sql_for_link_table_list)
        logging.info("len(insert_sql_for_link_table_list):{0}".format(len(insert_sql_for_link_table_list)))
        logging.info("insert_sql_for_link_table_list[:3]:{0}".format(insert_sql_for_link_table_list[:3]))

        # execute insert sql for page table
        cursor = self.con.cursor()
        success_insert = 0
        failure_insert = 0

        insert_sql_for_page_table_rdd_list = insert_sql_for_page_table_rdd.randomSplit([1] * int(math.ceil(float(insert_sql_for_page_table_rdd.count()) / 1000)))
        for rdd_idx in xrange(len(insert_sql_for_page_table_rdd_list)):
            logging.info("==========={0}th rdd_idx===========".format(rdd_idx))
            sub_rdd = insert_sql_for_page_table_rdd_list[rdd_idx]
            insert_sql_list = sub_rdd.collect()
            insert_sql_list_length = len(insert_sql_list)
            for sql_idx in xrange(len(insert_sql_list)):
                if (sql_idx % 1000 == 0 and sql_idx > 998) or (sql_idx == insert_sql_list_length-1):
                    logging.info("==========={0}th sql in {1}th rdd in sub_page_table_rdd_list===========".format(sql_idx, rdd_idx))
                    logging.info("sql_execute_index:{idx}, finish rate:{rate}".format(idx = sql_idx, rate = float(sql_idx+1)/ insert_sql_list_length))
                    logging.info("success_rate:{success_rate}".format(success_rate = success_insert/ float(success_insert + failure_insert + 1)))
                    logging.info("success_insert:{success}, failure_insert:{failure}".format(success = success_insert, failure = failure_insert))
                sql = insert_sql_list[sql_idx]
                try:
                    cursor.execute(sql)
                    self.con.commit()
                    success_insert = success_insert + 1
                except MySQLdb.Error, e:
                    self.con.rollback()
                    logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                    logging.error("error SQL:{0}".format(sql))
                    failure_insert = failure_insert + 1
        cursor.close()

        # execute insert sql for link table
        cursor = self.con.cursor()
        success_insert = 0
        failure_insert = 0
        insert_sql_list_length = len(insert_sql_for_link_table_list)

        for sql_idx in xrange(len(insert_sql_for_link_table_list)):
            if (sql_idx % 1000 == 0 and sql_idx > 998) or (sql_idx == insert_sql_list_length-1):
                logging.info("==========={0}th sql in sub_link_table_rdd_list===========".format(sql_idx))
                logging.info("sql_execute_index:{idx}, finish rate:{rate}".format(idx = sql_idx, rate = float(sql_idx+1)/ insert_sql_list_length))
                logging.info("success_rate:{success_rate}".format(success_rate = success_insert/ float(success_insert + failure_insert + 1)))
                logging.info("success_insert:{success}, failure_insert:{failure}".format(success = success_insert, failure = failure_insert))

            sql = insert_sql_for_link_table_list[sql_idx]
            try:
                cursor.execute(sql)
                self.con.commit()
                success_insert = success_insert + 1
            except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                logging.error("error SQL:{0}".format(sql))
                failure_insert = failure_insert + 1
        cursor.close()


################################### PART3 CLASS TEST ##################################
#'''
# Initialization
pyspark_app_name = "spider"
database_name = "WebDB"
link_table_name = "link_table"
page_table_name = "page_table"
pyspark_sc = SparkContext()
seed_url_list = ['http://www.eastmoney.com/']

crawl_layer_num = 3


Crawler = CreateCrawler(database_name = database_name, pyspark_sc = pyspark_sc)
seed_url_list = Crawler.get_seed_url_list(seed_url_list = seed_url_list)
#Crawler.proxy_setting()
#Crawler.enable_urllib2_debug_log(http_handle_debug_level = 1, https_handle_debug_level = 1)
final_crawl_page_rdd, final_crawl_link_tuple_list = Crawler\
    .crawl_from_seed_url_list(seed_url_list = seed_url_list,\
                              crawl_layer_num = crawl_layer_num)
Crawler.save_page_and_link_result_to_database(database_name = database_name,\
                                              link_table_name = link_table_name,\
                                              page_table_name = page_table_name,\
                                              final_crawl_page_rdd = final_crawl_page_rdd,\
                                              final_crawl_link_tuple_list = final_crawl_link_tuple_list)
#'''