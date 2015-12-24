# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_initialization_and_load_parameter.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-12-21 21:55:16
# Last:
__author__ = 'yuens'
################################### PART1 IMPORT ######################################
import logging
import ConfigParser
import time
################################### PART2 CLASS && FUNCTION ###########################
class InitializationAndLoadParameter(object):
    def __init__(self):
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
        logging.info("START CLASS {class_name}.".format(class_name = InitializationAndLoadParameter.__name__))



    def __del__(self):
        logging.info("Success in quiting MySQL.")
        logging.info("END CLASS {class_name}.".format(class_name = InitializationAndLoadParameter.__name__))

        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = InitializationAndLoadParameter.__name__, delta_time = self.end - self.start))



    def load_parameter(self, config_data_dir):
        conf = ConfigParser.ConfigParser()
        conf.read(config_data_dir)

        #[data]
        seed_urls = conf.get("data", "seed_urls")
        logging.info("seed_urls:{0}".format(seed_urls))
        seed_url_list = map(lambda seed_url: seed_url.strip(), seed_urls.split(","))
        logging.info("seed_url_list:{0}".format(seed_url_list))
        logging.info("type(seed_url_list):{0}".format(type(seed_url_list)))


        crawl_layer_num = int(conf.get("data", "crawl_layer_num"))
        logging.info("crawl_layer_num:{0}".format(crawl_layer_num))
        logging.info("type(crawl_layer_num):{0}".format(type(crawl_layer_num)))

        #[basic]
        database_name = conf.get("basic", "database_name")
        logging.info("database_name:{0}".format(database_name))

        link_table_name = conf.get("basic", "link_table_name")
        logging.info("link_table_name:{0}".format(link_table_name))

        page_table_name = conf.get("basic", "page_table_name")
        logging.info("page_table_name:{0}".format(page_table_name))

        pyspark_app_name = conf.get("basic", "pyspark_app_name")
        logging.info("pyspark_app_name:{0}".format(pyspark_app_name))

        return crawl_layer_num, seed_urls, database_name, link_table_name, page_table_name, pyspark_app_name


################################### PART3 CLASS TEST ##################################
'''
# Initialization
config_data_dir = "../config.ini"
# load parameters
ParameterLoader = InitializationAndLoadParameter()

seed_urls, database_name,\
link_table_name, page_table_name,\
pyspark_app_name = ParameterLoader.load_parameter(config_data_dir = config_data_dir)
'''