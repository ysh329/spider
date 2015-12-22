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

        #[bio network]
        seed_urls = conf.get("data", "seed_urls")
        logging.info("seed_urls:{0}".format(seed_urls))

        #[basic]
        database_name = conf.get("basic", "database_name")
        logging.info("database_name:{0}".format(database_name))

        link_table_name = conf.get("basic", "link_table_name")
        logging.info("link_table_name:{0}".format(link_table_name))

        page_table_name = conf.get("basic", "page_table_name")
        logging.info("page_table_name:{0}".format(page_table_name))

        pyspark_app_name = conf.get("basic", "pyspark_app_name")
        logging.info("pyspark_app_name:{0}".format(pyspark_app_name))

        return seed_urls, database_name, link_table_name, page_table_name, pyspark_app_name


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