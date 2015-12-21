# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: spider.py
# Description:

# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-12-21 22:09:36
# Last:
__author__ = 'yuens'
################################### PART1 IMPORT ######################################
from spider.class_initialization_and_load_parameter import *
from spider.class_create_database_table import *
from spider.class_create_spark import *

################################ PART3 MAIN ###########################################
def main():
    # class_initialization_and_load_parameter
    # Initialization
    config_data_dir = "./config.ini"
    # load parameters
    ParameterLoader = InitializationAndLoadParameter()

    seed_urls, database_name,\
    link_table_name, page_table_name,\
    pyspark_app_name = ParameterLoader.load_parameter(config_data_dir = config_data_dir)




    # class_create_database_table
    database_name = "WebDB"
    page_table_name = "page_table"
    link_table_name = "link_table"
    Creater = CreateDatabaseTable()
    Creater.create_database(database_name = database_name)
    Creater.create_table(database_name = database_name,\
                         link_table_name = link_table_name,\
                         page_table_name = page_table_name)


    # class_create_spark
    SparkCreator = CreateSpark(pyspark_app_name = pyspark_app_name)
    pyspark_sc = SparkCreator.return_spark_context()

################################ PART4 EXECUTE ##################################
if __name__ == "__main__":
    main()