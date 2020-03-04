import sys
import os
import ConfigParser as cp
import shutil
from pyspark import SparkContext,SQLContext,SparkConf

#get variables from parameter passed to script
env = 'dev'

#get variables from config File
props = cp.RawConfigParser()
props.read('../../resources/application.properties')


input_data_dir = props.get(env,'input_data_directory')
out_data_dir = props.get(env,'output_data_directory')

#
conf = SparkConf().setAppName("SparkPractice").setMaster("local")
sc = SparkContext(conf=conf)

rawOrders_data = sc.textFile(input_data_dir + '/orders/part-00000')
rawCustomers_data = sc.textFile(input_data_dir + '/customers/part-00000')

mapOrders = rawOrders_data.map(lambda l : (int(l.split(",")[2]),l))
mapCustomers =rawCustomers_data.map(lambda l : (int(l.split(",")[0]), l))

customerWithOrders = mapCustomers.leftOuterJoin(mapOrders)

inactiveCustomers = customerWithOrders.filter( lambda l : l[1][1]==None)

finalResult= inactiveCustomers.map(lambda l : l[1][0].split(",")[1]+"," +l[1][0].split(",")[2])

if os.path.exists(out_data_dir):
    shutil.rmtree(out_data_dir)

finalResult.coalesce(1).saveAsTextFile(out_data_dir)