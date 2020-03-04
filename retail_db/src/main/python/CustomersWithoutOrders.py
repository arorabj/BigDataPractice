import sys
import os
import ConfigParser as cp
from pyspark.sql import SparkSession

#get variables from parameter passed to script
env = 'dev'

#get variables from config File
props = cp.RawConfigParser()
props.read('../../resources/application.properties')


input_data_dir = props.get(env,'input_data_directory')
out_data_dir = props.get(env,'output_data_directory')

# defining Spark Session
spark = SparkSession.builder.master("local").appName("SparkDemo").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions","1")

rawOrders_data = spark.read.format('csv').option('sep',',').schema('order_id int , order_date string , order_customer_id int,  order_status string ').load(input_data_dir + '/orders/part-00000')
rawCustomer_data = spark.read.format('csv').option('sep',',').schema('customer_id int , fname string , lname string, a string, b string, address string, city string, state string, pincode string').load(input_data_dir + '/customers/part-00000')

rawOrders_data.createTempView("rawOrders_data")
rawCustomer_data.createTempView("rawCustomer_data")

customerWithNoOrder = spark.sql("select fname ||','||lname from rawCustomer_data \
           left outer join rawOrders_data \
           on rawCustomer_data.customer_id = rawOrders_data.order_customer_id \
           Where rawOrders_data.order_customer_id is null \
           Order by rawCustomer_data.lname, rawCustomer_data.fname")

customerWithNoOrder.write.mode('overwrite').format('text').save(out_data_dir)

