import ConfigParser as cp
import sys,os,shutil
from pyspark.sql import SparkSession

props =cp.RawConfigParser()
props.read('../../resources/application.properties')

env=sys.argv[1]

executionMode =  props.get(env,"execution_mode")
input_data_dir =  props.get(env,"input_data_directory")
out_data_dir = props.get(env,"output_data_directory")

spark = SparkSession.builder.appName("SparkCrimeAnalysis").master(executionMode).getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions","5")


rawCrimeData = spark.read.format('csv'). \
    option('sep',','). \
    option("header",False). \
    schema('ID string ,CaseNumber string, \
    Date string, Block string, IUCR string,PrimaryType string, \
    Description string,LocationDescription string,Arrest string, \
    Domestic string,Beat string,District string,Ward string, \
    CommunityArea string,FBICode string,XCoordinate string,YCoordinate string, \
    Year string,UpdatedOn string,Latitude string,Longitude string,Location string'). \
    load(input_data_dir + '/crimes.csv')

# Header = str(rawCrimeData.first())
#
# rawCrimeDataWithoutHeader = rawCrimeData.filter(lambda l : l != Header)

rawCrimeData.registerTempTable('rawCrimeData')
resultMonthlyData =  spark.sql('''Select PrimaryType, Substr(Date,1,2)|| Substr(Date,6,4) as Date, count(*) as cnt
             from rawCrimeData
             where PrimaryType<>'Primary Type'
             group by PrimaryType, Substr(Date,1,2)|| Substr(Date,6,4)
             order by Substr(Date,1,2)|| Substr(Date,6,4), cnt desc''')

result = resultMonthlyData.rdd.map(lambda l: str(l[0]) + "\t" + str(l[1]) + "\t" + str(l[2]))
# print (result.first())

if os.path.exists(out_data_dir):
    shutil.rmtree(out_data_dir)
result.coalesce(1).saveAsTextFile(out_data_dir,compressionCodecClass='org.apache.hadoop.io.compress.GzipCodec')

