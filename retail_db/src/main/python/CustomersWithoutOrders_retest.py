flag='0'
for i in range (1,1001):
    if i>1:
        flag='0'
        for j in range (2,i):
            if i%j == 0:
                flag='1'
                break;
    if flag=='0':
        print("number is prime " + str(i));
    else:
        print("number is non prime " + str(i));



from pyspark.sql import SparkSession
spark=SparkSession.builder.appName().master().getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions","5")


raw_data=spark.read.format('csv').option("sep",',').option("header",False).schema('',',''').load()