from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Test - Executor details') \
    .master('spark://spark:7077') \
.getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sc = spark._jsc.sc() 

result1 = sc.getExecutorMemoryStatus().keys() # will print all the executors + driver available

result2 = len([executor.host() for executor in sc.statusTracker().getExecutorInfos() ]) -1

print(result1, end ='\n')
print(result2)

spark.stop()
