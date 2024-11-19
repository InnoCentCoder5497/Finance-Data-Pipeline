from random import random
from operator import add

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('test-') \
    .master('spark://spark:7077') \
.getOrCreate()

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://spark_cluster-db-warehouse-1:5432/postgres") \
    .option("dbtable", "sample") \
    .option("user", "postgres") \
    .option("password", "postgres_pass") \
    .load()
jdbcDF.show()

spark.stop()
