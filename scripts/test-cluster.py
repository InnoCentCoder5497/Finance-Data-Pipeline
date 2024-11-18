from random import random
from operator import add

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('PhonPi') \
    .master('spark://spark:7077') \
.getOrCreate()

df = spark.read.csv('/data/raw/transactions_data.csv')

partitions = 5
n = 100000 * partitions

def f(_: int) -> float:
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0

count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
# time.sleep(30)
print("Pi is roughly %f" % (4.0 * count / n))

print(f'Data count: {df.count()}')

spark.stop()
