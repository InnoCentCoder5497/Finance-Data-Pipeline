from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.appName('EDA').master('local[*]').getOrCreate()

MCC_CODES_LOC = '/data/raw/mcc_codes.json'

with open(MCC_CODES_LOC, 'r') as f:
    data = json.load(f)
    corrected_data = []
    for key, value in data.items():
        corrected_data.append((int(key), value))

mcc_schema = 'mcc_code: int, mcc_desc:string'
mcc_df = spark.createDataFrame(corrected_data, mcc_schema)

print(f'Loading {mcc_df.count()} Records in warehouse')

mcc_df.distinct().write.format("jdbc") \
.option("url", "jdbc:postgresql://spark_cluster-db-warehouse-1:5432/postgres") \
.option("dbtable", "mcc") \
.option("user", "postgres") \
.option("password", "postgres_pass") \
.mode("append") \
.save()

spark.stop()