from pyspark.sql import SparkSession
import json

def write_rows_to_db(df, table_name, mode='append'):
    df.distinct().write.format("jdbc") \
        .option("url", "jdbc:postgresql://spark_cluster-db-warehouse-1:5432/postgres") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres_pass") \
        .mode(mode) \
        .save()

def main():
    spark = SparkSession.builder.appName('Load MCC').master('spark://spark:7077').getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print('Correcting MCC file data')
    MCC_CODES_LOC = '/data/raw/mcc_codes.json'

    with open(MCC_CODES_LOC, 'r') as f:
        data = json.load(f)
        corrected_data = []
        for key, value in data.items():
            corrected_data.append((int(key), value))

    mcc_schema = 'mcc_code: int, mcc_desc:string'
    mcc_df = spark.createDataFrame(corrected_data, mcc_schema)

    print(f'Loading {mcc_df.count()} Records in warehouse')

    write_rows_to_db(mcc_df, 'mcc')
    
    print('MCC load complete')

    spark.stop()
    
if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
        raise