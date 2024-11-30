from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace
import argparse
import shutil
import os

spark = None

def write_rows_to_db(df, table_name, mode='append'):
    df.distinct().write.format("jdbc") \
        .option("url", "jdbc:postgresql://spark_cluster-db-warehouse-1:5432/postgres") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres_pass") \
        .mode(mode) \
        .save()

def read_rows_to_df(table_name, query = None):
    if not query:
        query = f'select * from {table_name}'
        
    df = spark.read.format('jdbc') \
        .option("url", "jdbc:postgresql://spark_cluster-db-warehouse-1:5432/postgres") \
        .option("query", query) \
        .option("user", "postgres") \
        .option("password", "postgres_pass") \
        .load()
    print(f'Loaded {df.count()} rows from {table_name}')
    return df

def main(args):
    landing_file = args.filename
    processed_dir = '/data/processed'
    processed_file = os.path.join(processed_dir, os.path.basename(landing_file))
    
    trans_df = spark.read.csv(landing_file, header = True, inferSchema = True)

    column_list = [
        ('trans_id', 'integer'),
        ('trans_date', 'timestamp'),
        ('client_id', 'integer'),
        ('card_id', 'integer'),
        ('trans_amount', 'double'),
        ('payment_method', 'string'),
        ('trans_type', 'string'),
        ('merchant_id', 'integer'),
        ('merchant_city', 'string'),
        ('merchant_state', 'string'),
        ('zip', 'integer'),
        ('mcc', 'integer'),
        ('trans_errors', 'string')
    ]
    
    column_order = [value[0] for value in column_list]
    column_with_types = [f'{value[0]}:{value[1]}' for value in column_list]
    column_with_types = ','.join(column_with_types)
    
    print('Performing Transformations')
    # Create new column for trans_amount without $ sign
    df = trans_df.withColumn('trans_amount', regexp_replace("amount", r"^\$", "").cast('double'))
    # create new column for trans_type = Credit, Debit, Zero
    df = df.withColumn('trans_type', when(col('trans_amount') > 0, 'CREDIT').when(col('trans_amount') == 0, 'ZERO').otherwise('DEBIT'))
    # cast zip to int
    df = df.withColumn('zip', col('zip').cast('int'))
    # rename use_chip to payment_method
    df = df.withColumnRenamed('use_chip', 'payment_method')
    # rename id to trans_id
    df = df.withColumnRenamed('id', 'trans_id')
    # rename date to trans_date
    df = df.withColumnRenamed('date', 'trans_date')
    # rename errors to trans_errors
    df = df.withColumnRenamed('errors', 'trans_errors')

    # drop excess columns and select in order
    df = df.drop('amount')
    df = df.select(column_order).dropDuplicates()
    
    hist_query = 'select distinct trans_id from transactions'
    hist_df = read_rows_to_df('transactions', query=hist_query)
    
    df = df.join(hist_df, df.trans_id == hist_df.trans_id, how='leftanti')
    print(f'Loaded {df.count()} Records in warehouse')
    write_rows_to_db(df, 'transactions', mode='append')
    # print(f'Loaded {df.count()} Records in warehouse')
    
    # print('MCC load complete')
    print(f'Moving {landing_file} to {processed_file}')
    os.makedirs(processed_dir, exist_ok=True)
    shutil.move(landing_file, processed_file)
    

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser("simple_example")
        parser.add_argument("filename", help="Input File to process", type=str)
        args = parser.parse_args()
        print(f'Processing file: {args.filename}')
        spark = SparkSession.builder.appName('Load Transactions').master('spark://spark:7077').getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        main(args)
    except Exception as e:
        print(e)
        raise
    finally:
        if spark:
            spark.stop()
            