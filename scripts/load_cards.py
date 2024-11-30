from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when, to_date, concat, last_day, lit, regexp_replace
from utils.db_utils import write_rows_to_db, read_rows_to_df
import argparse
import shutil
import os

spark = None

def main(args):
    landing_file = args.filename
    processed_dir = '/data/processed'
    processed_file = os.path.join(processed_dir, os.path.basename(landing_file))
    
    cards_df = spark.read.csv(landing_file, header = True, inferSchema = True)

    column_list = [
        ('card_id', 'int'),
        ('client_id', 'int'),
        ('brand', 'string'),
        ('type', 'string'),
        ('card_number', 'long'),
        ('cvv', 'int'),
        ('acct_open_date', 'date'),
        ('expires', 'date'),
        ('has_chip', 'char'),
        ('num_cards_issued', 'int'),
        ('credit_limit', 'double'),
        ('year_pin_last_changed', 'int'),
        ('card_on_dark_web', 'string')
    ]
    
    column_order = [value[0] for value in column_list]
    column_with_types = [f'{value[0]}:{value[1]}' for value in column_list]
    column_with_types = ','.join(column_with_types)
    
    print('Performing Transformations')
    # transform has_chip column as Y,N,U
    df = cards_df.withColumn('has_chip', when(lower(col('has_chip')) == 'yes', 'Y') \
                             .when(lower(col('has_chip')) == 'no', 'N') \
                             .otherwise('U'))
    # transform card_on_dark_web column as Y,N,U
    df = df.withColumn('card_on_dark_web', when(lower(col('card_on_dark_web')) == 'yes', 'Y') \
                                .when(lower(col('card_on_dark_web')) == 'no', 'N') \
                                .otherwise('U'))
    # Create column for credit_limit without $ sign
    df = df.withColumn('credit_limit', regexp_replace("credit_limit", r"^\$", "").cast('double'))
    # Conver expires to date
    df = df.withColumn('expires', last_day(to_date(concat(lit('01/'), col('expires')), 'dd/MM/yyyy')))
    # Conver acct_open_date to date
    df = df.withColumn('acct_open_date', to_date(concat(lit('01/'), col('acct_open_date')), 'dd/MM/yyyy'))
    # rename id to card_id
    df = df.withColumnRenamed('id', 'card_id')
    # rename card_brand to brand
    df = df.withColumnRenamed('card_brand', 'brand')
    # rename card_type to type
    df = df.withColumnRenamed('card_type', 'type')
    # select in column order
    df = df.select(*column_order).dropDuplicates()
    
    hist_query = 'select distinct card_id from cards'
    hist_df = read_rows_to_df('cards', query=hist_query, sc=spark)
    
    df = df.join(hist_df, df.card_id == hist_df.card_id, how='leftanti')
    
    print(f'Loaded {df.count()} Records in warehouse')
    write_rows_to_db(df, 'cards', mode='append')
    
    print(f'Moving {landing_file} to {processed_file}')
    os.makedirs(processed_dir, exist_ok=True)
    shutil.move(landing_file, processed_file)
    

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser("simple_example")
        parser.add_argument("filename", help="Input File to process", type=str)
        args = parser.parse_args()
        print(f'Processing file: {args.filename}')
        spark = SparkSession.builder.appName('Load Users').master('spark://spark:7077').getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        main(args)
    except Exception as e:
        print(e)
        raise
    finally:
        if spark:
            spark.stop()
            