from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, lower
from utils.db_utils import write_rows_to_db
import argparse
import shutil
import os

spark = None

def main(args):
    landing_file = args.filename
    processed_dir = '/data/processed'
    processed_file = os.path.join(processed_dir, os.path.basename(landing_file))
    
    users_df = spark.read.csv(landing_file, header = True, inferSchema = True)

    column_list = [
        ('client_id', 'int'),
        ('gender', 'char'),
        ('birth_year', 'int'),
        ('birth_month', 'int'),
        ('retirement_age', 'int'),
        ('address', 'string'),
        ('latitude', 'double'),
        ('longitude', 'double'),
        ('per_capita_income', 'double'),
        ('yearly_income', 'double'),
        ('total_debt', 'double'),
        ('credit_score', 'int'),
        ('num_credit_cards', 'int')
    ]
    
    column_order = [value[0] for value in column_list]
    column_with_types = [f'{value[0]}:{value[1]}' for value in column_list]
    column_with_types = ','.join(column_with_types)
    
    print('Performing Transformations')
    # transform gender column as Female = F, Male = U, otheriwse = U
    df = users_df.withColumn('gender', when(lower(col('gender')) == 'female', 'F') \
                             .when(lower(col('gender')) == 'male', 'M') \
                             .otherwise('U'))
    # Create new column for per_capita_income without $ sign
    df = df.withColumn('per_capita_income', regexp_replace("per_capita_income", r"^\$", "").cast('double'))
    # Create new column for yearly_income without $ sign
    df = df.withColumn('yearly_income', regexp_replace("yearly_income", r"^\$", "").cast('double'))
    # Create new column for total_debt without $ sign
    df = df.withColumn('total_debt', regexp_replace("total_debt", r"^\$", "").cast('double'))
    # rename id to client_id
    df = df.withColumnRenamed('id', 'client_id')
    # drop excess columns and select in order
    df = df.drop('current_age')
    df = df.select(*column_order).dropDuplicates()
    
    print(f'Loaded {df.count()} Records in warehouse')
    write_rows_to_db(df, 'users', mode='overwrite')
    
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
            