from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, concat, lit
import os

def main():
    spark = SparkSession.builder.appName('Preprocess-transactions') \
        .master('spark://spark:7077') \
    .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.csv('/data/raw/transactions_data.csv', header=True, inferSchema=True)

    df = df.withColumn('transaction_month', concat(year(col('date')), lit('-'), month(col('date'))))

    distinct_months = df.select('transaction_month').distinct().collect()

    print(f'Number of distinct months: {len(distinct_months)}')
    for i, month_value in enumerate(distinct_months):
        month_df = df.filter(col('transaction_month') == month_value['transaction_month'])
        month_df = month_df.drop('transaction_month')
        month_filename = f'/data/source/transactions-{month_value['transaction_month']}'
        month_df.coalesce(1).write.csv(month_filename, header=True)
        print(f'Month: {month_value['transaction_month']}, Count: {month_df.count()}')
        print(f'Output: {month_filename}')
        files = os.listdir(month_filename)
        ugly_file_name = list(filter(lambda a: a.startswith('part') and a.endswith('.csv'), files))[0]
        os.rename(f'{month_filename}/{ugly_file_name}', f'{month_filename}/{month_filename.split('/')[-1]}.csv')
        print(f'Completed {i + 1} / {len(distinct_months)} - {((i + 1) / len(distinct_months)) * 100}%')
        
    print('Data Write complete!')

    spark.stop()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'ERROR: {e}')
        raise