from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
import os

def main():
    spark = SparkSession.builder.appName('Preprocess-cards') \
        .master('spark://spark:7077') \
    .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.csv('/data-loc/raw/cards_data.csv', header=True, inferSchema=True)
    df = df.withColumn('year', split(col('acct_open_date'), '/').getItem(1))

    cards_acct_year = df.select('year').distinct().collect()

    for i, open_dt in enumerate(cards_acct_year):
        month_df = df.filter(col('year') == open_dt['year'])
        month_df = month_df.drop('year')
        month_filename = f'/data-loc/source/cards_data/cards-{open_dt['year']}'
        month_df.coalesce(1).write.csv(month_filename, header=True)
        print(f'Year: {open_dt['year']}, Count: {month_df.count()}')
        print(f'Output: {month_filename}')
        files = os.listdir(month_filename)
        ugly_file_name = list(filter(lambda a: a.startswith('part') and a.endswith('.csv'), files))[0]
        os.rename(f'{month_filename}/{ugly_file_name}', f'{month_filename}/{month_filename.split('/')[-1]}.csv')
        print(f'Completed {i + 1} / {len(cards_acct_year)} - {((i + 1) / len(cards_acct_year)) * 100}%')

    spark.stop()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'ERROR: {e}')
        raise