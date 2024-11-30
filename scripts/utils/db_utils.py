# Utility functions for working with postgress db using spark

def write_rows_to_db(df, table_name, mode='append'):
    df.distinct().write.format("jdbc") \
        .option("url", "jdbc:postgresql://spark_cluster-db-warehouse-1:5432/postgres") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres_pass") \
        .mode(mode) \
        .save()

def read_rows_to_df(table_name, query = None, sc = None):
    if not query:
        query = f'select * from {table_name}'
        
    df = sc.read.format('jdbc') \
        .option("url", "jdbc:postgresql://spark_cluster-db-warehouse-1:5432/postgres") \
        .option("query", query) \
        .option("user", "postgres") \
        .option("password", "postgres_pass") \
        .load()
    print(f'Loaded {df.count()} rows from {table_name}')
    return df