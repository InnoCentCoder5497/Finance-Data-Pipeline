import subprocess
import argparse

TEST_SCRIPT_LOC = '/scripts/test.py'
JDBC_TEST_SCRIPT_LOC = '/scripts/test-cluster.py'
PREPARE_TRANSACTIONS_SCRIPT = '/scripts/prepare_transactions.py'
MCC_LOAD_SCRIPT = '/scripts/load_mcc.py'

def run_job(name: str) -> None :
    subprocess.run(f'docker exec -it spark_cluster-spark-1 spark-submit --driver-class-path /extra_jars/postgresql-9.4.1207.jar --master spark://spark:7077 {name}')


if __name__ == '__main__':
    run_job(MCC_LOAD_SCRIPT)
