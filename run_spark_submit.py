import subprocess
import argparse

TEST_SCRIPT_LOC = '/scripts/test.py'
PREPARE_TRANSACTIONS_SCRIPT = '/scripts/prepare_transactions.py'

def run_job(name: str) -> None :
    subprocess.run(f'docker exec -it spark_cluster-spark-1 spark-submit --master spark://spark:7077 {name} ')


if __name__ == '__main__':
    run_job(PREPARE_TRANSACTIONS_SCRIPT)
