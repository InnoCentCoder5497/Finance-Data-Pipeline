import time
import subprocess
import os
import re

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

SPARK_SUBMIT_CMD = [
    "/opt/bitnami/spark/bin/spark-submit",
    "--master", "spark://spark:7077",
    "--driver-class-path", "/extra_jars/postgresql-42.7.4.jar",
    "--jars", "/extra_jars/postgresql-42.7.4.jar"
]

FILE_EVENT_CONFIG = [
    {
        'name': 'Load Transactions',
        'directory':'/data/landing',
        'pattern': r'^transactions.+\.csv$',
        'job': '/scripts/load_transactions.py'
    },
    {
        'name': 'Load Users',
        'directory':'/data/landing',
        'pattern': r'^users.+\.csv$',
        'job': '/scripts/load_users.py'
    }
]

# FileCreatedEvent(src_path='/srcfolder/test', dest_path='', event_type='created', is_directory=False, is_synthetic=False)
class MyEventHandler(FileSystemEventHandler):
    def on_created(self, event: FileSystemEvent) -> None:
        if not event.is_directory:    
            key = event.src_path
            directory = os.path.dirname(key)
            filename = os.path.basename(key)
            for config in FILE_EVENT_CONFIG:
                if config['directory'] == directory and re.match(config['pattern'], filename):
                    print(f'Running Spark job {config['name']} for file: {key}')
                    print(f"Command: {['spark-submit', f' {config['job']} {key}']}")
                    command = SPARK_SUBMIT_CMD + [ config['job'], key ]
                    subprocess.run(command)

event_handler = MyEventHandler()
observer = PollingObserver()
observer.schedule(event_handler, "/data/", recursive=True)
observer.start()
print('Observing')
try:
    while True:
        time.sleep(30)
finally:
    observer.stop()
    observer.join()