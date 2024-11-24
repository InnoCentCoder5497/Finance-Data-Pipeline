FROM docker.io/bitnami/spark:3.5.3
ENV PYTHONUNBUFFERED=1

COPY watchdog/main.py watcher.py

RUN pip install watchdog

ENTRYPOINT [ "/opt/bitnami/scripts/spark/entrypoint.sh" ]
CMD [ "bash", "-c", "python watcher.py & /opt/bitnami/scripts/spark/run.sh" ]