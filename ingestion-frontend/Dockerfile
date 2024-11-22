FROM python:3.12-slim

WORKDIR /python-docker

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

ADD templates ./templates/
ADD static ./static/
COPY app.py app.py

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]