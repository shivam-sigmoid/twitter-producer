FROM apache/airflow:2.3.0

COPY req.txt /requirements.txt
RUN pip3 install -r /requirements.txt