FROM apache/airflow:2.3.0

COPY req.txt /requirements.txt
RUN pip3 install -r /requirements.txt

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME


RUN export PYSPARK_SUBMIT_ARGS="--master local[2] pyspark-shell"

RUN apt-get -y install procps

