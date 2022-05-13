FROM wurstmeister/kafka
RUN cd opt/kafka_2.13-2.8.1/bin/
RUN kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic covid_topic
# kafka-topics.sh --list --zookeeper zookeeper:2181
FROM apache/airflow:2.3.0

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr
RUN export JAVA_HOME

RUN apt-get -y install procps

USER airflow

WORKDIR /app

COPY req.txt /app

RUN pip3 install --trusted-host pypi.python.org -r req.txt