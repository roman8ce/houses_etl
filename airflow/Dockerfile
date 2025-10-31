FROM apache/airflow:2.9.2-python3.10

USER root

# Установка необходимых пакетов
RUN apt-get update && \
    apt install -y default-jdk wget && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN wget https://archive.apache.org/dist/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3.tgz && \
    mkdir -p /opt/spark && \
    tar -xvf spark-3.5.7-bin-hadoop3.tgz -C /opt/spark && \
    rm spark-3.5.7-bin-hadoop3.tgz

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV SPARK_HOME=/opt/spark/spark-3.5.7-bin-hadoop3
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

COPY requirements.txt /requirements.txt
RUN chmod 777 /requirements.txt

USER airflow
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt