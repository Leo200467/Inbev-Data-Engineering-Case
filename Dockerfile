
FROM apache/airflow:2.9.2

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.9.2

USER root

RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

ENV JAVA_HOME='/usr/lib/jvm/java-11-openjdk-amd64'

COPY ./scripts/entrypoint.sh /usr/local/
RUN chmod +x /usr/local/entrypoint.sh

USER $AIRFLOW_UID

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-apache-spark" \
    pyspark \
    py4j \
    unidecode