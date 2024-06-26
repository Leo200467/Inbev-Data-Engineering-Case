# First-time build can take upto 10 mins.

FROM apache/airflow

ENV AIRFLOW_HOME=/opt/airflow

USER root
# git gcc g++ -qqq

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" pyspark
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" py4j
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" unidecode

# RUN sudo /usr/local/bin/python -m pip install --upgrade pip
# RUN sudo /usr/local/bin/python -m pip install pyspark
# RUN sudo /usr/local/bin/python -m pip install py4j
# RUN sudo /usr/local/bin/python -m pip install apache-airflow-providers-apache-spark
# RUN sudo /usr/local/bin/python -m pip install unidecode
# RUN pip uninstall  --yes azure-storage && pip install -U azure-storage-blob apache-airflow-providers-microsoft-azure==1.1.0
# RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install azure-storage-file-datalake
# RUN export PATH=/Users/<you>/Library/Python/3.8/bin:$PATH

USER root

RUN sudo apt update
RUN sudo apt install default-jdk -y
RUN export JAVA_HOME='/usr/lib/jvm/java-8-openjdk-amd64'
RUN export SPARK_HOME='/opt/spark'

COPY ./scripts/entrypoint.sh /usr/local/

RUN ["chmod", "+x", "/usr/local/entrypoint.sh"]

USER $AIRFLOW_UID