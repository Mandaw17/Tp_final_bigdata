FROM apache/airflow:latest

USER root

COPY dags/ /opt/airflow/dags/

RUN apt-get update && \
    apt-get install -y zip


USER airflow

RUN  pip install --no-cache-dir -r /opt/airflow/dags/requirements.txt


COPY pysrc/ /opt/airflow/pysrc/


