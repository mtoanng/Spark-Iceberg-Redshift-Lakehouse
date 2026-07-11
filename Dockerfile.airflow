FROM apache/airflow:2.9.0

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    psycopg2-binary \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-postgres

