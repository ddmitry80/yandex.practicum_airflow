FROM apache/airflow:2.2.3-python3.8

USER airflow
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && \
    pip install --upgrade setuptools \
    pip install --no-cache-dir airflow-clickhouse-plugin \
    pip install --no-cache-dir \
  --upgrade-strategy only-if-needed \
  --user -r requirements.txt