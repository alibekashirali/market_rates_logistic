FROM apache/airflow:2.10.5-python3.11

# Пайплайн ставится в отдельный venv: его зависимости не смешиваются с constraints Airflow.
COPY requirements.txt /tmp/pipeline-requirements.txt
RUN python -m venv /opt/airflow/pipeline-venv \
 && /opt/airflow/pipeline-venv/bin/pip install --no-cache-dir -r /tmp/pipeline-requirements.txt

COPY --chown=airflow:root etl/ /opt/airflow/pipeline/etl/
COPY --chown=airflow:root ml/ /opt/airflow/pipeline/ml/
COPY --chown=airflow:root sql/ /opt/airflow/pipeline/sql/
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root --chmod=755 docker/airflow/entrypoint.sh /opt/airflow/airflow-entrypoint.sh

ENV PIPELINE_HOME=/opt/airflow/pipeline \
    PIPELINE_PYTHON=/opt/airflow/pipeline-venv/bin/python

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/opt/airflow/airflow-entrypoint.sh"]
