FROM apache/superset:4.1.1

# В базовом образе нет драйвера PostgreSQL.
USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.10
USER superset

COPY --chown=superset:superset docker/superset/superset_config.py /app/docker/superset_config.py
COPY --chown=superset:superset --chmod=755 docker/superset/superset-init.sh /app/docker/superset-init.sh
COPY --chown=superset:superset docker/superset/bootstrap_dashboard.py /app/docker/bootstrap_dashboard.py

ENV SUPERSET_CONFIG_PATH=/app/docker/superset_config.py
