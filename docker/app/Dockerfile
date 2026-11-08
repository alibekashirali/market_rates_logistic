FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY pyproject.toml .
COPY sql/ sql/
COPY etl/ etl/
COPY ml/ ml/
COPY tests/ tests/
COPY scripts/ scripts/

CMD ["bash", "scripts/run_all.sh"]
