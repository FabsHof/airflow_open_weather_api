FROM apache/airflow:3.1.3

COPY requirements.txt /requirements.txt

RUN uv pip install --no-cache-dir -r /requirements.txt