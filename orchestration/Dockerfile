FROM astrocrpublic.azurecr.io/runtime:3.3-9-python-3.13

# separating script env from airflow
COPY include/pipeline-requirements.txt /tmp/pipeline-requirements.txt
RUN python -m venv /usr/local/airflow/pipeline_venv \
 && /usr/local/airflow/pipeline_venv/bin/pip install --no-cache-dir -r /tmp/pipeline-requirements.txt
