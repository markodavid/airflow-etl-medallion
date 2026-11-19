# Imagen propia de Airflow: la base oficial + las dependencias del pipeline.
# Pandas y PyYAML ya vienen en la imagen apache/airflow; aqui solo lo extra.
FROM apache/airflow:3.4.0

ARG AIRFLOW_VERSION=3.4.0

COPY requirements-airflow.txt /requirements-airflow.txt

RUN pip install --no-cache-dir -r /requirements-airflow.txt

# El codigo del pipeline se monta como volumen en desarrollo (ver docker-compose.yaml).
# Si quisieras una imagen autocontenida, descomenta:
# COPY --chown=airflow:root include/ /opt/airflow/include/
