FROM base/public/python:3.7-slim

ARG AIRFLOW_VERSION=1.10.10
ARG AIRFLOW_USER_HOME=/var/lib/airflow
ARG AIRFLOW_MODULES=crypto,kubernetes,postgres,gcp_api,jdbc,mysql,s3
ARG BUILD_DEPS="freetds-dev libkrb5-dev libsasl2-dev libssl-dev libffi-dev libpq-dev default-libmysqlclient-dev build-essential"

ENV AIRFLOW_HOME=$AIRFLOW_USER_HOME

COPY requirements.txt /requirements.txt

RUN mkdir $AIRFLOW_USER_HOME && \
  apt-get update && \
  apt-get install -yqq --no-install-recommends ${BUILD_DEPS} && \
  pip install --no-cache-dir apache-airflow[${AIRFLOW_MODULES}]==${AIRFLOW_VERSION} && \
  pip install --no-cache-dir -r /requirements.txt && \
  apt-get purge --auto-remove -yqq ${BUILD_DEPS} && \
  apt-get autoremove -yqq --purge && \
  rm -rf /var/lib/apt/lists/*

WORKDIR $AIRFLOW_USER_HOME

