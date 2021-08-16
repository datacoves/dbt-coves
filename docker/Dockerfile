ARG BASE_IMAGE="python:3.8"

FROM $BASE_IMAGE
ARG DIST_PATH
ARG DBT_VERSION
ARG DBT_COVES_VERSION

# Install dbt and dbt-coves
RUN pip install -U pip && \
    pip install dbt==$DBT_VERSION && \
    pip install dbt-coves==$DBT_COVES_VERSION

ENV PYTHONIOENCODING=utf-8
ENV LANG C.UTF-8
WORKDIR /usr/app
VOLUME /usr/app
