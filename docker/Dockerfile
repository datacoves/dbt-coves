ARG BASE_IMAGE="python:3.8-slim-buster"

FROM $BASE_IMAGE
ARG DIST_PATH

RUN printf "deb http://deb.debian.org/debian buster-backports main" > /etc/apt/sources.list.d/backports_git.list && \
    apt-get update \
    && apt-get dist-upgrade -y \
    && apt-get install -y --no-install-recommends \
    git-man/buster-backports \
    git/buster-backports \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install dbt and dbt-coves
RUN pip install -U pip && \
    pip install dbt-coves==0.19.1-a.8

ENV PYTHONIOENCODING=utf-8
ENV LANG C.UTF-8
WORKDIR /usr/app
VOLUME /usr/app
