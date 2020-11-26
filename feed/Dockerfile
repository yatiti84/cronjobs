FROM python:3.8-alpine

WORKDIR /feed

RUN apk update \
    && apk upgrade \
    && apk add --no-cache --virtual .build-deps \
    build-base libffi-dev libxml2-dev libxslt-dev \
    && apk add libffi libxml2 libxslt

COPY . .

RUN set -x \
    && python3 -m pip install --upgrade pip \
    && for dir in */ ; \
    do cd /feed/$dir \
    && python3 -m venv .venv \
    && source /feed/${dir%/}/.venv/bin/activate \
    && pip install -r ./requirements.txt \
    && deactivate; \
    done

RUN apk del .build-deps \
    && rm -rf /var/cache/apk/*
