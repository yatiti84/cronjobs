FROM python:3.8-slim AS requirements

WORKDIR /feed

COPY . .

RUN find . -type f -not -name 'requirements.txt' -exec rm -rfv '{}' \;

FROM python:3.8-slim AS build

WORKDIR /feed

RUN apt update \
    && apt install -y \
    build-essential \
    libffi-dev \
    libxml2-dev \
    libxslt-dev

COPY --from=requirements /feed .

RUN set -x \
    && for dir in */ ; \
    do if cd /feed/$dir \
    && python3 -m venv .venv \
    && . .venv/bin/activate \
    && pip3 install --upgrade pip \
    && pip3 install -r ./requirements.txt \
    && deactivate; then echo "done"; else exit 1; fi ; \
    done

FROM python:3.8-slim

WORKDIR /feed

COPY --from=build /feed .
COPY . .
