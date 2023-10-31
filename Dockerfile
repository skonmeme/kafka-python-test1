FROM python:3.11-alpine

RUN apk update && apk add python3-dev gcc libc-dev libffi-dev librdkafka-dev
RUN apk add build-base

WORKDIR /usr/src/app

COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY src/* .
