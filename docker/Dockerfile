FROM python:3.9.14-alpine3.16

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN apk update && apk add curl

COPY . .

RUN python3 setup.py install

ENTRYPOINT ["/bin/sh", "docker/entrypoint.sh"]