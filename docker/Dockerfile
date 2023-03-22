FROM python:3.9.14-alpine3.16

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN apk update && apk add curl


COPY ./docker/test-elasticsearch.sh .
RUN chmod +x test-elasticsearch.sh

RUN apk add --update bash run-parts                                                     \
    curl                                                                                \
    && rm -rf /var/cache/apk/*                                                          \
    && curl https://codeload.github.com/kward/shunit2/tar.gz/v2.1.7 -o shunit2.tar.gz   \
    && tar -xvf shunit2.tar.gz                                                          \
    && mv shunit2-2.1.7 /bin/shunit2                                                    \
    && rm *.gz                                                                          \
    && ln -s /bin/shunit2/shunit2 /usr/sbin/shunit2

COPY . .
RUN python3 setup.py install

ENTRYPOINT ["/bin/sh", "docker/entrypoint.sh"]