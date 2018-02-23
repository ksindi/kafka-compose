FROM alpine:3.7

# System setup
RUN addgroup -g 1000 app && adduser -D -G app -u 1000 app
ENV dir /home/app
ENV LC_ALL=en_US.utf-8

# System dependencies
RUN apk add --no-cache python3 python3-dev
RUN python3 -m ensurepip
RUN pip3 install --upgrade pip setuptools
RUN apk add --no-cache librdkafka \
  librdkafka-dev \
  openssl-dev \
  gcc \
  musl-dev \
  git

WORKDIR ${dir}

# App dependencies
RUN pip3 install confluent-kafka \
 tweepy

# File upload
COPY producer.py ${dir}/

RUN chown -R app: ${dir}

# App setup
USER app

ENTRYPOINT []
CMD true
