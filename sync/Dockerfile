FROM ubuntu:20.04

RUN apt-get update
RUN apt-get install -y rsync nano

WORKDIR /app
COPY . .

CMD bash sync.sh