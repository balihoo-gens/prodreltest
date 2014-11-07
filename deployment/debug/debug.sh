#!/bin/sh

cmd=$1

if ! $(docker images | grep balihoo/os) 
then
  cat OS-Dockerfile | docker build --rm -t "balihoo/os" -
fi

docker build --rm -t balihoo/fulfillment .

docker run -e "ENV_NAME=dev" -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" --rm -i -t balihoo/fulfillment $cmd

