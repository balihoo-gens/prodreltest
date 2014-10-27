FROM ubuntu:14.04

WORKDIR /opt/balihoo/fulfillment

ENV DEBIAN_FRONTEND noninteractive

ADD config /opt/balihoo/fulfillment/config
ADD ffinstall.py /opt/balihoo/fulfillment/ffinstall.py
ADD fulfillment.jar /opt/balihoo/fulfillment/fulfillment.jar
ADD launcher.py /opt/balihoo/fulfillment/launcher.py
ADD splogger.py /opt/balihoo/fulfillment/splogger.py
ADD vesetup /opt/balihoo/fulfillment/vesetup

RUN apt-get update
RUN apt-get install -y curl unzip default-jre gcc make autoconf libssl-dev libbz2-dev python-pip
RUN pip install boto
RUN curl https://s3.amazonaws.com/aws-cli/awscli-bundle.zip -o awscli-bundle.zip && \
    unzip -o awscli-bundle.zip && \
    ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws
RUN chmod +x ffinstall.py

# disable policy-rc to work around init failures 
RUN echo '#!/bin/sh\nexit 0' > /usr/sbin/policy-rc.d

CMD python ffinstall.py

