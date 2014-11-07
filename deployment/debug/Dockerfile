FROM balihoo/os

WORKDIR /opt/balihoo/fulfillment

ADD config /opt/balihoo/fulfillment/config
ADD ffinstall.py /opt/balihoo/fulfillment/ffinstall.py
ADD fulfillment.jar /opt/balihoo/fulfillment/fulfillment.jar
ADD launcher.py /opt/balihoo/fulfillment/launcher.py
ADD splogger.py /opt/balihoo/fulfillment/splogger.py
ADD vesetup /opt/balihoo/fulfillment/vesetup

RUN chmod +x ffinstall.py

CMD python ffinstall.py && tail -F /var/log/balihoo/fulfillment/*.log

