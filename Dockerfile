FROM node:17.1.0
WORKDIR /usr/src/YADAMU
ADD https://download.oracle.com/otn_software/linux/instantclient/213000/oracle-instantclient-basic-21.3.0.0.0-1.x86_64.rpm .
COPY src/package*.json ./
RUN apt update \
 && apt-get -y install libaio1 \
 && apt-get -y install alien \
 && alien -i --scripts ./oracle-instantclient-basic-21.3.0.0.0-1.x86_64.rpm \
 && npm install --global electron \
 && npm install