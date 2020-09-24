FROM node:latest
WORKDIR /usr/src/YADAMU
ADD https://download.oracle.com/otn_software/linux/instantclient/19600/oracle-instantclient19.6-basic-19.6.0.0.0-1.x86_64.rpm .
COPY app/package*.json ./
COPY app app
RUN apt update \
 && apt-get -y install libaio1 \
 && apt-get -y install alien \
 && alien -i ./oracle-instantclient19.6-basic-19.6.0.0.0-1.x86_64.rpm \
 && npm install\
 && mkdir mnt \
 && mkdir mnt/log \
 &&	mkdir mnt/JSON \
 && mkdir mnt/tests \ 
 &&	mkdir mnt/work \	
 && ln -s mnt/log \
 && ln -s mnt/JSON \
 && ln -s mnt/tests \
 && ln -s mnt/work 