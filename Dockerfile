FROM node:latest
WORKDIR /usr/src/YADAMU
ADD https://download.oracle.com/otn_software/linux/instantclient/19600/oracle-instantclient19.6-basic-19.6.0.0.0-1.x86_64.rpm
COPY app/package*.json ./
COPY dockerfiles/software/oracle/oracle-instantclient19.5-basic_19.5.0.0.0-2_amd64.deb .
COPY app app
RUN npm install\
 && apt update \
 && apt-get install libaio1 \
 && apt-get install alien \
 && alien -i install ./oracle-instantclient19.6-basic-19.6.0.0.0-1.x86_64.rpm \
 && mkdir mnt \
 && mkdir mnt/log \
 &&	mkdir mnt/JSON \
 && mkdir mnt/tests \ 
 &&	mkdir mnt/work \	
 && ln -s mnt/log \
 && ln -s mnt/JSON \
 && ln -s mnt/tests \
 && ln -s mnt/work 