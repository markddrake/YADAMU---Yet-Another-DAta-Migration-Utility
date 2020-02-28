FROM node:latest
WORKDIR /usr/src/YADAMU
COPY app/package*.json ./
COPY dockerfiles/software/oracle/oracle-instantclient19.5-basic_19.5.0.0.0-2_amd64.deb .
COPY app app
RUN npm install\
 && apt update \
 && apt-get install libaio1 \
 && apt install ./oracle-instantclient19.5-basic_19.5.0.0.0-2_amd64.deb \
 && mkdir mnt \
 && mkdir mnt/log \
 &&	mkdir mnt/JSON \
 && mkdir mnt/tests \ 
 &&	mkdir mnt/work \	
 && ln -s mnt/log \
 && ln -s mnt/JSON \
 && ln -s mnt/tests \
 && ln -s mnt/work 