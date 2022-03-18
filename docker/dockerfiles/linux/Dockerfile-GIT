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
COPY src src
COPY bin bin
ENV YADAMU_HOME=/usr/src/YADAMU
ENV YADAMU_BIN=$YADAMU_HOME/bin
ENV YADAMU_SRC=$YADAMU_HOME/src
ENV PATH=$PATH:$YADAMU_BIN
RUN cd bin \
 && chmod +x $YADAMU_BIN/export.sh \
 &&	ln -s $YADAMU_BIN/export.sh yadamuExport \
 && chmod +x $YADAMU_BIN/import.sh \
 &&	ln -s $YADAMU_BIN/import.sh yadamuImport \
 && chmod +x $YADAMU_BIN/upload.sh \
 &&	ln -s $YADAMU_BIN/upload.sh yadamuUpload \
 && chmod +x $YADAMU_BIN/copy.sh \
 &&	ln -s $YADAMU_BIN/copy.sh yadamuCopy \
 && chmod +x $YADAMU_BIN/load.sh \
 &&	ln -s $YADAMU_BIN/load.sh yadamuLoad \
 && chmod +x $YADAMU_BIN/unload.sh \
 &&	ln -s $YADAMU_BIN/unload.sh yadamuUnload \
 && chmod +x $YADAMU_BIN/yadamu.sh \
 &&	ln -s $YADAMU_BIN/yadamu.sh yadamu \
 && chmod +x $YADAMU_BIN/decrypt.sh \
 &&	ln -s $YADAMU_BIN/decrypt.sh yadamuDecrypt \
 && chmod +x $YADAMU_BIN/encrypt.sh \
 &&	ln -s $YADAMU_BIN/encrypt.sh yadamuEncrypt 
ENTRYPOINT ["sleep","365d"]