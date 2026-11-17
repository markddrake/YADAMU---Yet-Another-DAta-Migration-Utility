# The GitHub repository's root Dockerfile: builds the YADAMU command line straight from Git, for example
#   docker build -t yadamu https://github.com/markddrake/YADAMU---Yet-Another-DAta-Migration-Utility.git
# NODE_VERSION picks the node image tag (lts by default), e.g. --build-arg NODE_VERSION=22.
# Source: docker/build/github/Dockerfile in the development repository; publish.list publishes it as /Dockerfile.
# It follows docker/build/environment + docker/build/yadamu, but GitHub has no package-lock.json, so npm install resolves the versions.
ARG NODE_VERSION=lts
FROM node:${NODE_VERSION}
WORKDIR /usr/src/YADAMU

ADD https://download.oracle.com/otn_software/linux/instantclient/2326300/oracle-instantclient-basic-23.26.3.0.0-1.el9.x86_64.rpm .
COPY src/package.json ./

# Debian 13 (trixie) renamed libaio1 to libaio1t64 (libaio.so.1t64); Instant Client loads libaio.so.1, so link it.
# Older node tags are Debian 12 (bookworm), which still has libaio1.
RUN apt-get update \
 && if apt-cache show libaio1t64 >/dev/null 2>&1; then LIBAIO=libaio1t64; else LIBAIO=libaio1; fi \
 && apt-get install -y --no-install-recommends $LIBAIO alien python3 make g++ \
 && rm -rf /var/lib/apt/lists/* \
 && if [ -e /usr/lib/x86_64-linux-gnu/libaio.so.1t64 ]; then ln -sf libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1; fi \
 && alien -i --scripts ./oracle-instantclient-basic-23.26.3.0.0-1.el9.x86_64.rpm \
 && rm ./oracle-instantclient-basic-23.26.3.0.0-1.el9.x86_64.rpm \
 && npm install --no-fund --no-audit --loglevel=warn \
 # sqlite3's prebuilt Linux binary can need a newer glibc than the base image provides
 && npm rebuild sqlite3 --build-from-source \
 # Fail the build if npm skipped a native install script (see allowScripts in src/package.json)
 && for m in sqlite3 ibm_db oracledb; do node -e "require('$m'); console.log('$m loads')" || exit 1; done \
 && node --version

COPY bin bin
COPY src src
ENV YADAMU_HOME=/usr/src/YADAMU
ENV YADAMU_BIN=$YADAMU_HOME/bin
ENV YADAMU_SRC=$YADAMU_HOME/src
ENV PATH=$PATH:$YADAMU_BIN
RUN mkdir mnt \
 && cd bin \
 && chmod +x *.sh \
 && ln -s export.sh yadamuExport \
 && ln -s import.sh yadamuImport \
 && ln -s upload.sh yadamuUpload \
 && ln -s copy.sh yadamuCopy \
 && ln -s load.sh yadamuLoad \
 && ln -s unload.sh yadamuUnload \
 && ln -s compare.sh yadamuCompare \
 && ln -s yadamu.sh yadamu \
 && ln -s decrypt.sh yadamuDecrypt \
 && ln -s encrypt.sh yadamuEncrypt
ENTRYPOINT ["sleep","365d"]
