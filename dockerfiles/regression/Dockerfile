FROM yadamu/base:latest
WORKDIR /usr/src/YADAMU
COPY qa/bin qa/bin
COPY qa/regression qa/regression
COPY dockerfiles/regression/bin  bin
COPY dockerfiles/regression/yadamuTestDefaults.json app/YADAMU/common/yadamuDefaults.json
COPY dockerfiles/regression/tnsnames.ora .
COPY dockerfiles/regression/connections.json qa/regression
ENV TNS_ADMIN=/usr/src/YADAMU
ENTRYPOINT ["/bin/bash", "/usr/src/YADAMU/bin/chooseTest.sh", "$YADAMU_TEST_NAME"]