FROM yadamu/base:latest
WORKDIR /usr/src/YADAMU/mnt
COPY dockerfiles/regression/tnsnames.ora .
COPY dockerfiles/regression/connections.json .
COPY dockerfiles/server/schemas.json .
COPY dockerfiles/server/configuration.json .
ENV PORT 3000
EXPOSE 3000
ENV TNS_ADMIN=/usr/src/YADAMU/mnt
ENTRYPOINT ["node", "/usr/src/YADAMU/app/YADAMU_SVR/node/main.js", "/usr/src/YADAMU/mnt/configuration.json"]
CMD []
