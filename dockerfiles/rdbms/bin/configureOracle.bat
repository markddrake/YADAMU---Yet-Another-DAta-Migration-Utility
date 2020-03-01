@set CONTAINER_NAME=%1
docker cp oracle/setup %CONTAINER_NAME%:/opt/oracle/diag
docker exec -it -u root -w / %CONTAINER_NAME% chown -R oracle:oinstall /opt/oracle/diag/setup
docker cp oracle/testdata %CONTAINER_NAME%:/opt/oracle/diag
docker exec -it -u root -w / %CONTAINER_NAME% chown -R oracle:oinstall /opt/oracle/diag/testdata
docker cp ../../app/install/oracle/sql %CONTAINER_NAME%:/opt/oracle/diag
docker cp ../../qa/sql/oracle/YADAMU_TEST.sql %CONTAINER_NAME%:/opt/oracle/diag/sql
docker exec -it -u root -w / %CONTAINER_NAME% chown -R oracle:oinstall /opt/oracle/diag/sql
docker exec -it %CONTAINER_NAME% /bin/bash /opt/oracle/diag/setup/configure.sh
