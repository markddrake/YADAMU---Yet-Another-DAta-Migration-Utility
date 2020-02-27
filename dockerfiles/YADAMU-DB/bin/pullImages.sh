docker load < /c/Docker/Images/Oracle_EE_19.3.0.0.0.tar
docker load < /c/Docker/Images/Oracle_EE_18.3.0.0.0.tar
docker load < /c/Docker/Images/Oracle_EE_12.2.0.1.0.tar
docker load < /c/Docker/Images/Oracle_EE_12.1.0.2.0.tar
docker load < /c/Docker/Images/Oracle_EE_11.2.0.1.0.tar
docker pull mysql
docker pull postgres
docker pull mariadb
docker pull mcr.microsoft.com/mssql/server:2017-latest
docker pull mcr.microsoft.com/mssql/server:2019-latest
docker images
docker system df