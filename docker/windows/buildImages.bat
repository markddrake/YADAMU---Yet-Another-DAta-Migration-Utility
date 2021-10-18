docker build -t yadamu/svr-2019:vs22             --file=c:\Docker\Build\YADAMU\Windows\dockerfile.vs22              c:\Docker\Build\YADAMU\Windows
docker build -t yadamu/svr-2019:vs13             --file=c:\Docker\Build\YADAMU\Windows\dockerfile.vs13              c:\Docker\Build\YADAMU\Windows
docker build -t yadamu/svr-2016:vs22             --file=c:\Docker\Build\YADAMU\Windows\dockerfile.svr-2016          c:\Docker\Build\YADAMU\Windows

docker build -t yadamu/mssql:2019                --file=c:\Docker\Build\YADAMU\MsSQL\Windows\2019\dockerfile        c:\Docker\Build\YADAMU\MsSQL\Windows\2019
docker build -t yadamu/mssql:2019-CU13           --file=c:\Docker\Build\YADAMU\MsSQL\Windows\2019\dockerfile.CU13   c:\Docker\Build\YADAMU\MsSQL\Windows\2019
docker build -t yadamu/mssql:2017                --file=c:\Docker\Build\YADAMU\MsSQL\Windows\2017\dockerfile        c:\Docker\Build\YADAMU\MsSQL\Windows\2017
docker build -t yadamu/svr-2016/mssql:2017       --file=c:\Docker\Build\YADAMU\MsSQL\Windows\2017\dockerfile.2017   c:\Docker\Build\YADAMU\MsSQL\Windows\2017
docker build -t yadamu/svr-2016/mssql:2017-CU26  --file=c:\Docker\Build\YADAMU\MsSQL\Windows\2017\dockerfile.CU26   c:\Docker\Build\YADAMU\MsSQL\Windows\2017
docker build -t yadamu/svr-2016/mssql:2014       --file=c:\Docker\Build\YADAMU\MsSQL\Windows\2014\dockerfile        c:\Docker\Build\YADAMU\MsSQL\Windows\2014

docker build -t yadamu/mysql:8                   --file=c:\Docker\Build\YADAMU\mysql\windows\08\dockerfile          c:\Docker\Build\YADAMU\mysql\windows\08
docker build -t yadamu/mariadb:10                --file=c:\Docker\Build\YADAMU\mariadb\windows\10\dockerfile        c:\Docker\Build\YADAMU\mariadb\windows\10
docker build -t yadamu/mongodb:5                 --file=c:\Docker\Build\YADAMU\mongodb\windows\05\dockerfile        c:\Docker\Build\YADAMU\mongodb\windows\05

docker build -t yadamu/oracle:21                 --file=c:\Docker\Build\YADAMU\oracle\Windows\21\dockerfile         c:\Docker\Build\YADAMU\oracle\Windows\21
docker build -t yadamu/oracle:19                 --file=c:\Docker\Build\YADAMU\oracle\Windows\19\dockerfile         c:\Docker\Build\YADAMU\oracle\Windows\19
docker build -t yadamu/oracle:18                 --file=c:\Docker\Build\YADAMU\oracle\Windows\18\dockerfile         c:\Docker\Build\YADAMU\oracle\Windows\18
docker build -t yadamu/oracle:12.2               --file=c:\Docker\Build\YADAMU\oracle\Windows\12.2\dockerfile       c:\Docker\Build\YADAMU\oracle\Windows\12.2
docker build -t yadamu/oracle:11.2               --file=c:\Docker\Build\YADAMU\oracle\Windows\11.2\dockerfile       c:\Docker\Build\YADAMU\oracle\Windows\11.2
						               								           
docker build -t yadamu/postgres:14              --file=c:\Docker\Build\YADAMU\postgres\windows\14\dockerfile       c:\Docker\Build\YADAMU\postgres\windows\14

