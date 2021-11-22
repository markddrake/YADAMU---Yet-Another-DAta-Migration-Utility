docker build -t yadamu/svr-2022:vs22             --file=z:\Build\YADAMU\Windows\dockerfile.2022.vs22         z:\Build\YADAMU\Windows
docker build -t yadamu/svr-2022:vs13             --file=z:\Build\YADAMU\Windows\dockerfile.2022.vs13         z:\Build\YADAMU\Windows
docker build -t yadamu/svr-2019:vs22             --file=z:\Build\YADAMU\Windows\dockerfile.2019.vs22         z:\Build\YADAMU\Windows
docker build -t yadamu/svr-2019:vs13             --file=z:\Build\YADAMU\Windows\dockerfile.2019.vs13         z:\Build\YADAMU\Windows
docker build -t yadamu/svr-2016:vs22             --file=z:\Build\YADAMU\Windows\dockerfile.2016.vs22         z:\Build\YADAMU\Windows
docker tag yadamu/svr-2022:vs22   yadamu/svr:vs22   
docker tag yadamu/svr-2022:vs13   yadamu/svr:vs13   

docker build -t yadamu/mssql:2019                --file=z:\Build\YADAMU\MsSQL\Windows\2019\dockerfile        z:\Build\YADAMU\MsSQL\Windows\2019
docker build -t yadamu/mssql:2019-CU13           --file=z:\Build\YADAMU\MsSQL\Windows\2019\dockerfile.CU13   z:\Build\YADAMU\MsSQL\Windows\2019
docker build -t yadamu/mssql:2017                --file=z:\Build\YADAMU\MsSQL\Windows\2017\dockerfile        z:\Build\YADAMU\MsSQL\Windows\2017
docker build -t yadamu/svr-2016/mssql:2017       --file=z:\Build\YADAMU\MsSQL\Windows\2017\dockerfile.2016   z:\Build\YADAMU\MsSQL\Windows\2017
docker build -t yadamu/svr-2016/mssql:2017-CU26  --file=z:\Build\YADAMU\MsSQL\Windows\2017\dockerfile.CU26   z:\Build\YADAMU\MsSQL\Windows\2017
docker build -t yadamu/svr-2016/mssql:2014       --file=z:\Build\YADAMU\MsSQL\Windows\2014\dockerfile        z:\Build\YADAMU\MsSQL\Windows\2014

docker build -t yadamu/mysql:8                   --file=z:\Build\YADAMU\mysql\windows\08\dockerfile          z:\Build\YADAMU\mysql\windows\08
docker build -t yadamu/mariadb:10                --file=z:\Build\YADAMU\mariadb\windows\10\dockerfile        z:\Build\YADAMU\mariadb\windows\10
docker build -t yadamu/mongodb:5                 --file=z:\Build\YADAMU\mongodb\windows\05\dockerfile        z:\Build\YADAMU\mongodb\windows\05

docker build -t yadamu/oracle:21.3.0             --file=z:\Build\YADAMU\oracle\Windows\21.3\dockerfile       z:\Build\YADAMU\oracle\Windows\21.3
docker build -t yadamu/oracle:19.3.0             --file=z:\Build\YADAMU\oracle\Windows\19.3\dockerfile       z:\Build\YADAMU\oracle\Windows\19.3
docker build -t yadamu/oracle:18.3.0             --file=z:\Build\YADAMU\oracle\Windows\18.3\dockerfile       z:\Build\YADAMU\oracle\Windows\18.3
docker build -t yadamu/oracle:12.2.0             --file=z:\Build\YADAMU\oracle\Windows\12.2\dockerfile       z:\Build\YADAMU\oracle\Windows\12.2
docker build -t yadamu/oracle:11.2.0.4           --file=z:\Build\YADAMU\oracle\Windows\11.2.0.4\dockerfile   z:\Build\YADAMU\oracle\Windows\11.2.0.4
docker build -t yadamu/oracle:11.2.0.1           --file=z:\Build\YADAMU\oracle\Windows\11.2.0.1\dockerfile   z:\Build\YADAMU\oracle\Windows\11.2.0.1
						               								           
docker build -t yadamu/postgres:14              --file=z:\Build\YADAMU\postgres\windows\14\dockerfile       z:\Build\YADAMU\postgres\windows\14

