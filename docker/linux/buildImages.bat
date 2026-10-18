docker build -t yadamu/oracle:21.3.0   --file=C:\Docker\Build\YADAMU\Oracle\Linux\21.3\dockerfile     C:\Docker\Build\YADAMU\Oracle\Linux\21.3
docker build -t yadamu/oracle:19.3.0   --file=C:\Docker\Build\YADAMU\Oracle\Linux\19.3\dockerfile     C:\Docker\Build\YADAMU\Oracle\Linux\19.3
docker build -t yadamu/oracle:18.3.0   --file=C:\Docker\Build\YADAMU\Oracle\Linux\18.3\dockerfile     C:\Docker\Build\YADAMU\Oracle\Linux\18.3
docker build -t yadamu/oracle:12.2.0   --file=C:\Docker\Build\YADAMU\Oracle\Linux\12.2\dockerfile     C:\Docker\Build\YADAMU\Oracle\Linux\12.2
docker build -t yadamu/oracle:12.1.0   --file=C:\Docker\Build\YADAMU\Oracle\Linux\12.1\dockerfile     C:\Docker\Build\YADAMU\Oracle\Linux\12.1
docker build -t yadamu/oracle:11.2.0.1 --file=C:\Docker\Build\YADAMU\Oracle\Linux\11.2.0.1\dockerfile C:\Docker\Build\YADAMU\Oracle\Linux\11.2.0.1
docker build -t yadamu/oracle:11.2.0.4 --file=C:\Docker\Build\YADAMU\Oracle\Linux\11.2.0.4\dockerfile C:\Docker\Build\YADAMU\Oracle\Linux\11.2.0.4
docker build -t yadamu/postgres:18     --file=C:\Docker\Build\YADAMU\Postgres\Linux\dockerfile.18     C:\Docker\Build\YADAMU\Postgres\Linux\
docker build -t yadamu/postgres:17     --file=C:\Docker\Build\YADAMU\Postgres\Linux\dockerfile.17     C:\Docker\Build\YADAMU\Postgres\Linux\
docker build -t yadamu/db2             --file c:\Docker\Build\YADAMU\DB2\linux\dockerfile             c:\Docker\Build\YADAMU\DB2\linux
docker tag yadamu/postgres:l7 yadamu/postgres:latest
