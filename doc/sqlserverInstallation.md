# Installing Yadamu Database support for Microsoft SQL Server

Procedures are installed using the script file src/install/mssql/sql/YADAMU_IMPORT.sql.
Procedures should be installed by the system administrator. 
Packages are installed in the master database.
For SQL Server 2014 use the script located in src/install/mssql/sql/2014/YADAMU_IMPORT.sql.

```bat
sqlcmd -Usa  -Stcp:yadamu-db1,1433 -I -i src\install\mssql\sql\YADAMU_IMPORT.sql
```

```
Password: Changed database context to 'master'.
YADAMU_INSTANCE_ID                   YADAMU_INSTALLATION_TIMESTAMP
------------------------------------ -----------------------------
9F3CF7B9-92B4-4CBD-9633-CB348A532CEE 2021-03-15T02:04:40+00:00

(1 rows affected)
```