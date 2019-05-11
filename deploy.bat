cd C:\Deployment\YADAMU
rmdir /s /q clarinet
rmdir /s /q common
rmdir /s /q dbShared
rmdir /s /q file
rmdir /s /q mariadb
rmdir /s /q mssql
rmdir /s /q mysql
rmdir /s /q oracle
rmdir /s /q postgres
rmdir /s /q tests
rmdir /s /q utilities
mkdir clarinet
copy c:\Development\YADAMU\clarinet\*.*              clarinet
mkdir common
copy c:\Development\YADAMU\common\*.*                common
mkdir dbShared
mkdir dbShared\mysql
mkdir dbshared\dbSkeleton
copy c:\Development\YADAMU\dbShared\mysql\*.*        dbShared\mysql
copy c:\Development\YADAMU\dbShared\dbSkeleton\*.*   dbShared\dbSkeleton
mkdir file
mkdir file\node
copy c:\Development\YADAMU\file\node\*.*             file\node
mkdir mariadb
mkdir mariadb\sql
mkdir mariadb\node
copy c:\Development\YADAMU\mariadb\sql\*.*           mariadb\sql
copy c:\Development\YADAMU\mariadb\node\*.*          mariadb\node
mkdir mysql
mkdir mysql\sql
mkdir mysql\node
copy c:\Development\YADAMU\mysql\sql\*.*             mysql\sql
copy c:\Development\YADAMU\mysql\node\*.*            mysql\node
mkdir mssql
mkdir mssql\sql
mkdir mssql\node
copy c:\Development\YADAMU\mssql\sql\*.*             mssql\sql
copy c:\Development\YADAMU\mssql\node\*.*            mssql\node
mkdir postgres
mkdir postgres\sql
mkdir postgres\node
copy c:\Development\YADAMU\postgres\sql\*.*          postgres\sql
copy c:\Development\YADAMU\postgres\node\*.*         postgres\node
mkdir oracle
mkdir oracle\sql
mkdir oracle\node
copy c:\Development\YADAMU\oracle\sql\*.*            oracle\sql
copy c:\Development\YADAMU\oracle\node\*.*           oracle\node
mkdir tests
mkdir tests\mariadb
mkdir tests\mariadb\env
mkdir tests\mariadb\sql
mkdir tests\mariadb\unix
mkdir tests\mariadb\windows
copy c:\Development\YADAMU\tests\mariadb\env\*.*     tests\mariadb\env
copy c:\Development\YADAMU\tests\mariadb\sql\*.*     tests\mariadb\sql
copy c:\Development\YADAMU\tests\mariadb\unix\*.*    tests\mariadb\unix
copy c:\Development\YADAMU\tests\mariadb\windows\*.* tests\mariadb\windows
mkdir tests\mssql
mkdir tests\mssql\env
mkdir tests\mssql\sql
mkdir tests\mssql\unix
mkdir tests\mssql\windows
copy c:\Development\YADAMU\tests\mssql\env\*.*     tests\mssql\env
copy c:\Development\YADAMU\tests\mssql\sql\*.*     tests\mssql\sql
copy c:\Development\YADAMU\tests\mssql\unix\*.*    tests\mssql\unix
copy c:\Development\YADAMU\tests\mssql\windows\*.* tests\mssql\windows
mkdir tests\mysql
mkdir tests\mysql\env
mkdir tests\mysql\sql
mkdir tests\mysql\unix
mkdir tests\mysql\windows
copy c:\Development\YADAMU\tests\mysql\env\*.*     tests\mysql\env
copy c:\Development\YADAMU\tests\mysql\sql\*.*     tests\mysql\sql
copy c:\Development\YADAMU\tests\mysql\unix\*.*    tests\mysql\unix
copy c:\Development\YADAMU\tests\mysql\windows\*.* tests\mysql\windows
mkdir tests\oracle
mkdir tests\oracle\env
mkdir tests\oracle\sql
mkdir tests\oracle\unix
mkdir tests\oracle\windows
copy c:\Development\YADAMU\tests\oracle\env\*.*     tests\oracle\env
copy c:\Development\YADAMU\tests\oracle\sql\*.*     tests\oracle\sql
copy c:\Development\YADAMU\tests\oracle\unix\*.*    tests\oracle\unix
copy c:\Development\YADAMU\tests\oracle\windows\*.* tests\oracle\windows
mkdir tests\postgres
mkdir tests\postgres\env
mkdir tests\postgres\sql
mkdir tests\postgres\unix
mkdir tests\postgres\windows
copy c:\Development\YADAMU\tests\postgres\env\*.*     tests\postgres\env
copy c:\Development\YADAMU\tests\postgres\sql\*.*     tests\postgres\sql
copy c:\Development\YADAMU\tests\postgres\unix\*.*    tests\postgres\unix
copy c:\Development\YADAMU\tests\postgres\windows\*.* tests\postgres\windows
mkdir tests\oracle12c
mkdir tests\oracle12c\env
mkdir tests\oracle12c\unix
mkdir tests\oracle12c\windows
copy c:\Development\YADAMU\tests\oracle12c\env\*.*     tests\oracle12c\env
copy c:\Development\YADAMU\tests\oracle12c\unix\*.*    tests\oracle12c\unix
copy c:\Development\YADAMU\tests\oracle12c\windows\*.* tests\oracle12c\windows
mkdir tests\oracle18c
mkdir tests\oracle18c\env
mkdir tests\oracle18c\unix
mkdir tests\oracle18c\windows
copy c:\Development\YADAMU\tests\oracle18c\env\*.*     tests\oracle18c\env
copy c:\Development\YADAMU\tests\oracle18c\unix\*.*    tests\oracle18c\unix
copy c:\Development\YADAMU\tests\oracle18c\windows\*.* tests\oracle18c\windows
mkdir tests\connections
copy c:\Development\YADAMU\tests\connections\*.*      tests\connections
mkdir tests\dbRoundtrip
copy c:\Development\YADAMU\tests\dbRoundtrip\*.*      tests\dbRoundtrip
mkdir tests\export
copy c:\Development\YADAMU\tests\export\*.*           tests\export
mkdir tests\exportRoundtrip
copy c:\Development\YADAMU\tests\exportRoundtrip\*.*  tests\exportRoundtrip
mkdir tests\import
copy c:\Development\YADAMU\tests\import\*.*           tests\import
mkdir tests\node
copy c:\Development\YADAMU\tests\node\*.*             tests\node
mkdir tests\unix
copy c:\Development\YADAMU\tests\unix\*.*             tests\unix
mkdir tests\windows
copy c:\Development\YADAMU\tests\windows\*.*          tests\windows
mkdir utilities
mkdir utilities\node
copy c:\Development\YADAMU\utilities\node\*.*         utilities\node
mkdir utilities\unix
copy c:\Development\YADAMU\utilities\unix\*.*         utilities\unix
mkdir utilities\windows
copy c:\Development\YADAMU\utilities\windows\*.*      utilities\windows
