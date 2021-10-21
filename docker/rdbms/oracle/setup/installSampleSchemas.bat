cd %STAGE%
powershell -command Expand-Archive -Path  $env:STAGE\testdata\sampleSchemas.zip -DestinationPath  $env:STAGE
cd sampleSchemas
echo off
(echo.set echo on
echo.ALTER SYSTEM SET "_disable_directory_link_check"=TRUE SCOPE=SPFILE;
echo.ALTER SYSTEM SET "_kolfuseslf"=TRUE SCOPE=SPFILE;
echo.SHUTDOWN
echo.STARTUP
) | sqlplus / as sysdba
echo on
exit | sqlplus /nolog @mksample oracle oracle oracle oracle oracle oracle oracle oracle USERS TEMP log/ localhost:1521/%ORACLE_PDB%
(echo.set echo on
echo.ALTER SYSTEM SET "_disable_directory_link_check"=FALSE SCOPE=SPFILE;
echo.ALTER SYSTEM SET "_kolfuseslf"=FALSE SCOPE=SPFILE;
echo.SHUTDOWN
echo.STARTUP
) | sqlplus / as sysdba
echo on
