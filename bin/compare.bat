@echo off
REM Run from YADAMU_HOME
set YADAMU_HOME=%CD%
node %YADAMU_HOME%\src\node\cli\compare.js %*
