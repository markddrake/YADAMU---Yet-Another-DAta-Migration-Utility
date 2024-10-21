@echo off
REM Run from YADAMU_HOME
if not defined YADAMU_HOME set YADAMU_HOME=%CD%
node %YADAMU_HOME%\src\node\cli\copy.js %*
