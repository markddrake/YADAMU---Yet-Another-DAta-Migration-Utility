@echo off
REM Run from YADAMU_HOME
set YADAMU_HOME=%CD%
npm --prefix ./src start  %*