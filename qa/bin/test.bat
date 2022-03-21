REM Run from YADAMU_HOME
set YADAMU_HOME=%CD%
set YADAMU_QA_HOME=%YADAMU_HOME%\qa
if not defined NODE_NO_WARNINGS set NODE_NO_WARNINGS=1
if not defined NODE_OPTIONS set NODE_OPTIONS="--max_old_space_size=8192"
node %YADAMU_HOME%\src\qa\cli\test.js CONFIG=%1
