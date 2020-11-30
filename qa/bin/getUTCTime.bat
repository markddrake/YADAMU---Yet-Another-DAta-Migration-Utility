for /f "tokens=*" %%i in ('tzutil /g') do set CTZ=%%i
tzutil /s UTC
set YADAMU_TIMESTAMP=
for /f  "skip=1 delims=" %%i in ('WMIC OS GET LocalDateTime') do if not defined YADAMU_TIMESTAMP set YADAMU_TIMESTAMP=%%i
tzutil /s "%CTZ%_dstoff"
REM tzutil /g
set YADAMU_TIMESTAMP=%YADAMU_TIMESTAMP:~0,4%-%YADAMU_TIMESTAMP:~4,2%-%YADAMU_TIMESTAMP:~6,2%T%YADAMU_TIMESTAMP:~8,6%.000Z