for /f "tokens=*" %%i in ('tzutil /g') do set CTZ=%%i
tzutil /s UTC
@set UTC=
for /f  "skip=1 delims=" %%i in ('WMIC OS GET LocalDateTime') do if not defined UTC set UTC=%%i
tzutil /s "%CTZ%_dstoff"
REM tzutil /g
@set UTC=%UTC:~0,4%-%UTC:~4,2%-%UTC:~6,3%T%UTC:~8,6%Z