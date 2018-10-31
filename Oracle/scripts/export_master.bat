@set TNS=%~1
@set MODE=DATA_ONLY
MKDIR ..\JSON\%TNS%\%MODE%
call scripts\export_oracle ..\JSON\%TNS%\%MODE% %TNS% "" "" %MODE%
@set MODE=DDL_ONLY
MKDIR ..\JSON\%TNS%\%MODE%
call scripts\export_oracle ..\JSON\%TNS%\%MODE% %TNS% "" "" %MODE%
@set MODE=DDL_AND_DATA
MKDIR ..\JSON\%TNS%\%MODE%
call scripts\export_oracle ..\JSON\%TNS%\%MODE% %TNS% "" "" %MODE%
