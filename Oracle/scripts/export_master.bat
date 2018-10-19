@set TNS=%~1
@set MODE=DATA_ONLY
call scripts\export_oracle ..\JSON\%TNS%\%MODE% %TNS% "" "" %MODE%
@set MODE=DDL_ONLY
call scripts\export_oracle ..\JSON\%TNS%\%MODE% %TNS% "" "" %MODE%
@set MODE=DDL_AND_DATA
call scripts\export_oracle ..\JSON\%TNS%\%MODE% %TNS% "" "" %MODE%
