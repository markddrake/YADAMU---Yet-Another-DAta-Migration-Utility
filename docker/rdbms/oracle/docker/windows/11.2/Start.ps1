# start the service
if ((-not (Test-Path -Path "c:\oracle\oradata"  -PathType Container)) -or (@( Get-ChildItem "c:\oracle\oradata" ).Count -eq 0)){
  Set-Location -Path $ENV:ORACLE_HOME
  Write-Output "Oracle: Creating Network"
  .\bin\netca -silent -responseFile $ENV:ORACLE_HOME\assistants\netca\netca.rsp
  Write-Output "Oracle: Creating Database"
  .\bin\dbca -silent -createDatabase -responseFile $ENV:ORACLE_HOME\dbca.rsp 
  Write-Output "Oracle: Database created"
}
Write-Output "Oracle: Container Ready. Monitoring Logs."
GET-Content -Path  C:\oracle\diag\rdbms\$ENV:ORACLE_SID\$ENV:ORACLE_SID\trace\alert_$ENV:ORACLE_SID.log  -Wait -ErrorAction 'silentlycontinue'
Wait-Event