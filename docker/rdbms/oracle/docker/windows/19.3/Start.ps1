Set-Location -Path $ENV:ORACLE_HOME
$service = Get-Service -Name OracleOraDB19Home1TNSListener -ErrorAction SilentlyContinue
if($service -eq $null) {
  $NOW = Get-Date -Format "o"
  Write-Output "$NOW Oracle: Creating Network Service ""OracleOraDB19Home1TNSListener""."
  netca -silent -responseFile $ENV:ORACLE_HOME\assistants\netca\netca.rsp
  $NOW = Get-Date -Format "o"
  Write-Output "$NOW Oracle: Creating Database Service ""OracleService$ORACLE_SID""."
  dbca -J"-Doracle.assistants.dbca.validate.ConfigurationParams=false" -silent -createDatabase -responseFile $ENV:ORACLE_HOME\dbca.rsp 
  cmd /c "echo Alter Pluggable Database all save state; | sqlplus /  as sysdba"
  $NOW = Get-Date -Format "o"
  Write-Output "$NOW Oracle: Database Service ""OracleService$ORACLE_SID"" created."
}
Write-Output "$NOW Oracle: Container Ready. Monitoring Logs."
GET-Content -Path  C:\oracle\diag\rdbms\$ENV:ORACLE_SID\$ENV:ORACLE_SID\trace\alert_$ENV:ORACLE_SID.log  -Wait -ErrorAction 'silentlycontinue'
Wait-Event