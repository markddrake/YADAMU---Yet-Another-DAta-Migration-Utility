$service = Get-Service -Name postgresql-x64-14 -ErrorAction SilentlyContinue
if($service -eq $null) {
  Start-Sleep -Seconds $ENV:DELAY
  $NOW = Get-Date -Format "o"
  Write-Output "$NOW Postgres: Creating Service ""postgresql-x64-14""."
  Set-TimeZone  -Id UTC
  New-Item -Path . -Name "pg.pwd" -ItemType "file" -Value "oracle"
  initdb.exe  -D c:\programData\PostgreSQL\14\Data --locale "English_United States.UTF-8" --username=postgres --pwfile=pg.pwd 
  Remove-Item pg.pwd
  Add-Content -path c:\programData\PostgreSQL\14\Data\postgresql.conf  -Value "logging_collector = on"
  Add-Content -path c:\programData\PostgreSQL\14\Data\postgresql.conf  -Value "listen_addresses = '*'"
  Add-Content -path c:\programData\PostgreSQL\14\Data\postgresql.conf  -Value "port = 5432"
  pg_ctl register -N postgresql-x64-14 -D c:\programData\PostgreSQL\14\Data 
  Add-Content -path c:\programData\PostgreSQL\14\Data\pg_hba.conf  -Value "host    all             all             0.0.0.0/0            trust"
  Start-Service -Name "postgresql-x64-14"
  $NOW = Get-Date -Format "o"
  Write-Output "$NOW Postgres: Service ""postgresql-x64-14"" created."
}
$NOW = Get-Date -Format "o"
Write-Output "$NOW Postgres: Container Ready. Monitoring Logs."
$logObject = get-childItem 'C:\ProgramData\PostgreSQL\14\data\log\' | sort LastWriteTime | select -last 1
$logFile = $logObject.FullName
GET-Content -Path  $logFile  -Wait -erroraction 'silentlycontinue'
Wait-Event
