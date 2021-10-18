$STATUS = (Get-Service postgresql-x64-14).StartType
if ($STATUS -eq "Manual") { 
  Write-Output "Postgres: Relocating ""data"" Folder"
  pg_ctl unregister -N postgresql-x64-14
  pg_ctl register -N postgresql-x64-14 -D c:\programData\PostgreSQL\14\Data
  Expand-Archive -Path 'C:\Program Files\PostgreSQL\14\data.zip'  -destinationPath 'C:\ProgramData\PostgreSQL\14\'
  Write-Output "Postgres: Relocated ""data"" Folder"
  Start-Service -Name "postgresql-x64-14"
  Write-Output "Postgres: Service Started"
}
Write-Output "Postgres: Container Ready. Monitoring Logs."
$logObject = get-childItem 'C:\ProgramData\PostgreSQL\14\data\log\' | sort LastWriteTime | select -last 1
$logFile = $logObject.FullName
GET-Content -Path  $logFile  -Wait -erroraction 'silentlycontinue'
Wait-Event
