$service = Get-Service -Name MongoDB -ErrorAction SilentlyContinue
if($service -eq $null) {
  Start-Sleep -Seconds $ENV:DELAY
  $NOW = Get-Date -Format "o"
  Write-Output "$NOW MongoDB: Enabling Database Service ""MongoDB""."
  New-Item -ItemType directory -Path C:\ProgramData\MongoDB\Server\5.0\data
  New-Item -ItemType directory -Path C:\ProgramData\MongoDB\Server\5.0\log
  mongod.exe --install -config "c:\Program Files\MongoDB\Server\5.0\bin\mongod.cfg"
  start-service MongoDB
  $NOW = Get-Date -Format "o"
  Write-Output "$NOW MongoDB: Database Service ""MongoDB"" created."
}
$NOW = Get-Date -Format "o"
Write-Output "$NOW MongoDB: Container Ready. Monitoring Logs."
GET-Content -Path  "C:\ProgramData\MongoDB\Server\5.0\log\mongod.log"  -Wait -ErrorAction 'silentlycontinue'
Wait-Event
