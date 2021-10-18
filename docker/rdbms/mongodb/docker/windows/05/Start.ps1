$STATUS = (Get-Service MongoDB).StartType
Write-Output "MongoDB: Start Type: ""$STATUS"""

if ($STATUS -eq "Manual") {
  Write-Output "MongoDB: Relocating ""data"" and ""log"" Folders"
  New-Item -Force -ItemType directory -Path "C:\ProgramData\MongoDB\Server\5.0"
  Expand-Archive -Path "C:\Program Files\MongoDB\Server\5.0\data.zip" -DestinationPath "C:\ProgramData\MongoDB\Server\5.0\"
  Expand-Archive -Path "C:\Program Files\MongoDB\Server\5.0\log.zip"  -DestinationPath "C:\ProgramData\MongoDB\Server\5.0\"
  Write-Output "MongoDB: Relocated ""data"" and ""log"" Folders"
  Set-Service -Name "MongoDB" -StartupType "Automatic"
  Write-Output "MongoDB: Set StartType ""Automatic"""
  Write-Output "MongoDB: Starting Service."
  start-service MongoDB
  Write-Output "MongoDB: Service Started"
}
Write-Output "MongoDB: Container Ready. Monitoring Logs."
GET-Content -Path  "C:\ProgramData\MongoDB\Server\5.0\log\mongod.log"  -Wait -ErrorAction 'silentlycontinue'
Wait-Event
