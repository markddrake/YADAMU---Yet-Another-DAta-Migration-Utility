if ((-not (Test-Path -Path "c:\Program Data\MariaDB\10\data"  -PathType Container)) -or (@( Get-ChildItem "c:\Program Data\MariaDB\10\data" ).Count -eq 0)){
  Write-Output "MariaDB: Initializing MySQL service"
  mysql_install_db.exe --datadir="c:\ProgramData\MariaDB\10\data" --service=MariaDB --password=oracle --default-user --allow-remote-root-access 
  Start-Service MariaDB
  Write-Output "MariaDB: Service Started"
}
Write-Output "MariaDB: Container Ready. Monitoring Logs."
$HOSTNAME=hostname
GET-Content -Path "C:\ProgramData\MariaDB\10\data\$env:COMPUTERNAME.err"  -Wait -erroraction 'silentlycontinue'
Wait-Event
