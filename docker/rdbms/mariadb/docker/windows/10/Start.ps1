if ((-not (Test-Path -Path "c:\ProgramData\MariaDB\10\data"  -PathType Container)) -or (@( Get-ChildItem "c:\ProgramData\MariaDB\10\data" ).Count -eq 0)){
  # Push the utf-8 configuration file into the newly mounted Conf.d folder.
  Move-Item -Path "c:\ProgramData\MariaDB\10\utf-8.cnf"  -Destination "c:\ProgramData\MariaDB\10\Conf.d\"
  Write-Output "MariaDB: Initializing MySQL service"
  mysql_install_db.exe --datadir="c:\ProgramData\MariaDB\10\data" --service=MariaDB --password=oracle --default-user --allow-remote-root-access
  # Append the !includedir to the newly created my.ini
  Add-Content -path "C:\ProgramData\MariaDB\10\data\my.ini" -Value "!includedir C:\ProgramData\MariaDB\10\conf.d"
  Start-Service MariaDB
  Write-Output "MariaDB: Service Started"
}
Write-Output "MariaDB: Container Ready. Monitoring Logs."
$HOSTNAME=hostname
GET-Content -Path "C:\ProgramData\MariaDB\10\data\$env:COMPUTERNAME.err"  -Wait -erroraction 'silentlycontinue'
Wait-Event
