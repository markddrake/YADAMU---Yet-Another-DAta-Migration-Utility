$service = Get-Service -Name MariaDB -ErrorAction SilentlyContinue
if($service -eq $null) {
  Start-Sleep -Seconds $ENV:DELAY
  $NOW = Get-Date -Format "o"
  Write-Output "$NOW MariaDB: Initializing MySQL service"
  # Push the utf-8 configuration file into the newly mounted Conf.d folder.
  Move-Item -Path "c:\ProgramData\MariaDB\11\utf-8.cnf"  -Destination "c:\ProgramData\MariaDB\11\Conf.d\"
  mysql_install_db.exe --datadir="c:\ProgramData\MariaDB\11\data" --service=MariaDB --password=oracle --default-user --allow-remote-root-access
  # Append the !includedir to the newly created my.ini
  Add-Content -path "C:\ProgramData\MariaDB\11\data\my.ini" -Value "!includedir C:\ProgramData\MariaDB\11\conf.d"
  Start-Service MariaDB
  $NOW = Get-Date -Format "o"
  Write-Output "$NOW MariaDB: Service Started"
}
$NOW = Get-Date -Format "o"
Write-Output "$NOW MariaDB: Container Ready. Monitoring Logs."
$HOSTNAME=hostname
GET-Content -Path "C:\ProgramData\MariaDB\11\data\$env:COMPUTERNAME.err"  -Wait -erroraction 'silentlycontinue'
Wait-Event
