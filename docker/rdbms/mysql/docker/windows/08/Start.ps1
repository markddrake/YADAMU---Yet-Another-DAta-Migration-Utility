if ((-not (Test-Path -Path "C:\Program Data\MySQL\8.0\data"  -PathType Container)) -or (@( Get-ChildItem "C:\ProgramData\MySQL\8.0\data" ).Count -eq 0)){
  Write-Output "MySQL: Initializing MySQL service"
  mysqld --initialize-insecure --log-error="C:\ProgramData\MySQL\8.0\data\MySQL.log"
  mysqld --install MySQL
  Start-Service MySQL
  Write-Output "MySQL: Service Started"
  mysql -uroot -e "ALTER USER 'root'@'localhost' IDENTIFIED BY 'oracle';"  
}
Write-Output "MySQL: Container Ready. Monitoring Logs."
GET-Content -Path "C:\ProgramData\MySQL\8.0\data\MySQL.log"  -Wait -erroraction 'silentlycontinue'
Wait-Event
