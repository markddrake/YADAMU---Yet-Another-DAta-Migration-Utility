param(
[Parameter(Mandatory=$false)]
[string]$sa_password,

[Parameter(Mandatory=$false)]
[string]$ACCEPT_EULA
)

if($ACCEPT_EULA -ne "Y" -And $ACCEPT_EULA -ne "y")
{
	Write-Output "ERROR: You must accept the End User License Agreement before this container can start."
	Write-Output "Set the environment variable ACCEPT_EULA to 'Y' if you accept the agreement."

    exit 1
}

# start the service

$STATUS = (Get-Service MSSQLSERVER).StartType
Write-Output "MSSQLSERVER: Start Type $STATUS"

if ($STATUS -eq "Manual") {
  Write-Output "MSSQLSERVER: Restoring DATA and Log Folders"
  Expand-Archive -Path "C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA.zip" -DestinationPath "C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\"  
  Expand-Archive -Path "C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\Log.zip"  -DestinationPath "C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\"  
  Write-Output "MSSQLSERVER: Restored DATA and Log Folders"
  Set-Service -Name "MSSQLSERVER" -StartupType "Automatic"
  Write-Output "MSSQLSERVER: Set StartType Automatic"
  Write-Output "MSSQLSERVER: Starting."
  start-service MSSQLSERVER
}

Write-Output "MSSQLSERVER: Started."

if($sa_password -ne "_")
{
    Write-Output "MSSQLSERVER: Updating sa credentials."
    $sqlcmd = "ALTER LOGIN sa with password=" +"'" + $sa_password + "'" + ";ALTER LOGIN sa ENABLE;"
    & sqlcmd -Q $sqlcmd
}

Write-Output "MSSQLSERVER: Container Ready. Monitoring Logs."

$lastCheck = (Get-Date).AddSeconds(-2) 
while ($true) 
{ 
    Get-EventLog -LogName Application -Source "MSSQL*" -After $lastCheck | Select-Object TimeGenerated, EntryType, Message	 
    $lastCheck = Get-Date 
    Start-Sleep -Seconds 2 
}