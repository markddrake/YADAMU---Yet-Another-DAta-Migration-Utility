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
$NOW = Get-Date -Format "o"
Write-Output "$NOW Start Type $STATUS"

if ($STATUS -eq "Manual") {
  $NOW = Get-Date -Format "o"
  Write-Output "$NOW MSSQLSERVER: Enabling Database Service ""MSSQLSERVER""."
  Expand-Archive -Path "C:\Program Files\Microsoft SQL Server\MSSQL12.MSSQLSERVER\MSSQL\DATA.zip" -DestinationPath "C:\Program Files\Microsoft SQL Server\MSSQL12.MSSQLSERVER\MSSQL\"  
  Expand-Archive -Path "C:\Program Files\Microsoft SQL Server\MSSQL12.MSSQLSERVER\MSSQL\Log.zip"  -DestinationPath "C:\Program Files\Microsoft SQL Server\MSSQL12.MSSQLSERVER\MSSQL\"  
  Set-Service  -Name "SQLWRITER" -StartupType "Automatic"; 
  start-service SQLWRITER
  Set-Service -Name "MSSQLSERVER" -StartupType "Automatic"
  start-service MSSQLSERVER
  $NOW = Get-Date -Format "o"
  Write-Output "$NOW MSSQLSERVER: Database Service enabled."

  if($sa_password -ne "_") {
    $NOW = Get-Date -Format "o"
    Write-Output "$NOW MSSQLSERVER: Updating sa credentials."
      $sqlcmd = "ALTER LOGIN sa with password=" +"'" + $sa_password + "'" + ";ALTER LOGIN sa ENABLE;"
      & sqlcmd -Q $sqlcmd
  }
}
$NOW = Get-Date -Format "o"
Write-Output "$NOW MSSQLSERVER: Container Ready. Monitoring Logs."
$lastCheck = (Get-Date).AddSeconds(-2) 
while ($true) 
{ 
    Get-EventLog -LogName Application -Source "MSSQL*" -After $lastCheck | Select-Object TimeGenerated, EntryType, Message	 
    $lastCheck = Get-Date 
    Start-Sleep -Seconds 2 
}