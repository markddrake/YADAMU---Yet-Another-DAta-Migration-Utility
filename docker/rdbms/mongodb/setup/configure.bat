set STAGE=c:\stage
cd %STAGE%
mkdir log
powershell -command (Get-Service """MongoDB""").waitForStatus("""Running""","""00:03:00""")
mongosh --host localhost --port 27017 --file c:\stage\js\yadamuInstanceID.js