@SET TGT=%~1
@SET VER=%~2
@SET ID=%~3
node node\jTableImport --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=Northwind%ID%        --TOUSER=\"dbo\"            --FILE=%TGT%\Northwind%VER%.json
node node\jTableImport --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks%ID%   --TOUSER=\"Sales\"          --FILE=%TGT%\Sales%VER%.json
node node\jTableImport --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks%ID%   --TOUSER=\"Person\"         --FILE=%TGT%\Person%VER%.json
node node\jTableImport --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks%ID%   --TOUSER=\"Production\"     --FILE=%TGT%\Production%VER%.json
node node\jTableImport --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks%ID%   --TOUSER=\"Purchasing\"     --FILE=%TGT%\Purchasing%VER%.json
node node\jTableImport --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks%ID%   --TOUSER=\"HumanResources\" --FILE=%TGT%\HumanResources%VER%.json
node node\jTableImport --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorksDW%ID% --TOUSER=\"dbo\"            --FILE=%TGT%\AdventureWorksDW%VER%.json
